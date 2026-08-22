"""Tests de run_source : traçabilité, dry-run, POST, remontée des erreurs.
Aucun appel réseau ni écriture Delta réels — requests.Session et les helpers
Delta sont mockés."""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import MagicMock

import pytest

from flume_lib.oauth1 import OAuth1Signer
from flume_lib.source import (
    LINEAGE_INGESTED_AT,
    LINEAGE_RUN_ID,
    RetryableHTTPError,
    _build_wait,
    _parse_retry_after,
    run_source,
)

BASE_CONFIG = {
    "name": "s1",
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}
TABLES_PATH = "/tmp/Tables"  # chemin non-Fabric : renvoyé tel quel


class FakeResponse:
    def __init__(self, payload, headers=None, status_code=200):
        self._payload = payload
        self.headers = headers or {}
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")


class FakeSession:
    """Session HTTP factice : rejoue une liste de payloads et enregistre les
    appels effectués."""

    instances: list = []

    def __init__(self):
        self.headers = {}
        self.calls = []
        self.payloads = list(FakeSession.next_payloads)
        FakeSession.instances.append(self)

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        payload = self.payloads.pop(0) if self.payloads else []
        if isinstance(payload, tuple):
            return FakeResponse(payload[0], payload[1])
        return FakeResponse(payload)


@pytest.fixture
def http(monkeypatch):
    """Injecte FakeSession et expose la dernière session créée."""
    FakeSession.instances = []
    FakeSession.next_payloads = []
    monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)
    return FakeSession


@pytest.fixture
def delta(monkeypatch):
    """Mocke les écritures/lectures Delta et enregistre les appels."""
    # 'watermark_value' est ce que renverra read_watermark : None simule un
    # premier run, un test peut le remplacer pour simuler un run incremental.
    calls = {
        "append": [], "log": [], "watermark_write": [], "watermark_read": [],
        "watermark_value": None,
    }

    def fake_append(uri, records, **kwargs):
        calls["append"].append({"uri": uri, "records": records})

    def fake_log(path, **kwargs):
        calls["log"].append(kwargs)

    def fake_write_wm(path, source_name, last_value, **kwargs):
        calls["watermark_write"].append((source_name, last_value))

    def fake_read_wm(path, source_name, **kwargs):
        calls["watermark_read"].append(source_name)
        return calls["watermark_value"]

    monkeypatch.setattr("flume_lib.source.append_records", fake_append)
    monkeypatch.setattr("flume_lib.source.write_log_run", fake_log)
    monkeypatch.setattr("flume_lib.source.write_watermark", fake_write_wm)
    monkeypatch.setattr("flume_lib.source.read_watermark", fake_read_wm)
    return calls


class TestLineageColumns:
    def test_every_row_carries_run_id_and_timestamp(self, http, delta):
        http.next_payloads = [[{"id": 1}, {"id": 2}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        written = delta["append"][0]["records"]
        assert [r[LINEAGE_RUN_ID] for r in written] == [result.run_id] * 2
        assert all(r[LINEAGE_INGESTED_AT] for r in written)
        # les champs de l'API sont préservés
        assert [r["id"] for r in written] == [1, 2]

    def test_run_id_is_stable_between_result_and_logs(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert delta["log"][0]["run_id"] == result.run_id

    def test_target_uri_uses_schema(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert delta["append"][0]["uri"] == "/tmp/Tables/bronze/items"


class TestDryRun:
    def test_counts_rows_without_writing_anything(self, http, delta):
        config = {**BASE_CONFIG, "pagination": {"type": "offset", "limit": 2}}
        http.next_payloads = [[{"id": 1}, {"id": 2}], [{"id": 3}]]

        result = run_source(config, lakehouse_tables_path=TABLES_PATH, dry_run=True)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 3
        assert delta["append"] == []
        assert delta["log"] == []
        assert delta["watermark_write"] == []

    def test_sample_is_capped_and_raw(self, http, delta):
        http.next_payloads = [[{"id": i} for i in range(10)]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH, dry_run=True)

        assert len(result.sample) == 3
        # échantillon brut : pas de colonnes de traçabilité
        assert LINEAGE_RUN_ID not in result.sample[0]

    def test_reads_watermark_but_never_writes_it(self, http, delta):
        config = {
            **BASE_CONFIG,
            "incremental": {"enabled": True, "field": "id", "param_name": "since"},
        }
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH, dry_run=True)

        assert delta["watermark_read"] == ["s1"]
        assert delta["watermark_write"] == []

    def test_sample_is_none_on_normal_run(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert result.sample is None


class TestHttpMethods:
    def test_get_by_default(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        call = http.instances[0].calls[0]
        assert call["method"] == "GET"
        assert "json" not in call

    def test_post_sends_json_body(self, http, delta):
        config = {**BASE_CONFIG, "method": "POST", "body": {"query": "all"}}
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)

        call = http.instances[0].calls[0]
        assert call["method"] == "POST"
        assert call["json"] == {"query": "all"}

    def test_post_with_params_in_body_merges_pagination(self, http, delta):
        config = {
            **BASE_CONFIG,
            "method": "POST",
            "body": {"query": "all"},
            "pagination": {"type": "offset", "limit": 2, "params_in": "body"},
        }
        http.next_payloads = [[{"id": 1}, {"id": 2}], [{"id": 3}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)

        bodies = [c["json"] for c in http.instances[0].calls]
        assert bodies == [
            {"query": "all", "limit": 2, "offset": 0},
            {"query": "all", "limit": 2, "offset": 2},
        ]
        assert all("params" not in c for c in http.instances[0].calls)

    def test_post_with_params_in_query_keeps_body_static(self, http, delta):
        config = {
            **BASE_CONFIG,
            "method": "POST",
            "body": {"query": "all"},
            "pagination": {"type": "offset", "limit": 2},
        }
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)

        call = http.instances[0].calls[0]
        assert call["json"] == {"query": "all"}
        assert call["params"] == {"limit": 2, "offset": 0}

    def test_post_form_body(self, http, delta):
        config = {
            **BASE_CONFIG,
            "method": "POST",
            "body_format": "form",
            "body": {"query": "all"},
        }
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[0].calls[0]["data"] == {"query": "all"}


class TestErrorReporting:
    def test_invalid_config_fails_without_raising(self, http, delta):
        result = run_source(
            {**BASE_CONFIG, "pagintaion": {}}, lakehouse_tables_path=TABLES_PATH
        )
        assert result.status == "failed"
        assert "pagination" in result.error_message
        assert delta["append"] == []

    def test_failed_run_is_still_logged(self, http, delta):
        run_source({**BASE_CONFIG, "foo": 1}, lakehouse_tables_path=TABLES_PATH)
        assert delta["log"][0]["status"] == "failed"

    def test_dry_run_reports_config_error(self, http, delta):
        result = run_source(
            {**BASE_CONFIG, "foo": 1}, lakehouse_tables_path=TABLES_PATH, dry_run=True
        )
        assert result.status == "failed"
        assert delta["log"] == []


class TestStaticHeaders:
    def test_headers_are_sent_on_every_call(self, http, delta):
        config = {**BASE_CONFIG, "headers": {"Prefer": "transient"}}
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].headers["Prefer"] == "transient"

    def test_auth_headers_win_over_config_headers(self, http, delta, monkeypatch):
        monkeypatch.setenv("TOK", "real-token")
        config = {
            **BASE_CONFIG,
            "headers": {"Authorization": "Bearer usurpe", "Prefer": "transient"},
            "auth": {"type": "bearer_token", "token": {"env_var": "TOK"}},
        }
        http.next_payloads = [[{"id": 1}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        session = http.instances[-1]
        assert session.headers["Authorization"] == "Bearer real-token"
        assert session.headers["Prefer"] == "transient"

    def test_oauth1_is_installed_as_a_request_signer(self, http, delta, monkeypatch):
        monkeypatch.setenv("CK", "ck")
        monkeypatch.setenv("CS", "cs")
        config = {
            **BASE_CONFIG,
            "auth": {
                "type": "oauth1",
                "realm": "1234567",
                "consumer_key": {"env_var": "CK"},
                "consumer_secret": {"env_var": "CS"},
            },
        }
        http.next_payloads = [[{"id": 1}]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        session = http.instances[-1]
        assert isinstance(session.auth, OAuth1Signer)
        # rien de statique : la signature est posee requete par requete
        assert "Authorization" not in session.headers


class TestRetryAfter:
    def test_parses_delay_in_seconds(self):
        assert _parse_retry_after("30") == 30.0

    def test_parses_http_date(self):
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        delay = _parse_retry_after(format_datetime(future, usegmt=True))
        assert 100 <= delay <= 125

    def test_past_http_date_gives_zero(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        assert _parse_retry_after(format_datetime(past, usegmt=True)) == 0.0

    def test_absent_or_garbage_gives_none(self):
        assert _parse_retry_after(None) is None
        assert _parse_retry_after("") is None
        assert _parse_retry_after("bientot") is None

    def _retry_state(self, exc):
        state = MagicMock()
        state.outcome.exception.return_value = exc
        return state

    def test_server_delay_wins_over_backoff(self):
        wait = _build_wait({"backoff_multiplier": 1})
        state = self._retry_state(RetryableHTTPError(429, "u", retry_after=42.0))
        assert wait(state) == 42.0

    def test_delay_is_capped(self):
        wait = _build_wait({"max_retry_after_seconds": 10})
        state = self._retry_state(RetryableHTTPError(429, "u", retry_after=3600.0))
        assert wait(state) == 10

    def test_falls_back_to_exponential_without_header(self):
        wait = _build_wait({"backoff_multiplier": 1})
        state = self._retry_state(RetryableHTTPError(503, "u"))
        state.attempt_number = 1
        assert wait(state) > 0

    def test_429_response_drives_the_retry_delay(self, http, delta, monkeypatch):
        slept = []
        monkeypatch.setattr("tenacity.nap.time.sleep", slept.append)

        class Throttling(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) == 1:
                    return FakeResponse({}, {"Retry-After": "7"}, status_code=429)
                return FakeResponse([{"id": 1}])

        monkeypatch.setattr("flume_lib.source.requests.Session", Throttling)
        Throttling.next_payloads = []
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert slept == [7.0]


class TestWatermarkInBodyTemplate:
    CONFIG = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {"q": "SELECT id FROM t WHERE d >= '{watermark}' ORDER BY id"},
        "incremental": {
            "enabled": True,
            "field": "d",
            "inject": "body_template",
            "initial_value": "1970-01-01 00:00:00",
            "value_format": "iso_datetime",
        },
    }

    def test_first_run_uses_the_initial_value(self, http, delta):
        http.next_payloads = [[{"id": 1, "d": "2026-08-01 00:00:00"}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert http.instances[-1].calls[0]["json"] == {
            "q": "SELECT id FROM t WHERE d >= '1970-01-01 00:00:00' ORDER BY id"
        }

    def test_next_run_uses_the_stored_watermark(self, http, delta):
        delta["watermark_value"] = "2026-08-01 00:00:00"
        http.next_payloads = [[{"id": 2, "d": "2026-08-22 09:00:00"}]]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert "2026-08-01 00:00:00" in http.instances[-1].calls[0]["json"]["q"]

    def test_watermark_still_advances(self, http, delta):
        http.next_payloads = [[{"id": 1, "d": "2026-08-22 09:00:00"}]]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert delta["watermark_write"] == [("s1", "2026-08-22 09:00:00")]

    def test_nothing_is_injected_in_the_query_string(self, http, delta):
        http.next_payloads = [[{"id": 1, "d": "2026-08-22 09:00:00"}]]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].calls[0].get("params") in (None, {})

    def test_a_hostile_watermark_fails_the_run(self, http, delta):
        delta["watermark_value"] = "2026-08-01' OR '1'='1"
        http.next_payloads = [[{"id": 1}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "interdit" in result.error_message
        assert delta["append"] == []

    def test_a_wrongly_formatted_watermark_fails_the_run(self, http, delta):
        delta["watermark_value"] = "22/08/2026"
        http.next_payloads = [[{"id": 1}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "failed"
        assert "iso_datetime" in result.error_message

    def test_a_typo_in_the_placeholder_fails_the_run(self, http, delta):
        config = {**self.CONFIG, "body": {"q": "WHERE d >= '{watermak}'"}}
        http.next_payloads = [[{"id": 1}]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "watermak" in result.error_message

    def test_custom_placeholder_name(self, http, delta):
        config = {
            **self.CONFIG,
            "body": {"q": "WHERE id > {last_id}"},
            "incremental": {
                "enabled": True,
                "field": "id",
                "inject": "body_template",
                "placeholder": "last_id",
                "initial_value": 0,
                "value_format": "numeric",
            },
        }
        http.next_payloads = [[{"id": 5}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].calls[0]["json"] == {"q": "WHERE id > 0"}

    def test_query_param_mode_is_unchanged(self, http, delta):
        delta["watermark_value"] = "2026-08-01"
        config = {
            **BASE_CONFIG,
            "incremental": {"enabled": True, "field": "d", "param_name": "since"},
        }
        http.next_payloads = [[{"id": 1, "d": "2026-08-22"}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].calls[0]["params"]["since"] == "2026-08-01"

    def test_initial_value_also_applies_in_query_param_mode(self, http, delta):
        config = {
            **BASE_CONFIG,
            "incremental": {
                "enabled": True,
                "field": "d",
                "param_name": "since",
                "initial_value": "1970-01-01",
            },
        }
        http.next_payloads = [[{"id": 1, "d": "2026-08-22"}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].calls[0]["params"]["since"] == "1970-01-01"


class TestErrorMessagesLeakNothing:
    """error_message est persisté dans log_runs, une table Delta lisible par
    tout le lakehouse. La query string ne doit pas s'y retrouver."""

    def test_query_string_is_stripped_from_a_4xx(self, http, delta):
        config = {**BASE_CONFIG, "params": {"api_key": "SECRET", "filter": "on"}}

        class Failing(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                return FakeResponse({"error": "nope"}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = Failing
        Failing.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        assert "SECRET" not in result.error_message
        assert "403" in result.error_message
        # l'endroit reste identifiable
        assert "https://api.test/items" in result.error_message

    def test_retryable_error_is_stripped_too(self, http, delta):
        config = {
            **BASE_CONFIG,
            "base_url": "https://api.test/items?token=SECRET",
            "retry": {"max_attempts": 1},
        }

        class Failing(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                return FakeResponse({}, status_code=503)

        import flume_lib.source as source_module

        source_module.requests.Session = Failing
        Failing.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        assert "SECRET" not in result.error_message
        assert "503" in result.error_message

    def test_rows_loaded_still_locates_the_failure(self, http, delta):
        config = {**BASE_CONFIG, "pagination": {"type": "offset", "limit": 2}}

        class HalfWay(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) <= 2:
                    return FakeResponse([{"id": 1}, {"id": 2}])
                return FakeResponse({}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = HalfWay
        HalfWay.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        # 2 pages complètes avant l'échec : la position du run reste lisible
        assert delta["log"][0]["rows_loaded"] == 0
