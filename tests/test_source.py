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
    # 'append_result' est ce que renverra append_records : (types Arrow
    # retenus, dégradations subies). Un test peut le remplacer pour simuler
    # une colonne repliée sur du texte.
    calls = {
        "append": [], "log": [], "watermark_write": [], "watermark_read": [],
        "watermark_value": None, "append_result": ({}, []),
    }

    def fake_append(uri, records, known_types=None, **kwargs):
        # copie : l'appelant réutilise et mute le même dict d'un lot à l'autre
        calls["append"].append(
            {"uri": uri, "records": records, "known_types": dict(known_types or {})}
        )
        return calls["append_result"]

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


class TestResponseErrors:
    """Erreurs applicatives renvoyées avec un statut HTTP 200 — la norme en
    GraphQL."""

    GRAPHQL = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {"query": "{ orders { edges { node { id } } } }"},
        "pagination": {
            "type": "none",
            "items_field": "data.orders.edges",
            "record_field": "node",
        },
        "errors": {"path": "errors", "retryable_codes": ["THROTTLED"]},
    }

    def test_error_in_a_200_fails_the_run(self, http, delta):
        http.next_payloads = [{
            "data": None,
            "errors": [{
                "message": "Access denied for orders field",
                "extensions": {"code": "ACCESS_DENIED"},
            }],
        }]
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "Access denied for orders field" in result.error_message
        assert delta["append"] == []

    def test_partial_error_beside_valid_data_fails_the_run(self, http, delta):
        """Le cas dangereux : des lignes exploitables *et* une erreur. Sans
        contrôle, le run passe `success` en ayant perdu une partie des données."""
        payload = {
            "data": {"orders": {"edges": [{"node": {"id": 1}}]}},
            "errors": [{"message": "champ refusé", "extensions": {"code": "X"}}],
        }
        http.next_payloads = [payload]
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "champ refusé" in result.error_message
        assert delta["append"] == []

    def test_without_the_errors_block_the_same_payload_passes_silently(
        self, http, delta
    ):
        """Documente ce que le nouveau bloc 'errors' corrige."""
        config = {k: v for k, v in self.GRAPHQL.items() if k != "errors"}
        http.next_payloads = [{
            "data": {"orders": {"edges": [{"node": {"id": 1}}]}},
            "errors": [{"message": "champ refusé"}],
        }]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "success"

    def test_empty_errors_list_is_not_an_error(self, http, delta):
        http.next_payloads = [{
            "data": {"orders": {"edges": [{"node": {"id": 1}}]}},
            "errors": [],
        }]
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "success", result.error_message
        assert delta["append"][0]["records"][0]["id"] == 1

    def test_retryable_code_is_replayed(self, http, delta, monkeypatch):
        monkeypatch.setattr("tenacity.nap.time.sleep", lambda _: None)

        class Throttled(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) == 1:
                    return FakeResponse({
                        "errors": [{
                            "message": "Throttled",
                            "extensions": {"code": "THROTTLED"},
                        }],
                    })
                return FakeResponse(
                    {"data": {"orders": {"edges": [{"node": {"id": 1}}]}}}
                )

        monkeypatch.setattr("flume_lib.source.requests.Session", Throttled)
        Throttled.next_payloads = []
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 1
        assert len(http.instances[-1].calls) == 2

    def test_non_retryable_code_is_not_replayed(self, http, delta):
        http.next_payloads = [
            {"errors": [{"message": "nope", "extensions": {"code": "ACCESS_DENIED"}}]},
            {"data": {"orders": {"edges": []}}},
        ]
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert len(http.instances[-1].calls) == 1

    def test_error_detail_is_capped(self, http, delta):
        http.next_payloads = [
            {"errors": [{"message": "x" * 5000, "extensions": {"code": "E"}}]}
        ]
        result = run_source(self.GRAPHQL, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "failed"
        assert len(result.error_message) < 1000

    def test_custom_envelope(self, http, delta):
        config = {
            **BASE_CONFIG,
            "errors": {
                "path": "response.faults",
                "message_field": "detail",
                "code_field": "kind",
                "retryable_codes": ["BUSY"],
            },
        }
        http.next_payloads = [
            {"response": {"faults": [{"detail": "boom", "kind": "FATAL"}]}}
        ]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert result.status == "failed"
        assert "boom" in result.error_message


class TestNestedBodyParams:
    """`params_path` : pagination imbriquée dans le corps (GraphQL variables)."""

    CONFIG = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {
            "query": "query($first: Int!, $after: String) { orders { id } }",
            "variables": {"sort": "ID"},
        },
        "pagination": {
            "type": "cursor",
            "cursor_param": "after",
            "cursor_field": "data.orders.pageInfo.endCursor",
            "has_more_field": "data.orders.pageInfo.hasNextPage",
            "items_field": "data.orders.edges",
            "record_field": "node",
            "limit": 250,
            "limit_param": "first",
            "params_in": "body",
            "params_path": "variables",
        },
    }

    @staticmethod
    def page(ids, end_cursor, has_next):
        return {"data": {"orders": {
            "edges": [{"node": {"id": i}} for i in ids],
            "pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next},
        }}}

    def test_pagination_params_land_in_variables(self, http, delta):
        http.next_payloads = [self.page([1], "c1", True), self.page([2], "c2", False)]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        bodies = [c["json"] for c in http.instances[-1].calls]
        assert bodies[0]["variables"] == {"sort": "ID", "first": 250}
        assert bodies[1]["variables"] == {"sort": "ID", "first": 250, "after": "c1"}
        # la requête elle-même reste intacte à côté des variables
        assert bodies[0]["query"] == self.CONFIG["body"]["query"]

    def test_records_are_unwrapped_before_the_delta_write(self, http, delta):
        http.next_payloads = [self.page([1, 2], "c2", False)]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        written = delta["append"][0]["records"]
        assert [r["id"] for r in written] == [1, 2]
        assert "node" not in written[0]

    def test_missing_variables_branch_is_created(self, http, delta):
        config = {**self.CONFIG, "body": {"query": "q"}}
        http.next_payloads = [self.page([1], "c1", False)]
        run_source(config, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[-1].calls[0]["json"]["variables"] == {"first": 250}


class TestTemplatePaths:
    """Le corps GraphQL est plein d'accolades : le templating doit pouvoir
    être restreint aux seules variables."""

    CONFIG = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {
            # accolades collées : indiscernables d'un placeholder sans restriction
            "query": "{orders(query:$q){edges{node{id updatedAt}}pageInfo{hasNextPage}}}",
            "variables": {"q": "updated_at:>'{watermark}'"},
        },
        "template_paths": ["variables"],
        "pagination": {
            "type": "none",
            "items_field": "data.orders.edges",
            "record_field": "node",
        },
        "incremental": {
            "enabled": True,
            "field": "updatedAt",
            "inject": "body_template",
            "initial_value": "1970-01-01T00:00:00Z",
            "value_format": "iso_datetime",
        },
    }

    PAYLOAD = {"data": {"orders": {"edges": [
        {"node": {"id": 1, "updatedAt": "2026-08-22T09:00:00Z"}}
    ]}}}

    def test_only_the_declared_branch_is_substituted(self, http, delta):
        http.next_payloads = [self.PAYLOAD]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        body = http.instances[-1].calls[0]["json"]
        assert body["variables"]["q"] == "updated_at:>'1970-01-01T00:00:00Z'"
        # la requête est repartie telle quelle, accolades comprises
        assert body["query"] == self.CONFIG["body"]["query"]

    def test_without_template_paths_the_graphql_braces_break_the_run(
        self, http, delta
    ):
        """Justifie l'existence de l'option."""
        config = {k: v for k, v in self.CONFIG.items() if k != "template_paths"}
        http.next_payloads = [self.PAYLOAD]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "placeholder" in result.error_message

    def test_watermark_still_advances(self, http, delta):
        http.next_payloads = [self.PAYLOAD]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert delta["watermark_write"] == [("s1", "2026-08-22T09:00:00Z")]

    def test_a_hostile_watermark_still_fails_the_run(self, http, delta):
        delta["watermark_value"] = "2026-08-01T00:00:00Z' OR '1'='1"
        http.next_payloads = [self.PAYLOAD]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "interdit" in result.error_message


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
                    start = 2 * len(self.calls) - 1
                    return FakeResponse([{"id": start}, {"id": start + 1}])
                return FakeResponse({}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = HalfWay
        HalfWay.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        # 2 pages complètes avant l'échec, mais sous le batch_size par défaut :
        # rien n'a été commité, rows_loaded le dit
        assert delta["log"][0]["rows_loaded"] == 0


class TestBatchedWrites:
    """Écriture par lots : la mémoire est bornée, et un run interrompu laisse
    derrière lui ce qu'il a réellement écrit."""

    def test_a_small_source_still_produces_a_single_commit(self, http, delta):
        http.next_payloads = [[{"id": 1}, {"id": 2}, {"id": 3}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert len(delta["append"]) == 1
        assert result.rows_loaded == 3

    def test_records_are_flushed_every_batch_size_rows(self, http, delta):
        config = {**BASE_CONFIG, "batch_size": 2}
        http.next_payloads = [[{"id": i} for i in range(5)]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert [len(c["records"]) for c in delta["append"]] == [2, 2, 1]
        assert result.rows_loaded == 5

    def test_a_batch_spanning_several_pages_is_written_once(self, http, delta):
        config = {
            **BASE_CONFIG,
            "batch_size": 4,
            "pagination": {"type": "offset", "limit": 2},
        }
        http.next_payloads = [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
            [{"id": 5}],
        ]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert [len(c["records"]) for c in delta["append"]] == [4, 1]

    def test_rows_loaded_reports_what_was_really_written(self, http, delta):
        config = {
            **BASE_CONFIG,
            "batch_size": 2,
            "pagination": {"type": "offset", "limit": 2},
        }

        class HalfWay(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) <= 2:
                    start = 2 * len(self.calls) - 1
                    return FakeResponse([{"id": start}, {"id": start + 1}])
                return FakeResponse({}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = HalfWay
        HalfWay.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        # les deux premières pages sont dans la table : le run est repris
        # depuis là, pas depuis zéro
        assert result.rows_loaded == 4
        assert delta["log"][0]["rows_loaded"] == 4


class TestWatermarkCoherence:
    CONFIG = {
        **BASE_CONFIG,
        "batch_size": 2,
        "incremental": {
            "enabled": True,
            "field": "ts",
            "param_name": "since",
        },
    }

    def test_watermark_is_written_once_at_the_end_by_default(self, http, delta):
        http.next_payloads = [[{"ts": i} for i in (1, 2, 3, 4)]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert len(delta["append"]) == 2
        assert delta["watermark_write"] == [("s1", 4)]

    def test_checkpoint_commits_the_watermark_after_each_batch(self, http, delta):
        config = {
            **self.CONFIG,
            "incremental": {**self.CONFIG["incremental"], "checkpoint": True},
        }
        http.next_payloads = [[{"ts": i} for i in (1, 2, 3, 4)]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert delta["watermark_write"] == [("s1", 2), ("s1", 4)]

    def test_checkpoint_survives_an_interrupted_run(self, http, delta):
        config = {
            **self.CONFIG,
            "pagination": {"type": "offset", "limit": 2},
            "incremental": {**self.CONFIG["incremental"], "checkpoint": True},
        }

        class HalfWay(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) == 1:
                    return FakeResponse([{"ts": 1}, {"ts": 2}])
                return FakeResponse({}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = HalfWay
        HalfWay.next_payloads = []
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        assert result.rows_loaded == 2
        # le watermark reflète exactement les lignes commitées
        assert delta["watermark_write"] == [("s1", 2)]

    def test_an_unsorted_source_stops_instead_of_skipping_rows(self, http, delta):
        config = {
            **self.CONFIG,
            "incremental": {**self.CONFIG["incremental"], "checkpoint": True},
        }
        http.next_payloads = [[{"ts": 5}, {"ts": 6}, {"ts": 3}, {"ts": 4}]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "trié" in result.error_message
        # le lot fautif n'est pas écrit : le watermark reste cohérent
        assert len(delta["append"]) == 1
        assert delta["watermark_write"] == [("s1", 6)]

    def test_an_unsorted_source_is_fine_without_checkpoint(self, http, delta):
        http.next_payloads = [[{"ts": 5}, {"ts": 6}, {"ts": 3}, {"ts": 4}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert delta["watermark_write"] == [("s1", 6)]

    def test_a_mixed_type_field_fails_before_anything_is_written(self, http, delta):
        http.next_payloads = [[{"ts": 1}, {"ts": "hier"}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "mélange des types" in result.error_message
        # l'ancien comportement écrivait le lot puis échouait sur le max()
        assert delta["append"] == []
        assert delta["watermark_write"] == []

    def test_rows_without_the_field_do_not_block_the_watermark(self, http, delta):
        http.next_payloads = [[{"ts": 1}, {"other": 2}]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert delta["watermark_write"] == [("s1", 1)]

    def test_no_row_at_all_writes_no_watermark(self, http, delta):
        http.next_payloads = [[]]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert delta["append"] == []
        assert delta["watermark_write"] == []


class TestTokenRefresh:
    """Un run plus long que la durée de vie du token voyait ses dernières
    pages répondre 401 — statut non rejouable, run perdu en entier."""

    SP_CONFIG = {
        **BASE_CONFIG,
        "auth": {
            "type": "oauth2_client_credentials",
            "token_url": "https://idp.test/token",
            "client_id": "id",
            "client_secret": "sec",
        },
    }
    STATIC_CONFIG = {
        **BASE_CONFIG,
        "auth": {"type": "bearer_token", "token": "static-tok"},
    }

    @pytest.fixture
    def token_endpoint(self, monkeypatch):
        """Compte les appels au token endpoint et sert un token différent à
        chaque fois, pour distinguer l'original du renouvelé."""
        issued = []

        def fake_post(url, **kwargs):
            issued.append(url)
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {"access_token": f"tok-{len(issued)}"}
            return response

        monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)
        monkeypatch.setattr("flume_lib.auth.requests.post", fake_post)
        return issued

    @staticmethod
    def _session_class(statuses):
        """Session factice qui rejoue une suite de statuts HTTP et retient le
        header d'auth de chaque appel."""

        class Recording(FakeSession):
            def request(self, method, url, **kwargs):
                index = len(self.calls)
                self.calls.append(
                    {"url": url, "auth": self.headers.get("Authorization")}
                )
                status = statuses[min(index, len(statuses) - 1)]
                if status == 200:
                    return FakeResponse([{"id": index}])
                return FakeResponse({}, status_code=status)

        return Recording

    def _run(self, monkeypatch, config, statuses):
        # FakeSession.__init__ enregistre chaque instance sur la classe de
        # base ; la fixture `http` a remis la liste à zéro.
        monkeypatch.setattr(
            "flume_lib.source.requests.Session", self._session_class(statuses)
        )
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        return result, FakeSession.instances[-1]

    def test_a_401_renews_the_token_and_replays_the_page(
        self, http, delta, token_endpoint, monkeypatch
    ):
        result, session = self._run(monkeypatch, self.SP_CONFIG, [401, 200])

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 1
        # un token à l'ouverture de la session, un second après le 401
        assert len(token_endpoint) == 2
        assert [c["auth"] for c in session.calls] == [
            "Bearer tok-1",
            "Bearer tok-2",
        ]

    def test_a_persistent_401_is_not_replayed_forever(
        self, http, delta, token_endpoint, monkeypatch
    ):
        result, session = self._run(monkeypatch, self.SP_CONFIG, [401])

        assert result.status == "failed"
        assert "401" in result.error_message
        # une seule tentative de renouvellement : un token neuf refusé n'est
        # pas une expiration
        assert len(token_endpoint) == 2
        assert len(session.calls) == 2

    def test_a_static_credential_is_never_renewed(self, http, delta, monkeypatch):
        result, session = self._run(monkeypatch, self.STATIC_CONFIG, [401])

        assert result.status == "failed"
        assert "401" in result.error_message
        # échec immédiat : rejouer un credential statique ne le corrigerait pas
        assert len(session.calls) == 1

    def test_the_query_string_is_still_stripped_from_the_401(
        self, http, delta, token_endpoint, monkeypatch
    ):
        config = {**self.SP_CONFIG, "params": {"api_key": "SECRET"}}
        result, _ = self._run(monkeypatch, config, [401])

        assert result.status == "failed"
        assert "SECRET" not in result.error_message


class TestTypeWarnings:
    """Une colonne dégradée à l'écriture ne doit pas rester invisible sous un
    run `success`."""

    def test_a_degraded_column_surfaces_in_the_result(self, http, delta):
        delta["append_result"] = ({}, ["colonne 'n' : écrite en texte"])
        http.next_payloads = [[{"n": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert result.warnings == ["colonne 'n' : écrite en texte"]

    def test_the_same_degradation_is_reported_once_per_run(self, http, delta):
        delta["append_result"] = ({}, ["colonne 'n' : écrite en texte"])
        config = {**BASE_CONFIG, "batch_size": 1}
        http.next_payloads = [[{"n": 1}, {"n": 2}, {"n": 3}]]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert len(delta["append"]) == 3
        assert result.warnings == ["colonne 'n' : écrite en texte"]

    def test_a_clean_run_carries_no_warning(self, http, delta):
        http.next_payloads = [[{"n": 1}]]
        result = run_source(BASE_CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert result.warnings == []

    def test_the_types_of_the_first_batch_are_passed_to_the_next(self, http, delta):
        import arro3.core as ac

        delta["append_result"] = ({"n": ac.DataType.int64()}, [])
        config = {**BASE_CONFIG, "batch_size": 1}
        http.next_payloads = [[{"n": 1}, {"n": 2}]]
        run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert delta["append"][0]["known_types"] == {}
        assert delta["append"][1]["known_types"] == {"n": ac.DataType.int64()}

    def test_warnings_survive_a_failed_run(self, http, delta):
        delta["append_result"] = ({}, ["colonne 'n' : écrite en texte"])
        config = {
            **BASE_CONFIG,
            "batch_size": 1,
            "pagination": {"type": "offset", "limit": 1},
        }

        class HalfWay(FakeSession):
            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url})
                if len(self.calls) == 1:
                    return FakeResponse([{"n": 1}])
                return FakeResponse({}, status_code=403)

        import flume_lib.source as source_module

        source_module.requests.Session = HalfWay
        try:
            result = run_source(config, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "failed"
        assert result.warnings == ["colonne 'n' : écrite en texte"]


class TestKeysetInBody:
    """Le cas SQL-over-REST : la clé de pagination vit dans la requête
    elle-même, pas en query string. C'est ce qui permet de dépasser le
    plafond d'offset des APIs qui en imposent un."""

    CONFIG = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {
            "q": "select id, amount from transactions "
                 "where id > {since_id} order by id"
        },
        "pagination": {
            "type": "keyset",
            "key_field": "id",
            "key_param": "since_id",
            "params_in": "body_template",
            "value_format": "numeric",
            "initial_value": 0,
            "limit": 2,
            "limit_param": "rows",
            "items_field": "items",
        },
    }

    def test_the_key_is_substituted_into_the_query(self, http, delta):
        http.next_payloads = [
            {"items": [{"id": 1}, {"id": 2}]},
            {"items": [{"id": 3}]},
        ]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 3
        queries = [c["json"]["q"] for c in http.instances[0].calls]
        assert queries[0].endswith("where id > 0 order by id")
        assert queries[1].endswith("where id > 2 order by id")

    def test_the_key_stays_out_of_the_query_string(self, http, delta):
        http.next_payloads = [{"items": [{"id": 1}]}]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        call = http.instances[0].calls[0]
        # la clé est dans le SQL, pas dans l'URL
        assert "since_id" not in call.get("params", {})
        assert "since_id" not in call["url"]

    def test_the_page_size_does_reach_the_api(self, http, delta):
        """Le placeholder de la clé est dans le corps, celui de `limit_param`
        n'y est pas : il part donc en query string. Sans cette répartition, la
        taille de page était silencieusement perdue."""
        http.next_payloads = [{"items": [{"id": 1}]}]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[0].calls[0]["params"] == {"rows": 2}

    def test_fixed_params_reach_the_api_too(self, http, delta):
        config = {**self.CONFIG, "params": {"status": "open"}}
        http.next_payloads = [{"items": [{"id": 1}]}]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert http.instances[0].calls[0]["params"] == {"status": "open", "rows": 2}

    def test_the_watermark_can_take_the_query_string(self, http, delta):
        """La clé dans le SQL et le watermark en query string : les deux
        canaux sont utilisables en même temps."""
        config = {
            **self.CONFIG,
            "incremental": {
                "enabled": True, "field": "ts", "param_name": "since",
                "initial_value": "2026-01-01",
            },
        }
        http.next_payloads = [{"items": [{"id": 1, "ts": "2026-02-01"}]}]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        params = http.instances[0].calls[0]["params"]
        assert params["since"] == "2026-01-01"
        assert "since_id" not in params
        assert "id > 0" in http.instances[0].calls[0]["json"]["q"]

    def test_a_page_shorter_than_the_limit_ends_the_run(self, http, delta):
        """Conséquence directe : la lib connaît la vraie taille de page, donc
        elle reconnaît la dernière page au lieu de rappeler l'API."""
        http.next_payloads = [
            {"items": [{"id": 1}, {"id": 2}]},
            {"items": [{"id": 3}]},
        ]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 3
        assert len(http.instances[0].calls) == 2

    def test_a_hostile_key_fails_the_run(self, http, delta):
        http.next_payloads = [{"items": [{"id": 1}, {"id": "0 OR 1=1"}]}]
        result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "numeric" in result.error_message
        # la clé hostile n'a jamais été envoyée
        assert len(http.instances[0].calls) == 1

    def test_the_watermark_and_the_key_share_the_body(self, http, delta):
        config = {
            **self.CONFIG,
            "body": {
                "q": "select id from t where ts > '{watermark}' "
                     "and id > {since_id} order by id"
            },
            "incremental": {
                "enabled": True,
                "field": "ts",
                "inject": "body_template",
                "value_format": "iso_datetime",
                "initial_value": "2026-01-01T00:00:00Z",
            },
        }
        http.next_payloads = [{"items": [{"id": 1, "ts": "2026-02-01T00:00:00Z"}]}]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "success", result.error_message
        query = http.instances[0].calls[0]["json"]["q"]
        assert "ts > '2026-01-01T00:00:00Z'" in query
        assert "id > 0" in query
        assert delta["watermark_write"] == [("s1", "2026-02-01T00:00:00Z")]

    def test_a_typo_in_a_placeholder_fails_the_run(self, http, delta):
        config = {
            **self.CONFIG,
            "body": {"q": "select id from t where id > {sinceid}"},
        }
        http.next_payloads = [{"items": [{"id": 1}]}]
        result = run_source(config, lakehouse_tables_path=TABLES_PATH)

        assert result.status == "failed"
        assert "sinceid" in result.error_message


class TestPageSizeIsNotLost:
    """Régression : en mode body_template, la taille de page déclarée n'était
    jamais envoyée. L'API servait la sienne, la lib croyait dicter la sienne,
    et la première page « partielle » arrêtait le run — statut success, une
    fraction des données."""

    CONFIG = {
        **BASE_CONFIG,
        "method": "POST",
        "body": {"q": "select id from t where id > {last_id} order by id"},
        "pagination": {
            "type": "keyset",
            "key_field": "id",
            "key_param": "last_id",
            "params_in": "body_template",
            "value_format": "numeric",
            "initial_value": 0,
            "items_field": "items",
            "limit": 1000,
            "limit_param": "limit",
        },
    }

    def test_the_declared_page_size_is_sent(self, http, delta):
        http.next_payloads = [{"items": [{"id": 1}]}]
        run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        assert http.instances[0].calls[0]["params"] == {"limit": 1000}

    def test_a_full_backfill_is_not_cut_short(self, http, delta):
        """L'API honore le `limit` reçu : les pages font 1 000 lignes et le
        run va jusqu'au bout au lieu de s'arrêter sur la première."""

        class Paged(FakeSession):
            served = 0

            def request(self, method, url, **kwargs):
                self.calls.append({"method": method, "url": url, **kwargs})
                size = int(kwargs["params"]["limit"])
                if Paged.served >= 2500:
                    return FakeResponse({"items": []})
                start = Paged.served + 1
                count = min(size, 2500 - Paged.served)
                Paged.served += count
                return FakeResponse(
                    {"items": [{"id": i} for i in range(start, start + count)]}
                )

        Paged.served = 0
        import flume_lib.source as source_module

        source_module.requests.Session = Paged
        try:
            result = run_source(self.CONFIG, lakehouse_tables_path=TABLES_PATH)
        finally:
            source_module.requests.Session = FakeSession

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 2500
