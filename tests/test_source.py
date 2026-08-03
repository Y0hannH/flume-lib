"""Tests de run_source : traçabilité, dry-run, POST, remontée des erreurs.
Aucun appel réseau ni écriture Delta réels — requests.Session et les helpers
Delta sont mockés."""

import pytest

from flume_lib.source import LINEAGE_INGESTED_AT, LINEAGE_RUN_ID, run_source

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
    calls = {"append": [], "log": [], "watermark_write": [], "watermark_read": []}

    def fake_append(uri, records, **kwargs):
        calls["append"].append({"uri": uri, "records": records})

    def fake_log(path, **kwargs):
        calls["log"].append(kwargs)

    def fake_write_wm(path, source_name, last_value, **kwargs):
        calls["watermark_write"].append((source_name, last_value))

    def fake_read_wm(path, source_name, **kwargs):
        calls["watermark_read"].append(source_name)
        return None

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
