"""Ce qu'un run laisse derrière lui : les colonnes de détail de `log_runs`, les
compteurs de `RunResult`, et le callback `on_progress`.

`log_runs` est écrite sur de vraies tables Delta : le risque est dans les types
(une colonne NULL au premier run ne doit pas se figer en texte) et dans
l'évolution d'une table créée par une version antérieure."""

import pytest

import flume_lib
from flume_lib import run_source, run_source_windows
from flume_lib._delta import query_table, read_table_types, write_records


class FakeResponse:
    def __init__(self, status, payload, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = str(payload)

    def json(self):
        return self._payload

    def close(self):
        pass


class FakeSession:
    script: list = []
    calls: list = []

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        FakeSession.calls.append(kwargs.get("params"))
        status, payload, headers = FakeSession.script.pop(0)
        return FakeResponse(status, payload, headers)


def ok(items):
    return (200, {"items": items}, {})


@pytest.fixture(autouse=True)
def http(monkeypatch):
    FakeSession.script, FakeSession.calls = [], []
    monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)


@pytest.fixture
def config():
    return {
        "name": "ledger",
        "base_url": "https://api.test/ledger",
        "target_schema": "bronze",
        "target_table": "ledger",
        "pagination": {"type": "none", "items_field": "items"},
    }


def paged(config):
    """Pagination par offset de 2 : deux pages pleines puis une partielle."""
    config["pagination"] = {"type": "offset", "limit": 2, "items_field": "items"}
    return config


def log(tmp_path, order="start_ts"):
    return query_table(
        str(tmp_path / "flume" / "log_runs"), f"select * from t order by {order}", alias="t"
    )


def run(config, tmp_path, **kwargs):
    return run_source(config, lakehouse_tables_path=str(tmp_path), **kwargs)


class TestRunResultCounters:
    def test_pages_are_the_data_responses_received(self, tmp_path, config):
        FakeSession.script = [
            ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}, {"id": 4}]), ok([{"id": 5}]),
        ]
        result = run(paged(config), tmp_path)

        assert result.status == "success", result.error_message
        assert (result.pages, result.rows_loaded, result.retries) == (3, 5, 0)
        assert result.token_renewals == 0
        assert result.duration_seconds >= 0

    def test_a_replayed_request_is_a_retry_not_a_page(self, tmp_path, config):
        FakeSession.script = [
            (503, {}, {"Retry-After": "0"}),
            (429, {}, {"Retry-After": "0"}),
            ok([{"id": 1}]),
        ]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert (result.pages, result.retries) == (1, 2)

    def test_a_failed_run_keeps_the_counters_it_reached(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), (404, {"error": "nope"}, {})]
        result = run(paged(config), tmp_path)

        assert result.status == "failed"
        assert result.pages == 1
        # la première page attendait encore dans le tampon d'un lot : rien n'est écrit
        assert result.rows_loaded == 0

    def test_a_run_that_fails_before_its_first_request_reports_zeros(self, tmp_path, config):
        config["nonsense"] = 1
        result = run(config, tmp_path)
        assert result.status == "failed"
        assert (result.pages, result.retries, result.token_renewals) == (0, 0, 0)

    def test_a_401_that_renews_the_token_is_counted(self, tmp_path, config, monkeypatch):
        monkeypatch.setenv("CID", "id-value-123")
        monkeypatch.setenv("CS", "secret-value-123")
        config["auth"] = {
            "type": "oauth2_client_credentials",
            "token_url": "https://idp.test/token",
            "client_id": {"env_var": "CID"},
            "client_secret": {"env_var": "CS"},
        }
        monkeypatch.setattr(
            "flume_lib.auth._build_headers",
            lambda cfg, network=None: ({"Authorization": "Bearer t"}, None),
        )
        FakeSession.script = [(401, {}, {}), ok([{"id": 1}])]

        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert result.token_renewals == 1
        assert result.retries == 1  # le rejeu qui a suivi le renouvellement

    def test_a_static_credential_never_renews(self, tmp_path, config, monkeypatch):
        monkeypatch.setenv("TOK", "token-value-123")
        config["auth"] = {"type": "bearer_token", "token": {"env_var": "TOK"}}
        FakeSession.script = [ok([{"id": 1}])]
        assert run(config, tmp_path).token_renewals == 0


class TestLogRunsDetail:
    def test_the_detail_columns_are_written(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}])]
        result = run(paged(config), tmp_path)

        row = log(tmp_path)[0]
        assert row["run_id"] == result.run_id
        assert row["pages"] == 2
        assert row["retries"] == 0
        assert row["token_renewals"] == 0
        assert row["target_table"] == "bronze.ledger"
        assert row["write_mode"] == "append"
        assert row["lib_version"] == flume_lib.__version__
        assert row["duration_seconds"] == result.duration_seconds
        assert row["warnings"] is None

    def test_warnings_reach_the_lakehouse(self, tmp_path, config):
        """Le warning d'un backfill qui n'a rien remplacé n'existait que dans le
        notebook : relu trois jours plus tard, il n'était nulle part."""
        config["write"] = {"mode": "replace_where", "replace_where": "day = '2026-01-01'"}
        FakeSession.script = [ok([])]
        result = run(config, tmp_path)

        assert result.warnings
        assert "nothing was replaced" in log(tmp_path)[0]["warnings"]

    def test_watermark_and_window_are_written(self, tmp_path, config):
        config["params"] = {"to": "2026-02-01"}
        config["incremental"] = {
            "enabled": True, "field": "day", "param_name": "from", "value_format": "iso_date",
            "initial_value": "2026-01-01",
            "window": {"upper_param": "to", "upper_value": "2026-02-01"},
            "advance": "upper_bound",
        }
        FakeSession.script = [ok([{"id": 1, "day": "2026-01-10"}])]
        result = run(config, tmp_path)

        row = log(tmp_path)[0]
        assert result.status == "success", result.error_message
        assert row["watermark_before"] == "2026-01-01"
        assert row["watermark_after"] == "2026-02-01"
        assert row["window_end"] == "2026-02-01"

    def test_merge_counts_are_written(self, tmp_path, config):
        config["write"] = {"mode": "merge", "merge_keys": ["id"]}
        FakeSession.script = [ok([{"id": 1}])]
        run(config, tmp_path)
        FakeSession.script = [ok([{"id": 1}, {"id": 2}])]
        run(config, tmp_path)

        rows = log(tmp_path)
        assert [(r["rows_inserted"], r["rows_updated"]) for r in rows] == [(1, 0), (1, 1)]
        assert all(r["write_mode"] == "merge" for r in rows)

    def test_a_failed_run_is_logged_with_its_error_and_counters(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), (404, {}, {})]
        run(paged(config), tmp_path)

        row = log(tmp_path)[0]
        assert row["status"] == "failed"
        assert "404" in row["error_message"]
        assert row["pages"] == 1

    def test_a_column_null_at_first_does_not_freeze_as_text(self, tmp_path, config):
        """`rows_inserted` est NULL hors mode merge : l'inférence l'aurait typée
        texte au premier run, et les entiers du premier merge y auraient été
        coercés en texte sans erreur."""
        FakeSession.script = [ok([{"id": 1}])]
        run(config, tmp_path)  # append : rows_inserted NULL
        config["write"] = {"mode": "merge", "merge_keys": ["id"]}
        FakeSession.script = [ok([{"id": 1}])]
        run(config, tmp_path)

        uri = str(tmp_path / "flume" / "log_runs")
        types = {k: str(v).strip() for k, v in read_table_types(uri).items()}
        assert types["rows_inserted"].endswith("Int64>")
        assert types["rows_updated"].endswith("Int64>")
        assert types["duration_seconds"].endswith("Float64>")
        assert [r["rows_updated"] for r in log(tmp_path)] == [None, 1]

    def test_a_table_from_an_older_version_gains_the_columns(self, tmp_path, config):
        uri = str(tmp_path / "flume" / "log_runs")
        write_records(uri, [{
            "run_id": "old", "source_name": "ledger", "start_ts": "2026-01-01T00:00:00+00:00",
            "end_ts": "2026-01-01T00:01:00+00:00", "status": "success", "rows_loaded": 3,
            "error_message": None,
        }])
        FakeSession.script = [ok([{"id": 1}])]
        run(config, tmp_path)

        rows = log(tmp_path)
        assert [r["run_id"] for r in rows][0] == "old"
        assert rows[0]["pages"] is None and rows[0]["lib_version"] is None  # ancienne ligne
        assert rows[1]["pages"] == 1 and rows[1]["lib_version"] == flume_lib.__version__

    def test_a_very_long_warning_list_is_capped(self, tmp_path, config):
        from flume_lib.source import MAX_RUN_MESSAGE_CHARS, RunResult, _run_details

        result = RunResult("s", "success", 0, None, "a", "b", warnings=["x" * 900] * 5)
        warnings = _run_details(result, config)["warnings"]
        assert len(warnings) == MAX_RUN_MESSAGE_CHARS
        assert warnings.endswith("…")

    def test_an_unknown_detail_key_is_ignored(self, tmp_path):
        from flume_lib.logging_ import write_log_run

        write_log_run(
            str(tmp_path), "r", "s", "a", "b", "success", 1, None, details={"surprise": 1}
        )
        assert "surprise" not in read_table_types(str(tmp_path / "flume" / "log_runs"))


class TestOnProgress:
    def test_it_is_called_after_each_page_with_running_totals(self, tmp_path, config):
        FakeSession.script = [
            ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}, {"id": 4}]), ok([{"id": 5}]),
        ]
        seen = []
        run(paged(config), tmp_path, on_progress=lambda pages, rows: seen.append((pages, rows)))
        assert seen == [(1, 2), (2, 4), (3, 5)]

    def test_it_works_in_a_dry_run(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}])]
        seen = []
        result = run(
            paged(config), tmp_path, dry_run=True, on_progress=lambda p, r: seen.append((p, r))
        )
        assert result.status == "success"
        assert seen == [(1, 2), (2, 3)]

    def test_a_callback_that_raises_does_not_fail_the_run(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}])]
        calls = []

        def broken(pages, rows):
            calls.append(pages)
            raise RuntimeError("boom")

        result = run(paged(config), tmp_path, on_progress=broken)

        assert result.status == "success", result.error_message
        assert result.rows_loaded == 3
        assert calls == [1]  # abandonné après le premier échec
        assert result.warnings == [
            "on_progress raised RuntimeError and was disabled for the rest of the run"
        ]

    def test_the_exception_message_is_not_copied_into_the_warning(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}])]

        def broken(pages, rows):
            raise RuntimeError("token=abc123")

        result = run(config, tmp_path, on_progress=broken)
        assert "abc123" not in " ".join(result.warnings)

    def test_something_that_is_not_callable_fails_the_run(self, tmp_path, config):
        result = run(config, tmp_path, on_progress="print")
        assert result.status == "failed"
        assert "on_progress must be a function" in result.error_message

    def test_an_interruption_from_the_callback_stops_the_run_and_is_logged(self, tmp_path, config):
        FakeSession.script = [ok([{"id": 1}, {"id": 2}]), ok([{"id": 3}])]

        def stop(pages, rows):
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            run(paged(config), tmp_path, on_progress=stop)
        assert log(tmp_path)[0]["status"] == "failed"

    def test_run_source_windows_passes_it_on(self, tmp_path, monkeypatch):
        seen = []

        def fake_run_source(config, **kwargs):
            seen.append(kwargs["on_progress"])
            raise AssertionError("stop here")

        monkeypatch.setattr("flume_lib.windows.run_source", fake_run_source)
        monkeypatch.setattr("flume_lib.windows.read_watermark", lambda *a, **k: "2026-01-01")

        def factory(window_end):
            return {"name": "s", "incremental": {"initial_value": "2026-01-01"}}

        def callback(pages, rows):
            return None

        with pytest.raises(AssertionError, match="stop here"):
            run_source_windows(
                factory, "2026-02-01", 7, lakehouse_tables_path=str(tmp_path), on_progress=callback
            )
        assert seen == [callback]
