"""`run_sources` : l'isolation entre sources, l'ordre, l'arrêt, le parallélisme.

Le parallélisme est testé sur de vraies tables Delta : le risque n'est pas dans
la boucle, il est dans les commits concurrents sur les tables techniques que
toutes les sources partagent."""

import threading
import time

import pytest

import flume_lib
from flume_lib import RunSourcesError, SourcesResult, run_sources
from flume_lib._delta import query_table, read_table_types


class FakeResponse:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.headers = {}
        self.text = str(payload)

    def json(self):
        return self._payload

    def close(self):
        pass


class FakeSession:
    """Répond selon l'URL demandée : `routes[url]` est un statut, ou une
    fonction qui reçoit l'URL et rend (statut, payload)."""

    routes: dict = {}
    seen: list = []

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        FakeSession.seen.append(url)
        route = FakeSession.routes.get(url, 200)
        if callable(route):
            status, payload = route(url)
        else:
            status, payload = route, {"items": [{"id": 1, "day": "2026-01-02"}]}
        return FakeResponse(status, payload)


@pytest.fixture(autouse=True)
def http(monkeypatch):
    FakeSession.routes, FakeSession.seen = {}, []
    monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)


def source(name, **extra):
    return {
        "name": name,
        "base_url": f"https://api.test/{name}",
        "target_schema": "bronze",
        "target_table": name,
        "pagination": {"type": "none", "items_field": "items"},
        **extra,
    }


def run(configs, tmp_path, **kwargs):
    return run_sources(configs, lakehouse_tables_path=str(tmp_path), **kwargs)


class TestSequential:
    def test_results_follow_the_order_given(self, tmp_path):
        outcome = run([source("c"), source("a"), source("b")], tmp_path)

        assert isinstance(outcome, SourcesResult)
        assert [r.source_name for r in outcome.results] == ["c", "a", "b"]
        assert outcome.ok and outcome.skipped == []
        assert outcome.rows_loaded == 3
        assert FakeSession.seen == [
            "https://api.test/c", "https://api.test/a", "https://api.test/b",
        ]

    def test_a_failing_source_does_not_stop_the_others(self, tmp_path):
        FakeSession.routes["https://api.test/b"] = 404
        outcome = run([source("a"), source("b"), source("c")], tmp_path)

        assert [r.status for r in outcome.results] == ["success", "failed", "success"]
        assert [r.source_name for r in outcome.failed] == ["b"]
        assert [r.source_name for r in outcome.succeeded] == ["a", "c"]
        assert not outcome.ok

    def test_stop_on_failure_skips_the_rest_and_names_them(self, tmp_path):
        FakeSession.routes["https://api.test/b"] = 404
        outcome = run(
            [source("a"), source("b"), source("c"), source("d")], tmp_path, stop_on_failure=True
        )

        assert [r.source_name for r in outcome.results] == ["a", "b"]
        assert outcome.skipped == ["c", "d"]
        assert FakeSession.seen == ["https://api.test/a", "https://api.test/b"]
        assert not outcome.ok

    def test_stop_on_failure_is_inert_when_nothing_fails(self, tmp_path):
        outcome = run([source("a"), source("b")], tmp_path, stop_on_failure=True)
        assert outcome.ok and len(outcome.results) == 2

    def test_an_empty_list_is_an_empty_outcome(self, tmp_path):
        outcome = run([], tmp_path)
        assert outcome.results == [] and outcome.ok and outcome.rows_loaded == 0

    def test_dry_run_is_passed_down_and_writes_nothing(self, tmp_path):
        outcome = run([source("a")], tmp_path, dry_run=True)
        assert outcome.results[0].status == "success"
        assert not (tmp_path / "bronze").exists()
        assert not (tmp_path / "flume").exists()

    def test_each_run_is_logged(self, tmp_path):
        run([source("a"), source("b")], tmp_path)
        rows = query_table(
            str(tmp_path / "flume" / "log_runs"), "select source_name from t", alias="t"
        )
        assert sorted(r["source_name"] for r in rows) == ["a", "b"]


class TestFailingTheNotebook:
    def test_raise_for_failures_is_silent_when_all_went_well(self, tmp_path):
        run([source("a")], tmp_path).raise_for_failures()

    def test_it_raises_with_the_summary_and_the_outcome(self, tmp_path):
        FakeSession.routes["https://api.test/b"] = 404
        outcome = run([source("a"), source("b")], tmp_path)

        with pytest.raises(RunSourcesError) as caught:
            outcome.raise_for_failures()

        assert caught.value.outcome is outcome
        assert "1 failed" in str(caught.value)
        assert "FAILED" in str(caught.value) and "b" in str(caught.value)

    def test_a_skipped_source_also_fails_the_notebook(self, tmp_path):
        FakeSession.routes["https://api.test/a"] = 404
        outcome = run([source("a"), source("b")], tmp_path, stop_on_failure=True)
        with pytest.raises(RunSourcesError, match="1 skipped"):
            outcome.raise_for_failures()


class TestSummary:
    def test_one_line_per_source_after_the_total(self, tmp_path):
        FakeSession.routes["https://api.test/b"] = 404
        outcome = run([source("a"), source("b"), source("c")], tmp_path, stop_on_failure=True)
        lines = str(outcome).splitlines()

        assert lines[0].startswith("3 source(s): 1 succeeded, 1 failed, 1 skipped — 1 row(s)")
        assert lines[1].lstrip().startswith("ok")
        assert "a  1 row(s), 1 page(s)" in lines[1]
        assert lines[2].lstrip().startswith("FAILED") and "HTTPError" in lines[2]
        assert lines[3].strip() == "skipped  c"

    def test_the_error_is_cut_in_the_summary_not_in_the_result(self, tmp_path):
        from flume_lib.orchestration import SUMMARY_ERROR_CHARS
        from flume_lib.source import RunResult

        long_error = "x" * 500
        failed = RunResult("s", "failed", 0, long_error, "a", "b")
        outcome = SourcesResult([failed])

        assert "x" * SUMMARY_ERROR_CHARS in outcome.summary()
        assert "x" * (SUMMARY_ERROR_CHARS + 1) not in outcome.summary()
        assert failed.error_message == long_error

    def test_warnings_are_counted(self, tmp_path):
        from flume_lib.source import RunResult

        r = RunResult("s", "success", 1, None, "a", "b", warnings=["w1", "w2"])
        assert "2 warning(s)" in SourcesResult([r]).summary()


class TestArguments:
    @pytest.mark.parametrize("bad", ["a", {"name": "a"}, None, 3])
    def test_configs_must_be_a_list(self, tmp_path, bad):
        with pytest.raises(ValueError, match="list of source configurations"):
            run(bad, tmp_path)

    def test_a_config_must_be_an_object(self, tmp_path):
        with pytest.raises(ValueError, match=r"configs\[1\] is not a source configuration"):
            run([source("a"), "oops"], tmp_path)

    @pytest.mark.parametrize("name", [None, "", "  ", 5])
    def test_a_name_is_required(self, tmp_path, name):
        config = source("a")
        config["name"] = name
        with pytest.raises(ValueError, match=r"configs\[0\] needs a 'name'"):
            run([config], tmp_path)

    def test_two_sources_cannot_share_a_name(self, tmp_path):
        with pytest.raises(ValueError, match=r"named 'a' \(configs\[0\] and configs\[1\]\)"):
            run([source("a"), source("a")], tmp_path)

    @pytest.mark.parametrize("workers", [0, -1, True, "2", 1.5])
    def test_max_workers_must_be_a_positive_integer(self, tmp_path, workers):
        with pytest.raises(ValueError, match="max_workers"):
            run([source("a")], tmp_path, max_workers=workers)

    def test_stop_on_failure_must_be_a_boolean(self, tmp_path):
        with pytest.raises(ValueError, match="stop_on_failure"):
            run([source("a")], tmp_path, stop_on_failure="yes")

    def test_on_progress_must_be_callable(self, tmp_path):
        with pytest.raises(ValueError, match="on_progress"):
            run([source("a")], tmp_path, on_progress="print")

    def test_nothing_runs_when_the_arguments_are_refused(self, tmp_path):
        with pytest.raises(ValueError):
            run([source("a"), source("a")], tmp_path)
        assert FakeSession.seen == []

    def test_two_sources_on_one_table_are_refused_in_parallel(self, tmp_path):
        a, b = source("a"), source("b")
        b["target_table"] = "a"
        with pytest.raises(ValueError, match="write the same table bronze.a"):
            run([a, b], tmp_path, max_workers=2)
        assert FakeSession.seen == []

    def test_two_sources_on_one_table_are_fine_one_after_the_other(self, tmp_path):
        a, b = source("a"), source("b")
        b["target_table"] = "a"
        outcome = run([a, b], tmp_path)
        assert outcome.ok

    def test_an_invalid_config_is_a_failed_run_not_an_exception(self, tmp_path):
        bad = source("bad")
        bad["nonsense"] = 1
        outcome = run([bad, source("good")], tmp_path)
        assert [r.status for r in outcome.results] == ["failed", "success"]
        assert "ConfigError" in outcome.results[0].error_message


class TestOnProgress:
    def test_the_source_name_is_prepended(self, tmp_path):
        seen = []
        run(
            [source("a"), source("b")], tmp_path,
            on_progress=lambda name, pages, rows: seen.append((name, pages, rows)),
        )
        assert seen == [("a", 1, 1), ("b", 1, 1)]


class TestParallel:
    def test_results_keep_the_input_order_whatever_finishes_first(self, tmp_path):
        slow_done = threading.Event()

        def slow(url):
            time.sleep(0.3)
            slow_done.set()
            return 200, {"items": [{"id": 1}]}

        def fast(url):
            # `fast` termine avant `slow`
            assert not slow_done.is_set()
            return 200, {"items": [{"id": 1}]}

        FakeSession.routes["https://api.test/slow"] = slow
        FakeSession.routes["https://api.test/fast"] = fast
        outcome = run([source("slow"), source("fast")], tmp_path, max_workers=2)

        assert [r.source_name for r in outcome.results] == ["slow", "fast"]
        assert outcome.ok

    def test_many_sources_write_their_own_tables_and_share_the_technical_ones(self, tmp_path):
        """Le cas qui compte : le premier run d'un lakehouse neuf, quand
        `log_runs` et `watermark` n'existent pas encore et que six sources les
        créent en même temps."""
        names = [f"s{i}" for i in range(6)]
        configs = [
            source(
                n,
                params={},
                incremental={
                    "enabled": True, "field": "day", "param_name": "since",
                    "value_format": "iso_date", "initial_value": "2026-01-01",
                },
            )
            for n in names
        ]
        outcome = run(configs, tmp_path, max_workers=6)

        assert [r.error_message for r in outcome.results] == [None] * 6
        assert outcome.ok
        runs = query_table(
            str(tmp_path / "flume" / "log_runs"), "select source_name from t", alias="t"
        )
        assert sorted(r["source_name"] for r in runs) == names
        for n in names:
            assert flume_lib.read_watermark(str(tmp_path), n) == "2026-01-02"
            rows = query_table(str(tmp_path / "bronze" / n), "select id from t", alias="t")
            assert len(rows) == 1

    def test_the_technical_tables_keep_their_types_under_concurrency(self, tmp_path):
        configs = [source(f"s{i}") for i in range(4)]
        run(configs, tmp_path, max_workers=4)
        types = {
            k: str(v).strip()
            for k, v in read_table_types(str(tmp_path / "flume" / "log_runs")).items()
        }
        assert types["rows_loaded"].endswith("Int64>")
        assert types["rows_inserted"].endswith("Int64>")

    def test_stop_on_failure_keeps_queued_sources_from_starting(self, tmp_path):
        gate = threading.Event()
        threading.Timer(0.4, gate.set).start()

        def held(url):
            gate.wait(5)
            return 200, {"items": [{"id": 1}]}

        FakeSession.routes["https://api.test/bad"] = 404
        FakeSession.routes["https://api.test/held"] = held
        outcome = run(
            [source("bad"), source("held"), source("late1"), source("late2")],
            tmp_path, max_workers=2, stop_on_failure=True,
        )

        assert [r.source_name for r in outcome.results] == ["bad", "held"]
        assert outcome.skipped == ["late1", "late2"]
        assert "https://api.test/late1" not in FakeSession.seen

    def test_an_interruption_in_a_worker_reaches_the_caller(self, tmp_path):
        def interrupted(url):
            raise KeyboardInterrupt

        FakeSession.routes["https://api.test/a"] = interrupted
        with pytest.raises(KeyboardInterrupt):
            run([source("a"), source("b")], tmp_path, max_workers=2)
