"""Tests de run_source_windows.

Deux niveaux. Les premiers remplacent `run_source` : ils vérifient la boucle
(bornes, arrêts, refus) sans rien écrire. Les derniers laissent tout tourner —
vraie table `watermark`, vraie écriture Delta — contre une API factice qui
renvoie *uniquement* les lignes du jour de la borne basse, le cas qui faisait
tourner la boucle de l'appelant à l'infini."""

from datetime import date

import pytest

from flume_lib import RunResult, WindowRunError, read_watermark, run_source_windows
from flume_lib.windows import _as_date

TABLES = "/tmp/Tables"


def window_config(window_end, advance="upper_bound", name="wd_C1"):
    return {
        "name": name,
        "base_url": "https://api.test/report",
        "target_schema": "bronze",
        "target_table": "invoices",
        "params": {"Company": "C1", "Accounting_Date_To": window_end},
        "pagination": {"type": "none", "items_field": "Report_Entry"},
        "incremental": {
            "enabled": True,
            "field": "Last_Inv_Date",
            "param_name": "Accounting_Date_From",
            "initial_value": "2026-09-01",
            "advance": advance,
            "window": {"upper_param": "Accounting_Date_To", "upper_value": window_end},
        },
    }


def result(before, after, end, status="success", rows=1, error=None):
    return RunResult(
        source_name="wd_C1", status=status, rows_loaded=rows, error_message=error,
        start_ts="t0", end_ts="t1", watermark_before=before, watermark_after=after,
        window_end=end,
    )


class Loop:
    """Remplace read_watermark et run_source par un watermark en mémoire que
    chaque faux run fait avancer à sa guise."""

    def __init__(self, monkeypatch, advance_to=None):
        self.stored = None
        self.configs = []
        self.advance_to = advance_to or (lambda before, end: end)
        monkeypatch.setattr(
            "flume_lib.windows.read_watermark", lambda *a, **k: self.stored
        )
        monkeypatch.setattr("flume_lib.windows.run_source", self.run)

    def run(self, config, **kwargs):
        self.configs.append(config)
        before = self.stored or config["incremental"]["initial_value"]
        end = config["incremental"]["window"]["upper_value"]
        after = self.advance_to(before, end)
        self.stored = after
        return result(before, after, end)


class TestWindowLoop:
    def test_it_walks_the_period_in_spans_and_stops_at_until(self, monkeypatch):
        loop = Loop(monkeypatch)
        results = run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

        ends = [c["incremental"]["window"]["upper_value"] for c in loop.configs]
        # le premier appel de la fabrique (la sonde) ne lance pas de run
        assert ends == ["2026-09-11", "2026-09-21", "2026-09-25"]
        assert len(results) == 3

    def test_the_last_window_is_clamped_to_until(self, monkeypatch):
        loop = Loop(monkeypatch)
        run_source_windows(window_config, "2026-09-05", 31, lakehouse_tables_path=TABLES)

        assert [c["incremental"]["window"]["upper_value"] for c in loop.configs] == ["2026-09-05"]

    def test_a_source_already_at_until_runs_nothing(self, monkeypatch):
        loop = Loop(monkeypatch)
        loop.stored = "2026-09-30"
        results = run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

        assert results == [] and loop.configs == []

    def test_it_resumes_from_the_stored_watermark(self, monkeypatch):
        loop = Loop(monkeypatch)
        loop.stored = "2026-09-20"
        run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

        assert [c["incremental"]["window"]["upper_value"] for c in loop.configs] == ["2026-09-25"]

    def test_a_stalled_watermark_raises(self, monkeypatch):
        Loop(monkeypatch, advance_to=lambda before, end: before)
        with pytest.raises(WindowRunError, match="did not move") as caught:
            run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

        assert len(caught.value.results) == 1

    def test_a_watermark_that_goes_backwards_raises(self, monkeypatch):
        Loop(monkeypatch, advance_to=lambda before, end: "2026-08-01")
        with pytest.raises(WindowRunError, match="did not move"):
            run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

    def test_a_failed_run_raises_with_its_message(self, monkeypatch):
        monkeypatch.setattr("flume_lib.windows.read_watermark", lambda *a, **k: None)
        monkeypatch.setattr(
            "flume_lib.windows.run_source",
            lambda config, **k: result(
                None, None, "2026-09-11", status="failed", error="HTTPError: boom"
            ),
        )
        with pytest.raises(WindowRunError, match="HTTPError: boom"):
            run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

    def test_max_iterations_is_honoured(self, monkeypatch):
        loop = Loop(monkeypatch)
        with pytest.raises(WindowRunError, match="max_iterations=2") as caught:
            run_source_windows(
                window_config, "2026-12-31", 5, max_iterations=2, lakehouse_tables_path=TABLES
            )

        assert len(loop.configs) == 2
        assert len(caught.value.results) == 2

    def test_a_config_without_window_is_refused(self, monkeypatch):
        monkeypatch.setattr("flume_lib.windows.read_watermark", lambda *a, **k: None)
        monkeypatch.setattr(
            "flume_lib.windows.run_source",
            lambda config, **k: result("2026-09-01", "2026-09-05", None),
        )
        with pytest.raises(WindowRunError, match="no incremental.window"):
            run_source_windows(window_config, "2026-09-25", 10, lakehouse_tables_path=TABLES)

    def test_a_source_without_any_start_is_refused(self, monkeypatch):
        monkeypatch.setattr("flume_lib.windows.read_watermark", lambda *a, **k: None)

        def factory(end):
            config = window_config(end)
            del config["incremental"]["initial_value"]
            return config

        with pytest.raises(ValueError, match="initial_value"):
            run_source_windows(factory, "2026-09-25", 10, lakehouse_tables_path=TABLES)

    @pytest.mark.parametrize("span", [0, -3, True, 1.5])
    def test_max_span_days_must_be_a_positive_integer(self, span):
        with pytest.raises(ValueError, match="max_span_days"):
            run_source_windows(window_config, "2026-09-25", span, lakehouse_tables_path=TABLES)

    def test_until_must_be_a_date(self):
        with pytest.raises(ValueError, match="until"):
            run_source_windows(window_config, "tomorrow", 10, lakehouse_tables_path=TABLES)

    def test_until_may_be_a_date_object(self, monkeypatch):
        loop = Loop(monkeypatch)
        run_source_windows(window_config, date(2026, 9, 5), 31, lakehouse_tables_path=TABLES)

        assert len(loop.configs) == 1

    def test_as_date_rejects_a_datetime_string(self):
        with pytest.raises(ValueError):
            _as_date("2026-09-05T10:00:00", "until")


class BoundaryOnlyAPI:
    """Ne renvoie que des lignes datées du jour de la borne basse : c'est ce
    que fait un rapport à borne inclusive quand rien de neuf n'est arrivé."""

    def __init__(self):
        self.calls = []

    def __call__(self):
        api = self

        class Session:
            def __init__(self):
                self.headers = {}

            def close(self):
                pass

            def request(self, method, url, **kwargs):
                params = kwargs["params"]
                api.calls.append(dict(params))
                day = params["Accounting_Date_From"]

                class Response:
                    status_code = 200
                    headers = {}

                    def json(self):
                        return {"Report_Entry": [{"Inv_Ref_ID": "INV-1", "Last_Inv_Date": day}]}

                return Response()

        return Session


class TestEndToEnd:
    def test_boundary_only_rows_do_not_stall_with_upper_bound(self, tmp_path, monkeypatch):
        api = BoundaryOnlyAPI()
        monkeypatch.setattr("flume_lib.source.requests.Session", api())

        results = run_source_windows(
            window_config, "2026-09-20", 7, lakehouse_tables_path=str(tmp_path)
        )

        assert [r.status for r in results] == ["success"] * 3
        assert [c["Accounting_Date_To"] for c in api.calls] == [
            "2026-09-08", "2026-09-15", "2026-09-20",
        ]
        assert read_watermark(str(tmp_path), "wd_C1", schema="flume") == "2026-09-20"

    def test_the_same_data_with_field_max_stalls_and_says_so(self, tmp_path, monkeypatch):
        api = BoundaryOnlyAPI()
        monkeypatch.setattr("flume_lib.source.requests.Session", api())

        def factory(end):
            return window_config(end, advance="field_max")

        with pytest.raises(WindowRunError, match="did not move"):
            run_source_windows(factory, "2026-09-20", 7, lakehouse_tables_path=str(tmp_path))

        # une seule requête : pas de boucle sans fin
        assert len(api.calls) == 1
