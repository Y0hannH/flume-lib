"""`compact_data_tables` sur de vraies tables : ce que delta-rs fait d'un
OPTIMIZE, pas ce qu'un mock en dirait."""

import pytest

import flume_lib
from flume_lib import compact_data_tables, run_source
from flume_lib import maintenance as maintenance_module
from flume_lib._delta import query_table, write_records


def table(tmp_path, name, appends, schema="bronze", **kwargs):
    uri = f"{tmp_path}/{schema}/{name}"
    for i in range(appends):
        write_records(uri, [{"n": i, "p": "a" if i % 2 else "b"}], **kwargs)
    return uri


def count(uri):
    return len(query_table(uri, "select n from t", alias="t"))


class TestCompaction:
    def test_small_files_are_merged_and_no_row_is_lost(self, tmp_path):
        uri = table(tmp_path, "orders", 6)

        report = compact_data_tables(["bronze.orders"], str(tmp_path))

        assert report == {"bronze.orders": {"files_removed": 6, "files_added": 1}}
        assert count(uri) == 6

    def test_compacting_twice_has_nothing_left_to_do(self, tmp_path):
        table(tmp_path, "orders", 4)
        compact_data_tables(["bronze.orders"], str(tmp_path))
        again = compact_data_tables(["bronze.orders"], str(tmp_path))
        assert again["bronze.orders"] == {"files_removed": 0, "files_added": 0}

    def test_several_tables_and_both_ways_to_name_them(self, tmp_path):
        table(tmp_path, "orders", 3)
        table(tmp_path, "invoices", 3, schema="silver")

        report = compact_data_tables(["bronze.orders", ("silver", "invoices")], str(tmp_path))

        assert set(report) == {"bronze.orders", "silver.invoices"}
        assert all(r["files_added"] == 1 for r in report.values())

    def test_a_partitioned_table_keeps_its_partitions(self, tmp_path):
        uri = table(tmp_path, "parts", 6, partition_by=["p"])

        report = compact_data_tables(["bronze.parts"], str(tmp_path))

        assert report["bronze.parts"]["files_added"] == 2  # un fichier par partition
        assert count(uri) == 6

    def test_a_table_written_by_a_run_with_small_batches(self, tmp_path, monkeypatch):
        class Session:
            def __init__(self):
                self.headers = {}

            def close(self):
                pass

            def request(self, method, url, **kwargs):
                response = type("R", (), {})()
                response.status_code, response.headers = 200, {}
                response.json = lambda: {"items": [{"n": i} for i in range(10)]}
                return response

        monkeypatch.setattr("flume_lib.source.requests.Session", Session)
        config = {
            "name": "s", "base_url": "https://api.test/x", "target_schema": "bronze",
            "target_table": "many", "batch_size": 2,
            "pagination": {"type": "none", "items_field": "items"},
        }
        assert run_source(config, lakehouse_tables_path=str(tmp_path)).status == "success"

        report = compact_data_tables(["bronze.many"], str(tmp_path))

        assert report["bronze.many"]["files_removed"] == 5
        assert count(f"{tmp_path}/bronze/many") == 10


class TestVacuum:
    def test_it_is_opt_in(self, tmp_path):
        table(tmp_path, "orders", 3)
        assert "files_vacuumed" not in compact_data_tables(["bronze.orders"], str(tmp_path))[
            "bronze.orders"
        ]

    def test_it_is_reported_and_given_the_retention(self, tmp_path, monkeypatch):
        table(tmp_path, "orders", 3)
        calls = []

        def fake_vacuum(uri, storage_options, retention_hours):
            calls.append(retention_hours)
            return 7

        monkeypatch.setattr(maintenance_module, "vacuum_table", fake_vacuum)
        report = compact_data_tables(
            ["bronze.orders"], str(tmp_path), vacuum=True, retention_hours=336
        )

        assert report["bronze.orders"]["files_vacuumed"] == 7
        assert calls == [336]

    def test_the_real_vacuum_runs_and_keeps_every_row(self, tmp_path):
        uri = table(tmp_path, "orders", 3)
        report = compact_data_tables(["bronze.orders"], str(tmp_path), vacuum=True)
        assert isinstance(report["bronze.orders"]["files_vacuumed"], int)
        assert count(uri) == 3

    @pytest.mark.parametrize("hours", [0, 24, 167, True, 168.0, "168"])
    def test_a_retention_shorter_than_a_week_is_refused(self, tmp_path, hours):
        table(tmp_path, "orders", 2)
        with pytest.raises(ValueError, match="retention_hours"):
            compact_data_tables(["bronze.orders"], str(tmp_path), vacuum=True,
                                retention_hours=hours)


class TestArguments:
    def test_an_absent_table_stops_everything_before_any_work(self, tmp_path):
        table(tmp_path, "orders", 3)
        with pytest.raises(ValueError, match=r"not found: bronze\.ordres"):
            compact_data_tables(["bronze.orders", "bronze.ordres"], str(tmp_path))
        # la table qui existe n'a pas été touchée : ses 3 fichiers sont là
        again = compact_data_tables(["bronze.orders"], str(tmp_path))
        assert again["bronze.orders"]["files_removed"] == 3

    @pytest.mark.parametrize("bad", [[], None, "bronze.orders", 3, {"bronze.orders"}])
    def test_tables_must_be_a_non_empty_list(self, tmp_path, bad):
        with pytest.raises(ValueError, match="non-empty list"):
            compact_data_tables(bad, str(tmp_path))

    @pytest.mark.parametrize("bad", ["orders", "a.b.c", ("only",), 5, "bronze.", ".orders"])
    def test_a_table_is_schema_dot_table(self, tmp_path, bad):
        with pytest.raises(ValueError):
            compact_data_tables([bad], str(tmp_path))

    def test_a_name_cannot_traverse_the_lakehouse(self, tmp_path):
        with pytest.raises(ValueError, match="invalid schema"):
            compact_data_tables([("../x", "orders")], str(tmp_path))

    def test_it_is_exported(self):
        assert flume_lib.compact_data_tables is compact_data_tables
        assert "compact_data_tables" in flume_lib.__all__
