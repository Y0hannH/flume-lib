"""Tests du mode `write.mode = "merge"` sur de vraies tables Delta. Un mock de
`merge_records` dirait toujours oui : ce qui compte ici est ce que delta-rs fait
d'un MERGE — correspondance des clés, doublons dans un lot, types, lignage."""

import pytest

from flume_lib._delta import DeltaWriteError, merge_records, query_table
from flume_lib.source import run_source


def rows(uri):
    return query_table(uri, "select * from t order by Inv_Ref_ID", alias="t")


class FakeResponse:
    status_code = 200
    headers = {}

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    payload: list = []

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        return FakeResponse({"Report_Entry": FakeSession.payload})


def invoice(ref, status="Unpaid", lines=None, **extra):
    return {
        "Inv_Ref_ID": ref,
        "Status": status,
        "Line": lines if lines is not None else [{"Inv_Line_Ref_ID": f"{ref}-1"}],
        **extra,
    }


@pytest.fixture(autouse=True)
def http(monkeypatch):
    monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)


@pytest.fixture
def config():
    return {
        "name": "wd_C1",
        "base_url": "https://api.test/report",
        "target_schema": "bronze",
        "target_table": "invoices",
        "pagination": {"type": "none", "items_field": "Report_Entry"},
        "write": {"mode": "merge", "merge_keys": ["Inv_Ref_ID"]},
    }


@pytest.fixture
def target(tmp_path):
    return str(tmp_path / "bronze" / "invoices")


def run(config, tmp_path):
    return run_source(config, lakehouse_tables_path=str(tmp_path))


class TestMergeRuns:
    def test_the_first_run_creates_the_table(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A"), invoice("B")]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert [r["Inv_Ref_ID"] for r in rows(target)] == ["A", "B"]
        assert (result.rows_loaded, result.rows_inserted, result.rows_updated) == (2, 2, 0)

    def test_the_same_window_twice_leaves_the_row_count_unchanged(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A"), invoice("B")]
        run(config, tmp_path)
        second = run(config, tmp_path)

        assert second.status == "success", second.error_message
        assert len(rows(target)) == 2
        assert (second.rows_loaded, second.rows_inserted, second.rows_updated) == (2, 0, 2)

    def test_a_changed_field_is_updated_in_place(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A"), invoice("B")]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", status="Paid")]
        result = run(config, tmp_path)

        assert (result.rows_inserted, result.rows_updated) == (0, 1)
        assert {r["Inv_Ref_ID"]: r["Status"] for r in rows(target)} == {
            "A": "Paid", "B": "Unpaid",
        }

    def test_new_and_known_keys_in_one_run(self, tmp_path, config):
        FakeSession.payload = [invoice("A")]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", status="Paid"), invoice("C")]
        result = run(config, tmp_path)

        assert (result.rows_loaded, result.rows_inserted, result.rows_updated) == (2, 1, 1)

    def test_duplicate_keys_inside_a_batch_keep_the_last_one(self, tmp_path, config, target):
        FakeSession.payload = [
            invoice("A", status="Unpaid"), invoice("A", status="Paid"), invoice("B"),
        ]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", status="v1"), invoice("A", status="v2")]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert {r["Inv_Ref_ID"]: r["Status"] for r in rows(target)} == {"A": "v2", "B": "Unpaid"}
        assert result.rows_loaded == 2          # lignes traitées, avant dédoublonnage
        assert (result.rows_inserted, result.rows_updated) == (0, 1)

    def test_duplicates_in_the_first_batch_of_a_new_table_collapse(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A", status="v1"), invoice("A", status="v2")]
        result = run(config, tmp_path)

        assert [r["Status"] for r in rows(target)] == ["v2"]
        assert result.rows_inserted == 1

    def test_lineage_is_refreshed_on_updated_rows_only(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A"), invoice("B")]
        first = run(config, tmp_path)
        FakeSession.payload = [invoice("A", status="Paid")]
        second = run(config, tmp_path)

        by_ref = {r["Inv_Ref_ID"]: r for r in rows(target)}
        assert by_ref["A"]["_flume_run_id"] == second.run_id
        assert by_ref["B"]["_flume_run_id"] == first.run_id
        assert by_ref["A"]["_flume_ingested_at"] > by_ref["B"]["_flume_ingested_at"]

    def test_a_nested_array_stays_a_json_string(self, tmp_path, config, target):
        two_lines = [{"Inv_Line_Ref_ID": "L1"}, {"Inv_Line_Ref_ID": "L2"}]
        FakeSession.payload = [invoice("A", lines=two_lines)]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", lines=[{"Inv_Line_Ref_ID": "L1"}])]
        run(config, tmp_path)

        (row,) = rows(target)
        assert row["Line"] == '[{"Inv_Line_Ref_ID": "L1"}]'

    def test_batches_of_one_run_merge_into_each_other(self, tmp_path, config, target):
        # la clé du lot 2 existe déjà : insérée par le lot 1, mise à jour par le 2
        config["batch_size"] = 2
        FakeSession.payload = [
            invoice("A", status="v1"), invoice("B"), invoice("A", status="v2"), invoice("C"),
        ]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert {r["Inv_Ref_ID"]: r["Status"] for r in rows(target)}["A"] == "v2"
        assert len(rows(target)) == 3
        assert (result.rows_inserted, result.rows_updated) == (3, 1)

    def test_a_new_column_evolves_the_schema(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A")]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", Amount=12.5), invoice("B", Amount=3.0)]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert {r["Inv_Ref_ID"]: r["Amount"] for r in rows(target)} == {"A": 12.5, "B": 3.0}

    def test_an_integer_batch_merges_into_a_float_column(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A", Amount=1.5)]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", Amount=7), invoice("B", Amount=2)]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert {r["Inv_Ref_ID"]: r["Amount"] for r in rows(target)} == {"A": 7.0, "B": 2.0}

    def test_a_composite_key(self, tmp_path, config, target):
        config["write"]["merge_keys"] = ["Inv_Ref_ID", "Company"]
        FakeSession.payload = [invoice("A", Company="C1"), invoice("A", Company="C2")]
        run(config, tmp_path)
        FakeSession.payload = [invoice("A", Company="C2", status_note="x")]
        result = run(config, tmp_path)

        assert len(rows(target)) == 2
        assert (result.rows_inserted, result.rows_updated) == (0, 1)

    def test_it_works_with_checkpoint(self, tmp_path, config, target):
        config["incremental"] = {
            "enabled": True, "field": "Last_Inv_Date", "param_name": "From",
            "checkpoint": True,
        }
        config["batch_size"] = 2
        FakeSession.payload = [
            invoice("A", Last_Inv_Date="2026-09-01"), invoice("B", Last_Inv_Date="2026-09-02"),
            invoice("C", Last_Inv_Date="2026-09-03"),
        ]
        first = run(config, tmp_path)
        second = run(config, tmp_path)  # rejoue tout : idempotent

        assert first.status == second.status == "success"
        assert len(rows(target)) == 3
        assert second.rows_updated == 3

    def test_an_empty_source_changes_nothing_and_says_so(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A")]
        run(config, tmp_path)
        FakeSession.payload = []
        config["incremental"] = {"enabled": True, "field": "Last_Inv_Date", "param_name": "From"}
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert len(rows(target)) == 1
        assert (result.rows_inserted, result.rows_updated) == (0, 0)
        assert any("returned no rows" in w for w in result.warnings)

    def test_a_record_without_the_key_fails_the_run_before_writing(self, tmp_path, config, target):
        FakeSession.payload = [invoice("A")]
        run(config, tmp_path)
        FakeSession.payload = [{"Status": "Paid"}]
        result = run(config, tmp_path)

        assert result.status == "failed"
        assert "no value for the key 'Inv_Ref_ID'" in result.error_message
        assert [r["Status"] for r in rows(target)] == ["Unpaid"]

    def test_a_key_missing_from_the_target_is_explained(self, tmp_path, config):
        FakeSession.payload = [invoice("A")]
        run(config, tmp_path)
        config["write"]["merge_keys"] = ["Inv_Ref_ID", "Company"]
        FakeSession.payload = [invoice("A", Company="C1")]
        result = run(config, tmp_path)

        assert result.status == "failed"
        assert "missing from the target table" in result.error_message

    def test_append_is_still_append(self, tmp_path, config, target):
        config["write"] = {"mode": "append"}
        FakeSession.payload = [invoice("A")]
        first = run(config, tmp_path)
        run(config, tmp_path)

        assert len(rows(target)) == 2
        # les compteurs de merge n'ont de sens qu'en merge
        assert first.rows_inserted is None and first.rows_updated is None


class TestMergeRecords:
    def test_it_returns_the_counts_reported_by_delta(self, tmp_path):
        uri = str(tmp_path / "t")
        types, fallbacks, inserted, updated = merge_records(uri, [{"id": "a", "v": 1}], ["id"])
        assert (inserted, updated) == (1, 0)
        assert "id" in types and fallbacks == []

        _, _, inserted, updated = merge_records(
            uri, [{"id": "a", "v": 2}, {"id": "b", "v": 3}], ["id"]
        )
        assert (inserted, updated) == (1, 1)

    def test_a_null_key_is_refused(self, tmp_path):
        with pytest.raises(DeltaWriteError, match="no value for the key"):
            merge_records(str(tmp_path / "t"), [{"id": None, "v": 1}], ["id"])

    def test_a_backtick_in_a_key_is_refused(self, tmp_path):
        uri = str(tmp_path / "t")
        merge_records(uri, [{"id": "a"}], ["id"])
        with pytest.raises(ValueError, match="backtick"):
            merge_records(uri, [{"id": "a"}], ["id` OR 1=1 --"])
