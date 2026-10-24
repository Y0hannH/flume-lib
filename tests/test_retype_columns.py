"""`retype_columns` sur de vraies tables Delta : c'est la réécriture par
delta-rs, et les casts de DataFusion, qui sont en cause — un mock dirait oui."""

from decimal import Decimal

import pytest

import flume_lib
from flume_lib import retype_columns
from flume_lib._delta import DeltaWriteError, query_table, read_table_types, write_records


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path)


def uri_of(lake, table="t"):
    return f"{lake}/bronze/{table}"


def rows(lake, table="t", order="n"):
    return query_table(uri_of(lake, table), f"select * from t order by {order}", alias="t")


def make(lake, records, table="t", **kwargs):
    write_records(uri_of(lake, table), records, **kwargs)


def names(lake, table="t"):
    return {k: str(v).strip() for k, v in read_table_types(uri_of(lake, table)).items()}


def go(lake, columns, table="t"):
    return retype_columns("bronze", table, columns, lakehouse_tables_path=lake)


class TestTheMigrationTheGuardPointsTo:
    def test_an_integer_column_becomes_a_double_and_keeps_its_values(self, lake):
        make(lake, [{"n": 1, "x": 100}, {"n": 2, "x": 250}])

        assert go(lake, {"x": "double"}) == 2

        assert names(lake)["x"] == "arro3.core.DataType<Float64>"
        assert [r["x"] for r in rows(lake)] == [100.0, 250.0]

    def test_the_next_batch_of_decimals_then_goes_through(self, lake):
        from flume_lib._delta import read_table_types

        make(lake, [{"n": 1, "x": 100}])
        go(lake, {"x": "double"})
        uri = uri_of(lake)
        write_records(uri, [{"n": 2, "x": 1234.56}], table_types=read_table_types(uri))

        assert [r["x"] for r in rows(lake)] == [100.0, 1234.56]

    def test_the_other_columns_and_the_lineage_are_untouched(self, lake):
        make(lake, [{"n": 1, "x": 5, "s": "a", "_flume_run_id": "r1"}])
        go(lake, {"x": "double"})

        assert rows(lake) == [{"n": 1, "x": 5.0, "s": "a", "_flume_run_id": "r1"}]

    def test_the_public_function_is_exported(self):
        assert flume_lib.retype_columns is retype_columns
        assert "retype_columns" in flume_lib.__all__


class TestConversions:
    def test_text_becomes_a_decimal(self, lake):
        make(lake, [{"n": 1, "x": "12.50"}, {"n": 2, "x": "3"}])
        go(lake, {"x": "decimal(10,2)"})
        assert [r["x"] for r in rows(lake)] == [Decimal("12.50"), Decimal("3.00")]

    def test_text_becomes_a_date(self, lake):
        make(lake, [{"n": 1, "x": "2026-01-02"}])
        go(lake, {"x": "date"})
        assert str(rows(lake)[0]["x"]) == "2026-01-02"

    def test_anything_becomes_text(self, lake):
        make(lake, [{"n": 1, "x": 5}, {"n": 2, "x": 2.5}][:1])
        go(lake, {"x": "string"})
        assert rows(lake)[0]["x"] == "5"

    def test_a_column_already_of_that_type_is_left_alone(self, lake):
        make(lake, [{"n": 1, "x": 1.5, "s": "a"}])
        assert go(lake, {"x": "double", "s": "string"}) == 0

    def test_nulls_stay_null(self, lake):
        make(lake, [{"n": 1, "x": 3}, {"n": 2, "x": None}])
        go(lake, {"x": "double"})
        assert [r["x"] for r in rows(lake)] == [3.0, None]

    def test_a_partitioned_table_keeps_its_partitioning(self, lake):
        from deltalake import DeltaTable

        make(lake, [{"n": 1, "p": "a", "x": 1}, {"n": 2, "p": "b", "x": 2}], partition_by=["p"])
        go(lake, {"x": "double"})
        assert DeltaTable(uri_of(lake)).metadata().partition_columns == ["p"]
        assert len(rows(lake)) == 2


class TestNothingIsLostSilently:
    def test_a_decimal_that_would_be_rounded_stops_everything(self, lake):
        make(lake, [{"n": 1, "x": "1.234"}, {"n": 2, "x": "5"}])
        with pytest.raises(DeltaWriteError, match="more decimals than the scale"):
            go(lake, {"x": "decimal(10,2)"})
        assert [r["x"] for r in rows(lake)] == ["1.234", "5"]
        assert names(lake)["x"].endswith("Utf8>")

    def test_unreadable_values_are_counted_not_quoted(self, lake):
        make(lake, [{"n": 1, "x": "12"}, {"n": 2, "x": "abc"}, {"n": 3, "x": "def"}])
        with pytest.raises(DeltaWriteError) as caught:
            go(lake, {"x": "double"})
        assert "2 value(s) cannot be read as double" in str(caught.value)
        assert "abc" not in str(caught.value)

    def test_an_identifier_beyond_2_pow_53_is_not_turned_into_a_double(self, lake):
        make(lake, [{"n": 1, "x": 2**60}])
        with pytest.raises(DeltaWriteError, match=r"exceed 2\^53"):
            go(lake, {"x": "double"})

    def test_a_decimal_that_overflows_its_precision_is_refused(self, lake):
        make(lake, [{"n": 1, "x": "123456789"}])
        with pytest.raises(DeltaWriteError, match=r"cannot be read as decimal\(5,2\)"):
            go(lake, {"x": "decimal(5,2)"})

    def test_a_float_is_not_converted_to_a_decimal(self, lake):
        make(lake, [{"n": 1, "x": 0.1}])
        with pytest.raises(DeltaWriteError, match="without a loss"):
            go(lake, {"x": "decimal(10,2)"})

    def test_a_number_is_not_a_date(self, lake):
        make(lake, [{"n": 1, "x": 20260102}])
        with pytest.raises(DeltaWriteError, match="without a loss"):
            go(lake, {"x": "date"})

    def test_a_partition_column_cannot_be_retyped(self, lake):
        make(lake, [{"n": 1, "p": 1}], partition_by=["p"])
        with pytest.raises(DeltaWriteError, match="partition column"):
            go(lake, {"p": "string"})

    def test_a_failure_on_one_column_changes_none_of_them(self, lake):
        make(lake, [{"n": 1, "a": 5, "b": "oops"}])
        with pytest.raises(DeltaWriteError):
            go(lake, {"a": "double", "b": "double"})
        assert names(lake)["a"].endswith("Int64>")


class TestArguments:
    def test_a_missing_table_is_explained(self, lake):
        with pytest.raises(DeltaWriteError, match="does not exist"):
            go(lake, {"x": "double"}, table="nope")

    def test_an_unknown_column_lists_the_available_ones(self, lake):
        make(lake, [{"n": 1, "x": 1}])
        with pytest.raises(DeltaWriteError, match=r"\['y'\].*columns: \['n', 'x'\]"):
            go(lake, {"y": "double"})

    def test_an_unknown_type_is_refused(self, lake):
        make(lake, [{"n": 1, "x": 1}])
        with pytest.raises(ValueError, match="unknown column type"):
            go(lake, {"x": "int128"})

    @pytest.mark.parametrize("bad", [{}, None, ["x"]])
    def test_an_empty_or_malformed_mapping_is_refused(self, lake, bad):
        with pytest.raises(ValueError, match="non-empty"):
            go(lake, bad)

    def test_a_schema_name_cannot_traverse(self, lake):
        with pytest.raises(ValueError, match="invalid schema"):
            retype_columns("../x", "t", {"x": "double"}, lakehouse_tables_path=lake)
