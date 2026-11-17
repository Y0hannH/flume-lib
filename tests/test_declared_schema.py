"""Le schéma déclaré existe dans la table, que l'API ait renvoyé le champ ou non.

Une API omet volontiers un champ optionnel quand aucun objet de la fenêtre ne
le porte. Sans déclaration, la table n'a que les colonnes vues ce jour-là ; avec
`write.column_types`, chaque colonne déclarée est écrite — à NULL et à son
type — même absente du lot. Sur de vraies tables Delta : c'est le schéma
réellement commité qui compte.
"""

import pytest

from flume_lib._delta import (
    DeltaWriteError,
    _type_name,
    merge_records,
    parse_column_type,
    query_table,
    read_table_types,
    records_to_table,
    retype_table_columns,
    write_records,
)
from flume_lib.validation import ConfigError, validate_config

SCHEMA = {
    "id": "bigint",
    "amount": "decimal(18,2)",
    "ratio": "double",
    "closed": "boolean",
    "closed_on": "date",
    "comment": "string",
}


def write(uri, records, **kwargs):
    return write_records(uri, records, table_types=read_table_types(uri), **kwargs)


def table_types(uri):
    return {name: _type_name(dtype) for name, dtype in read_table_types(uri).items()}


def select(uri, columns="*"):
    return query_table(uri, f"select {columns} from t order by id", alias="t")


@pytest.fixture
def uri(tmp_path):
    return str(tmp_path / "bronze" / "t")


class TestAbsentDeclaredColumnsAreWritten:
    def test_the_table_is_created_with_the_whole_declared_schema(self, uri):
        write(uri, [{"id": 1}], column_types=SCHEMA)

        assert table_types(uri) == {
            "id": "Int64",
            "amount": "Decimal128(18, 2)",
            "ratio": "Float64",
            "closed": "Boolean",
            "closed_on": "Date32",
            "comment": "Utf8",
        }
        assert select(uri) == [
            {"id": 1, "amount": None, "ratio": None, "closed": None,
             "closed_on": None, "comment": None}
        ]

    def test_a_field_absent_one_day_and_present_the_next(self, uri):
        write(uri, [{"id": 1}], column_types=SCHEMA)
        write(uri, [{"id": 2, "amount": "12.50", "closed": True}], column_types=SCHEMA)

        rows = select(uri, "id, cast(amount as varchar) as amount, closed")
        assert rows == [
            {"id": 1, "amount": None, "closed": None},
            {"id": 2, "amount": "12.50", "closed": True},
        ]

    def test_present_one_day_and_absent_the_next(self, uri):
        write(uri, [{"id": 1, "comment": "x"}], column_types=SCHEMA)
        write(uri, [{"id": 2}], column_types=SCHEMA)
        assert [r["comment"] for r in select(uri)] == ["x", None]

    def test_a_declared_column_is_added_to_an_existing_table(self, uri):
        write(uri, [{"id": 1}])
        write(uri, [{"id": 2}], column_types={"closed_on": "date"})
        assert table_types(uri) == {"id": "Int64", "closed_on": "Date32"}

    def test_undeclared_columns_are_still_inferred(self, uri):
        write(uri, [{"id": 1, "extra": 2.5}], column_types={"comment": "string"})
        assert table_types(uri) == {"id": "Int64", "extra": "Float64", "comment": "Utf8"}

    def test_declared_columns_follow_the_received_ones(self):
        table, fallbacks = records_to_table([{"b": 1, "a": 2}], column_types={"z": "string"})
        assert table.column_names == ["b", "a", "z"]
        assert fallbacks == []

    def test_an_absent_column_conflicting_with_the_table_fails_before_writing(self, uri):
        write(uri, [{"id": 1, "amount": 2.5}])
        with pytest.raises(DeltaWriteError, match=r"column 'amount' is declared decimal\(18,2\)"):
            write(uri, [{"id": 2}], column_types={"amount": "decimal(18,2)"})
        assert [r["id"] for r in select(uri)] == [1]

    def test_merge_adds_the_declared_columns_too(self, uri):
        merge_records(uri, [{"id": 1}], ["id"], column_types=SCHEMA)
        merge_records(uri, [{"id": 1, "comment": "x"}, {"id": 2}], ["id"], column_types=SCHEMA)

        assert table_types(uri)["closed_on"] == "Date32"
        assert [(r["id"], r["comment"]) for r in select(uri)] == [(1, "x"), (2, None)]

    def test_merge_on_an_existing_table_without_the_column(self, uri):
        merge_records(uri, [{"id": 1}], ["id"])
        merge_records(uri, [{"id": 2}], ["id"], column_types={"closed": "boolean"})
        assert table_types(uri) == {"id": "Int64", "closed": "Boolean"}

    def test_merge_keeps_the_value_of_a_column_the_batch_does_not_carry(self, uri):
        # une colonne de NULL dans la source du MERGE effacerait la valeur :
        # le comportement d'avant est conservé, la colonne existe déjà
        merge_records(uri, [{"id": 1, "comment": "x"}], ["id"], column_types=SCHEMA)
        merge_records(uri, [{"id": 1}], ["id"], column_types=SCHEMA)
        assert select(uri, "id, comment") == [{"id": 1, "comment": "x"}]


class TestBigint:
    def values(self, *xs):
        table, _ = records_to_table([{"x": x} for x in xs], column_types={"x": "bigint"})
        assert _type_name(table.schema.field("x").type) == "Int64"
        return table.column("x").to_pylist()

    def test_integers_integral_floats_and_numeric_texts(self):
        assert self.values(1, 2.0, " 3 ", "4.0", None, 2**63 - 1) == [1, 2, 3, 4, None, 2**63 - 1]

    def test_a_first_batch_of_nulls_is_still_an_integer_column(self):
        assert self.values(None, None) == [None, None]

    @pytest.mark.parametrize(
        ("bad", "reason"),
        [
            (2.5, "has decimals"),
            ("2.5", "has decimals"),
            ("abc", "is not a number"),
            (True, "not a number"),
            ({"a": 1}, "not a number"),
            (float("nan"), "not a finite number"),
            (2**63, "does not fit in 64 bits"),
        ],
    )
    def test_what_does_not_fit_is_refused(self, bad, reason):
        with pytest.raises(DeltaWriteError, match=reason) as caught:
            self.values(1, bad)
        assert "record 2" in str(caught.value)

    def test_the_value_is_never_quoted(self):
        with pytest.raises(DeltaWriteError) as caught:
            self.values("secret-looking")
        assert "secret" not in str(caught.value)


class TestBoolean:
    def values(self, *xs):
        table, _ = records_to_table([{"x": x} for x in xs], column_types={"x": "boolean"})
        assert _type_name(table.schema.field("x").type) == "Boolean"
        return table.column("x").to_pylist()

    def test_booleans_and_their_texts(self):
        assert self.values(True, False, "true", " FALSE ", None) == [True, False, True, False, None]

    @pytest.mark.parametrize("bad", [1, 0, "yes", "1", 1.0, [True]])
    def test_anything_else_is_refused(self, bad):
        with pytest.raises(DeltaWriteError, match="is not true or false"):
            self.values(bad)


class TestConfigAndRetype:
    def test_the_new_types_are_valid_in_a_config(self):
        assert parse_column_type(" BigInt ") == ("bigint", None, None)
        assert parse_column_type("boolean") == ("boolean", None, None)
        with pytest.raises(ValueError, match="bigint, boolean"):
            parse_column_type("int")

    def test_validate_config_accepts_a_full_schema(self):
        config = {
            "name": "s",
            "base_url": "https://api.example.com",
            "target_schema": "bronze",
            "target_table": "items",
            "write": {"column_types": SCHEMA},
        }
        validate_config(config)
        config["write"] = {"column_types": {"id": "integer"}}
        with pytest.raises(ConfigError, match="unknown column type"):
            validate_config(config)

    def test_text_becomes_bigint_or_boolean(self, uri):
        write(uri, [{"id": 1, "n": "12", "b": "true"}, {"id": 2, "n": None, "b": "False"}])
        assert retype_table_columns(uri, {"n": "bigint", "b": "boolean"}) == 2
        assert table_types(uri) == {"id": "Int64", "n": "Int64", "b": "Boolean"}
        assert select(uri, "id, n, b") == [
            {"id": 1, "n": 12, "b": True},
            {"id": 2, "n": None, "b": False},
        ]

    def test_text_that_is_not_an_integer_or_a_boolean_is_refused(self, uri):
        write(uri, [{"id": 1, "n": "2.5", "b": "yes"}])
        with pytest.raises(DeltaWriteError, match="cannot be read as bigint"):
            retype_table_columns(uri, {"n": "bigint"})
        with pytest.raises(DeltaWriteError, match="are not true or false"):
            retype_table_columns(uri, {"b": "boolean"})
        assert table_types(uri)["n"] == "Utf8"

    def test_a_float_column_does_not_become_bigint(self, uri):
        write(uri, [{"id": 1, "n": 2.5}])
        with pytest.raises(DeltaWriteError, match="cannot be converted to bigint"):
            retype_table_columns(uri, {"n": "bigint"})

    def test_already_the_right_type_is_a_no_op(self, uri):
        write(uri, [{"id": 1, "b": True}])
        assert retype_table_columns(uri, {"id": "bigint", "b": "boolean"}) == 0
