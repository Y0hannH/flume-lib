"""Le type de la table fait loi à l'écriture.

delta-rs ne refuse presque jamais un lot au type différent de celui de la
table : il le coerce, et ce qui ne rentre pas disparaît sans erreur (une
colonne entière qui reçoit 2.5 stocke 2). Ces tests tournent sur de vraies
tables Delta — un mock de `write_deltalake` dirait toujours oui, et c'est
précisément la coercition de delta-rs qui est en cause.
"""

import pytest

from flume_lib._delta import (
    DeltaWriteError,
    query_table,
    read_table_types,
    write_records,
)
from flume_lib.source import run_source


def rows(uri, column="x"):
    return [r[column] for r in query_table(uri, f"select {column} from t order by n", alias="t")]


def batch(*xs, start=0):
    return [{"n": start + i, "x": x} for i, x in enumerate(xs)]


def write(uri, records, **kwargs):
    """Une écriture comme le fait un run : la table existante lue d'abord."""
    return write_records(uri, records, table_types=read_table_types(uri), **kwargs)


@pytest.fixture
def uri(tmp_path):
    return str(tmp_path / "bronze" / "t")


class TestNarrowingIsRefused:
    def test_floats_into_an_integer_column_are_refused_not_truncated(self, uri):
        write(uri, batch(1, 2, 3))
        with pytest.raises(DeltaWriteError) as caught:
            write(uri, batch(2.5, 3.7, start=3))

        assert rows(uri) == [1, 2, 3]  # rien n'a été écrit, et surtout rien de tronqué
        message = str(caught.value)
        assert "column 'x' is Int64 in the table" in message
        assert "Float64" in message
        assert "retype_columns" in message

    def test_a_boolean_is_not_a_number(self, uri):
        write(uri, batch(1, 2))
        with pytest.raises(DeltaWriteError, match="column 'x' is Int64"):
            write(uri, batch(True, start=2))
        assert rows(uri) == [1, 2]

    def test_text_into_a_numeric_column_is_refused(self, uri):
        write(uri, batch(1.5, 2.5))
        with pytest.raises(DeltaWriteError, match="column 'x' is Float64"):
            write(uri, batch("abc", start=2))

    def test_a_number_into_a_boolean_column_is_refused(self, uri):
        write(uri, batch(True, False))
        with pytest.raises(DeltaWriteError, match="column 'x' is Boolean"):
            write(uri, batch(1, start=2))

    def test_an_integer_too_large_for_the_column_width_is_refused(self, tmp_path):
        # une table créée ailleurs avec un entier 32 bits
        import arro3.core as ac
        from deltalake import write_deltalake

        uri = str(tmp_path / "ext")
        write_deltalake(uri, ac.Table.from_pydict({"n": ac.Array([0], type=ac.DataType.int64()),
                                                   "x": ac.Array([1], type=ac.DataType.int32())}))
        with pytest.raises(DeltaWriteError, match="column 'x' is Int32"):
            write(uri, batch(2**40, start=1))

    def test_the_message_never_quotes_a_value(self, uri):
        write(uri, batch(1))
        with pytest.raises(DeltaWriteError) as caught:
            write(uri, batch(987654.321, start=1))
        assert "987654" not in str(caught.value)

    def test_without_the_table_types_the_old_silent_truncation_is_still_there(self, uri):
        """Ce test documente le défaut que le garde-fou ferme : sans lui, delta-rs
        écrit 2 et 3 à la place de 2.5 et 3.7. Si delta-rs un jour refuse, il
        devient inutile et peut partir."""
        write_records(uri, batch(1, 2))
        write_records(uri, batch(2.5, 3.7, start=2))
        assert rows(uri) == [1, 2, 2, 3]


class TestWideningPasses:
    def test_integers_into_a_float_column_keep_their_value(self, uri):
        write(uri, batch(1.5, 2.5))
        write(uri, batch(3, 4, start=2))
        assert rows(uri) == [1.5, 2.5, 3.0, 4.0]

    def test_floats_into_a_float_column(self, uri):
        write(uri, batch(1.5))
        write(uri, batch(0.1, start=1))
        assert rows(uri) == [1.5, 0.1]

    def test_anything_scalar_into_a_text_column_is_written_as_text_and_reported(self, uri):
        write(uri, batch("a", "b"))
        _, fallbacks = write(uri, batch(2.5, start=2))

        assert rows(uri) == ["a", "b", "2.5"]
        assert any("column 'x': text in the table" in f and "Float64" in f for f in fallbacks)

    def test_a_text_batch_into_a_text_column_says_nothing(self, uri):
        write(uri, batch("a"))
        _, fallbacks = write(uri, batch("b", start=1))
        assert fallbacks == []

    def test_an_all_null_batch_leaves_the_column_alone(self, uri):
        write(uri, batch(1, 2))
        _, fallbacks = write(uri, batch(None, None, start=2))
        assert rows(uri) == [1, 2, None, None]
        assert fallbacks == []

    def test_a_new_column_is_still_inferred_and_added(self, uri):
        write(uri, batch(1))
        write(uri, [{"n": 1, "x": 2, "y": 2.5}])
        assert rows(uri, "y") == [None, 2.5]

    def test_a_missing_column_is_written_null(self, uri):
        write(uri, batch(1, 2))
        write(uri, [{"n": 2}])
        assert rows(uri) == [1, 2, None]

    def test_a_nested_value_into_a_text_column_stays_json(self, uri):
        write(uri, batch("a"))
        write(uri, batch({"k": 1}, start=1))
        assert rows(uri) == ["a", '{"k": 1}']


class TestTypesTheLibraryCannotInfer:
    @pytest.fixture
    def decimal_uri(self, tmp_path):
        uri = str(tmp_path / "bronze" / "d")
        write_records(
            uri, [{"n": 0, "x": "12.50"}], column_types={"x": "decimal(10,2)"}
        )
        return uri

    def test_a_decimal_column_needs_its_declaration(self, decimal_uri):
        with pytest.raises(
            DeltaWriteError, match=r"column 'x' is Decimal128\(10, 2\).*write.column_types"
        ):
            write(decimal_uri, batch("13.10", start=1))

    def test_the_declaration_makes_it_pass(self, decimal_uri):
        write(decimal_uri, batch("13.10", start=1), column_types={"x": "decimal(10,2)"})
        assert [str(v) for v in rows(decimal_uri)] == ["12.50", "13.10"]

    def test_an_all_null_batch_omits_the_column_instead_of_failing(self, decimal_uri):
        write(decimal_uri, batch(None, start=1))
        assert [None if v is None else str(v) for v in rows(decimal_uri)] == ["12.50", None]

    def test_a_declared_type_that_differs_from_the_table_is_refused(self, decimal_uri):
        with pytest.raises(
            DeltaWriteError, match=r"declared decimal\(12,2\) but is Decimal128\(10, 2\)"
        ):
            write(decimal_uri, batch("1.00", start=1), column_types={"x": "decimal(12,2)"})

    def test_a_declared_double_over_an_integer_table_is_refused(self, uri):
        write(uri, batch(1, 2))
        with pytest.raises(DeltaWriteError, match="declared double but is Int64"):
            write(uri, batch(2.5, start=2), column_types={"x": "double"})
        assert rows(uri) == [1, 2]


class TestDeclaredDouble:
    """`double` est ce qui évite le problème à la source : une table créée par
    un premier lot de valeurs rondes naît flottante au lieu d'entière."""

    def test_round_values_still_create_a_float_column(self, uri):
        write(uri, batch(1, 2), column_types={"x": "double"})
        write(uri, batch(2.5, start=2), column_types={"x": "double"})
        assert rows(uri) == [1.0, 2.0, 2.5]

    def test_numeric_text_is_accepted(self, uri):
        write(uri, batch("12.5", " 3 ", None), column_types={"x": "double"})
        assert rows(uri) == [12.5, 3.0, None]

    @pytest.mark.parametrize("bad", [True, "abc", "nan", "inf", {"a": 1}, [1]])
    def test_what_is_not_a_finite_number_fails_the_run(self, uri, bad):
        with pytest.raises(DeltaWriteError, match="column 'x' is declared double but record 1"):
            write(uri, batch(bad), column_types={"x": "double"})

    @pytest.mark.parametrize("big", [2**53 + 1, str(2**60)])
    def test_an_integer_beyond_2_pow_53_is_refused_not_rounded(self, uri, big):
        with pytest.raises(DeltaWriteError, match="beyond 2\\^53"):
            write(uri, batch(big), column_types={"x": "double"})

    def test_a_native_float_is_never_rounded_by_the_bound(self, uri):
        write(uri, batch(1e300), column_types={"x": "double"})
        assert rows(uri) == [1e300]

    def test_the_error_names_the_record_not_the_value(self, uri):
        with pytest.raises(DeltaWriteError) as caught:
            write(uri, batch(1.5, "secret-looking", ), column_types={"x": "double"})
        assert "record 2" in str(caught.value)
        assert "secret-looking" not in str(caught.value)


class FakeResponse:
    status_code = 200
    headers: dict = {}

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
        return FakeResponse({"items": FakeSession.payload})


@pytest.fixture
def http(monkeypatch):
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


def run(config, tmp_path):
    return run_source(config, lakehouse_tables_path=str(tmp_path))


def amounts(tmp_path):
    return [r["amount"] for r in query_table(
        str(tmp_path / "bronze" / "ledger"), "select amount from t order by id", alias="t")]


@pytest.mark.usefixtures("http")
class TestAtRunLevel:
    def test_the_month_after_a_round_month_fails_loudly(self, tmp_path, config):
        """Le scénario du client découpé par mois : un mois de montants ronds
        fige la colonne en entier ; le mois suivant ne doit pas écrire 1234."""
        FakeSession.payload = [{"id": 1, "amount": 100}, {"id": 2, "amount": 250}]
        assert run(config, tmp_path).status == "success"

        FakeSession.payload = [{"id": 3, "amount": 1234.56}]
        second = run(config, tmp_path)

        assert second.status == "failed"
        assert "column 'amount' is Int64 in the table" in second.error_message
        assert amounts(tmp_path) == [100, 250]
        assert second.rows_loaded == 0

    def test_declaring_double_makes_both_months_load(self, tmp_path, config):
        config["write"] = {"column_types": {"amount": "double"}}
        FakeSession.payload = [{"id": 1, "amount": 100}, {"id": 2, "amount": 250}]
        assert run(config, tmp_path).status == "success"

        FakeSession.payload = [{"id": 3, "amount": 1234.56}]
        second = run(config, tmp_path)

        assert second.status == "success", second.error_message
        assert amounts(tmp_path) == [100.0, 250.0, 1234.56]

    def test_a_dry_run_reads_and_writes_nothing(self, tmp_path, config):
        FakeSession.payload = [{"id": 1, "amount": 1}]
        result = run_source(config, lakehouse_tables_path=str(tmp_path), dry_run=True)
        assert result.status == "success"
        assert not (tmp_path / "bronze" / "ledger").exists()

    def test_the_guard_covers_a_replacing_write(self, tmp_path, config):
        FakeSession.payload = [{"id": 1, "amount": 100}]
        run(config, tmp_path)
        config["write"] = {"mode": "overwrite"}
        FakeSession.payload = [{"id": 1, "amount": 100.5}]

        result = run(config, tmp_path)

        assert result.status == "failed"
        assert "column 'amount' is Int64" in result.error_message
        assert amounts(tmp_path) == [100]

    def test_the_guard_covers_merge(self, tmp_path, config):
        config["write"] = {"mode": "merge", "merge_keys": ["id"]}
        FakeSession.payload = [{"id": 1, "amount": 100}]
        run(config, tmp_path)
        FakeSession.payload = [{"id": 1, "amount": 100.5}]

        result = run(config, tmp_path)

        assert result.status == "failed"
        assert "column 'amount' is Int64" in result.error_message
        assert amounts(tmp_path) == [100]

    def test_a_first_run_has_no_table_to_respect(self, tmp_path, config):
        FakeSession.payload = [{"id": 1, "amount": 1.5}]
        assert run(config, tmp_path).status == "success"
        assert amounts(tmp_path) == [1.5]

    def test_the_table_is_read_once_per_run_not_once_per_batch(self, tmp_path, config, monkeypatch):
        config["batch_size"] = 2
        FakeSession.payload = [{"id": i, "amount": i} for i in range(7)]
        calls = []
        real = read_table_types

        def counting(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr("flume_lib.source.read_table_types", counting)
        assert run(config, tmp_path).status == "success"
        assert len(calls) == 1
