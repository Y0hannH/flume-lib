"""Tests d'écriture Delta réels : ceux-ci commitent vraiment, sur une table
locale. Tout le reste de la suite mocke `write_records`, ce qui vérifie ce que
la lib demande à delta-rs mais jamais ce que delta-rs en fait. Or c'est
précisément là que se joue le remplacement d'une fenêtre : le mock dirait
toujours oui."""

import pytest

from flume_lib._delta import DeltaWriteError, query_table, write_records
from flume_lib.source import run_source


def rows(uri):
    return query_table(uri, "select * from t order by m, v", alias="t")


def values(uri):
    return [(r["m"], r["v"]) for r in rows(uri)]


@pytest.fixture
def uri(tmp_path):
    return str(tmp_path / "items")


def recs(*pairs):
    return [{"m": m, "v": v} for m, v in pairs]


class TestAppend:
    def test_rows_accumulate(self, uri):
        write_records(uri, recs(("01", 1)))
        write_records(uri, recs(("01", 2)))
        assert values(uri) == [("01", 1), ("01", 2)]

    def test_a_predicate_without_overwrite_is_refused(self, uri):
        # garde-fou de programmation : delta-rs ignorerait le prédicat en
        # append, et la fenêtre visée doublerait au lieu d'être remplacée
        with pytest.raises(ValueError, match="overwrite"):
            write_records(uri, recs(("01", 1)), predicate="m = '01'")


class TestReplaceWhere:
    def test_the_window_is_replaced_and_the_rest_survives(self, uri):
        write_records(uri, recs(("01", 1), ("01", 2), ("02", 3)))
        write_records(uri, recs(("01", 9)), mode="overwrite", predicate="m = '01'")

        assert values(uri) == [("01", 9), ("02", 3)]

    def test_an_absent_window_is_written_without_deleting_anything(self, uri):
        write_records(uri, recs(("02", 3)))
        write_records(uri, recs(("05", 5)), mode="overwrite", predicate="m = '05'")

        # backfill d'une fenêtre neuve et rejeu d'une fenêtre déjà chargée
        # empruntent le même chemin : l'appelant n'a pas à savoir laquelle
        assert values(uri) == [("02", 3), ("05", 5)]

    def test_replaying_a_backfill_does_not_duplicate(self, uri):
        for _ in range(3):
            write_records(
                uri, recs(("01", 1), ("01", 2)),
                mode="overwrite", predicate="m = '01'",
            )
        assert values(uri) == [("01", 1), ("01", 2)]

    def test_rows_outside_the_predicate_are_refused(self, uri):
        write_records(uri, recs(("01", 1)))
        with pytest.raises(DeltaWriteError, match="do not satisfy predicate"):
            write_records(
                uri, recs(("03", 3)), mode="overwrite", predicate="m = '01'"
            )
        # le refus est un refus : la table est intacte
        assert values(uri) == [("01", 1)]

    def test_an_unknown_column_in_the_predicate_is_explained(self, uri):
        write_records(uri, recs(("01", 1)))
        with pytest.raises(DeltaWriteError, match="column missing"):
            write_records(
                uri, recs(("01", 2)), mode="overwrite", predicate="nawak = '1'"
            )

    def test_a_new_column_still_gets_through(self, uri):
        write_records(uri, recs(("01", 1)))
        write_records(
            uri,
            [{"m": "01", "v": 2, "extra": "x"}],
            mode="overwrite",
            predicate="m = '01'",
        )
        assert [r.get("extra") for r in rows(uri)] == ["x"]


class TestOverwrite:
    def test_the_table_is_replaced_whole(self, uri):
        write_records(uri, recs(("01", 1), ("02", 2)))
        write_records(uri, recs(("03", 3)), mode="overwrite")
        assert values(uri) == [("03", 3)]


class TestPartitioning:
    def test_a_partitioned_table_reads_back_whole(self, uri):
        write_records(uri, recs(("01", 1), ("02", 2)), partition_by=["m"])
        write_records(uri, recs(("01", 3)), partition_by=["m"])
        assert values(uri) == [("01", 1), ("01", 3), ("02", 2)]

    def test_partitioning_an_existing_table_is_explained(self, uri):
        write_records(uri, recs(("01", 1)))
        with pytest.raises(DeltaWriteError, match="frozen at creation"):
            write_records(uri, recs(("02", 2)), partition_by=["m"])


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.headers = {}
        self.status_code = 200

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class FakeSession:
    """Rejoue une page unique, quelle que soit la requête."""

    payload: list = []

    def __init__(self):
        self.headers = {}

    def request(self, method, url, **kwargs):
        return FakeResponse(FakeSession.payload)


class TestBackfillEndToEnd:
    """Le scénario qui motive tout ça : relancer un backfill sur la même
    fenêtre, jusqu'ici impossible sans dédoublonner à la main."""

    @pytest.fixture
    def config(self):
        return {
            "name": "items",
            "base_url": "https://api.test/items",
            "target_schema": "bronze",
            "target_table": "items",
            "write": {"mode": "replace_where", "replace_where": "m = '01'"},
        }

    @pytest.fixture(autouse=True)
    def http(self, monkeypatch):
        monkeypatch.setattr("flume_lib.source.requests.Session", FakeSession)

    def test_two_identical_runs_leave_one_copy(self, tmp_path, config):
        FakeSession.payload = recs(("01", 1), ("01", 2))
        target = str(tmp_path / "bronze" / "items")

        first = run_source(config, lakehouse_tables_path=str(tmp_path))
        assert first.status == "success", first.error_message
        second = run_source(config, lakehouse_tables_path=str(tmp_path))
        assert second.status == "success", second.error_message

        assert values(target) == [("01", 1), ("01", 2)]
        # et chaque ligne porte le run qui l'a écrite : c'est le second
        assert {r["_flume_run_id"] for r in rows(target)} == {second.run_id}

    def test_the_same_run_in_append_duplicates(self, tmp_path, config):
        # le comportement d'avant, conservé par défaut : c'est bien le mode
        # qui change quelque chose, pas l'écriture Delta en général
        FakeSession.payload = recs(("01", 1))
        config["write"] = {"mode": "append"}
        target = str(tmp_path / "bronze" / "items")

        run_source(config, lakehouse_tables_path=str(tmp_path))
        run_source(config, lakehouse_tables_path=str(tmp_path))

        assert values(target) == [("01", 1), ("01", 1)]

    def test_an_empty_source_leaves_the_window_alone(self, tmp_path, config):
        FakeSession.payload = recs(("01", 1))
        target = str(tmp_path / "bronze" / "items")
        run_source(config, lakehouse_tables_path=str(tmp_path))

        FakeSession.payload = []
        result = run_source(config, lakehouse_tables_path=str(tmp_path))

        assert result.status == "success", result.error_message
        assert values(target) == [("01", 1)]
        assert any("returned no" in w for w in result.warnings)
