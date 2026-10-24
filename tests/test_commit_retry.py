"""Un commit perdu face à un autre écrivain est rejoué, pas propagé.

Mesuré sur la pile installée : huit écrivains qui créent la même table en même
temps, six commits refusés (`Failed to commit transaction: Protocol changed
since last commit`). Rien n'avait été écrit : rejouer est sûr."""

import sys
import threading

import pytest
from deltalake.exceptions import CommitFailedError

from flume_lib import _delta
from flume_lib._delta import (
    COMMIT_ATTEMPTS,
    DeltaWriteError,
    commit_with_retry,
    merge_records,
    query_table,
    write_records,
)


@pytest.fixture
def no_pause(monkeypatch):
    """Les tests unitaires ne dorment pas. Pas les tests de contention : ils
    mesurent justement que la pause aléatoire désynchronise les écrivains."""
    monkeypatch.setattr(_delta.time, "sleep", lambda seconds: None)


@pytest.mark.usefixtures("no_pause")
class TestCommitWithRetry:
    def test_a_lost_commit_is_replayed_until_it_passes(self):
        calls = []

        def operation():
            calls.append(1)
            if len(calls) < 3:
                raise CommitFailedError("Failed to commit transaction")
            return "done"

        assert commit_with_retry(operation) == "done"
        assert len(calls) == 3

    def test_the_last_error_is_raised_untouched_when_attempts_run_out(self):
        calls = []

        def operation():
            calls.append(1)
            raise CommitFailedError("Failed to commit transaction: lost again")

        with pytest.raises(CommitFailedError, match="lost again"):
            commit_with_retry(operation)
        assert len(calls) == COMMIT_ATTEMPTS

    def test_any_other_error_is_not_replayed(self):
        calls = []

        def operation():
            calls.append(1)
            raise ValueError("not a conflict")

        with pytest.raises(ValueError):
            commit_with_retry(operation)
        assert calls == [1]

    def test_the_pause_grows_and_is_randomised(self, monkeypatch):
        pauses = []
        monkeypatch.setattr(_delta.time, "sleep", pauses.append)
        monkeypatch.setattr(_delta.random, "uniform", lambda low, high: high)

        def operation():
            raise CommitFailedError("x")

        with pytest.raises(CommitFailedError):
            commit_with_retry(operation)

        assert len(pauses) == COMMIT_ATTEMPTS - 1
        assert pauses == sorted(pauses) and pauses[0] < pauses[-1]


class TestWritesUnderContention:
    def test_eight_writers_creating_the_same_table_all_get_in(self, tmp_path):
        uri = str(tmp_path / "bronze" / "shared")
        errors = []

        def write(i):
            try:
                write_records(uri, [{"n": i}])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        rows = query_table(uri, "select n from t", alias="t")
        assert sorted(r["n"] for r in rows) == list(range(8))

    @pytest.mark.usefixtures("no_pause")
    def test_a_write_that_keeps_losing_says_so(self, tmp_path, monkeypatch):
        def always_lost(*args, **kwargs):
            raise CommitFailedError("Failed to commit transaction: Metadata changed")

        monkeypatch.setattr(_delta, "write_deltalake", always_lost)
        with pytest.raises(DeltaWriteError) as caught:
            write_records(str(tmp_path / "t"), [{"n": 1}])
        assert f"{COMMIT_ATTEMPTS} attempts were not enough" in str(caught.value)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="le système de fichiers local de Windows refuse parfois l'ouverture d'un "
        "fichier du journal qu'un autre thread est en train d'écrire (os error 5) : "
        "une particularité de ce test, absente de Linux et d'OneLake",
    )
    def test_concurrent_merges_on_one_table_all_apply(self, tmp_path):
        uri = str(tmp_path / "bronze" / "merged")
        write_records(uri, [{"id": -1, "v": 0}])
        errors = []

        def merge(i):
            try:
                merge_records(uri, [{"id": i, "v": i}], ["id"])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=merge, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        rows = query_table(uri, "select id from t order by id", alias="t")
        assert [r["id"] for r in rows] == [-1, 0, 1, 2, 3]
