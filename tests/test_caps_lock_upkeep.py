"""Plafonds de réponse et de durée, verrou `single_instance`, entretien des
tables techniques.

Les trois ferment ce que l'audit laissait ouvert : un serveur qui déverse un flux
ou goutte à goutte, deux runs de la même source qui se chevauchent, et une table
de watermark qui accumule un fichier par run.
"""

# Les fixtures `http` et `delta` sont importées de test_source, puis reçues en
# argument : pytest les résout par nom, ce que F811 prend pour une redéfinition.
# ruff: noqa: F811

import io
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
import requests
from requests.structures import CaseInsensitiveDict

# Import à plat : voir test_error_sanitization.py
from test_source import BASE_CONFIG, FakeResponse, delta, http  # noqa: F401

from flume_lib import compact_technical_tables
from flume_lib._delta import query_table, write_records
from flume_lib.locks import (
    DEFAULT_LOCK_TTL_SECONDS,
    LOCKS_TABLE,
    STATE_ACQUIRED,
    RunLock,
    RunLockedError,
    active_runs,
    stamp,
)
from flume_lib.source import (
    DEFAULT_MAX_RESPONSE_BYTES,
    ResponseTooLargeError,
    RunTimeoutError,
    _check_deadline,
    _error_body_text,
    _read_json,
    _response_detail,
    run_source,
)
from flume_lib.validation import ConfigError, validate_config
from flume_lib.watermark import read_watermark, write_watermark

BASE = {
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}


def cfg(**overrides):
    return {**BASE, **overrides}


class CountingIO(io.BytesIO):
    """Compte ce qui a été lu : `close()` rend `tell()` inutilisable."""

    consumed = 0

    def read(self, size=-1):
        data = super().read(size)
        self.consumed += len(data)
        return data


def real_response(body: bytes, status=200, headers=None):
    """Une vraie `requests.Response`, lue en flux comme celle d'une session."""
    response = requests.Response()
    response.status_code = status
    response.raw = CountingIO(body)
    response.headers = CaseInsensitiveDict(headers or {})
    response.encoding = "utf-8"
    return response


# ---------------------------------------------------------------------------
# Taille de réponse
# ---------------------------------------------------------------------------


class TestResponseSizeCap:
    URL = "https://api.test/items?token=SECRET"

    def test_a_normal_body_is_parsed(self):
        response = real_response(b'{"items": [{"id": 1}]}')
        assert _read_json(response, self.URL, 1000, None, None) == {"items": [{"id": 1}]}

    def test_a_body_exactly_at_the_cap_is_accepted(self):
        body = b'{"a": 1}'
        assert _read_json(real_response(body), self.URL, len(body), None, None) == {"a": 1}

    def test_a_declared_length_above_the_cap_is_refused_without_reading(self):
        response = real_response(b"x" * 10, headers={"Content-Length": "5000"})
        with pytest.raises(ResponseTooLargeError, match="announces 5000 bytes") as exc:
            _read_json(response, self.URL, 1000, None, None)
        assert response.raw.consumed == 0
        assert "SECRET" not in str(exc.value)  # l'URL est celle de safe_url

    def test_a_body_that_outgrows_the_cap_is_cut_off(self):
        """Pas d'en-tête Content-Length (transfert par blocs) : c'est la lecture
        qui compte."""
        response = real_response(b'{"a": "' + b"x" * 500_000 + b'"}')
        with pytest.raises(ResponseTooLargeError, match="exceeds max_response_bytes"):
            _read_json(response, self.URL, 100_000, None, None)
        assert response.raw.consumed < 500_000  # on n'a pas tout lu

    def test_a_double_dash_in_the_message_names_the_remedy(self):
        with pytest.raises(ResponseTooLargeError, match="not paginating"):
            _read_json(real_response(b"x" * 300), self.URL, 100, None, None)

    def test_invalid_json_keeps_the_type_requests_would_have_raised(self):
        with pytest.raises(requests.exceptions.JSONDecodeError):
            _read_json(real_response(b"<html>not json</html>"), self.URL, 1000, None, None)

    def test_a_double_that_is_not_a_response_keeps_json(self):
        assert _read_json(FakeResponse({"a": 1}), self.URL, 10, None, None) == {"a": 1}

    def test_the_default_is_generous_but_finite(self):
        assert DEFAULT_MAX_RESPONSE_BYTES == 100 * 1024 * 1024


class TestErrorBodyIsBounded:
    def test_only_the_start_of_a_huge_error_body_is_read(self):
        response = real_response(b"error: " + b"x" * 5_000_000, status=400)
        detail = _response_detail(response)
        assert detail.startswith(": error: xxx")
        assert len(detail) <= 2 + 500
        assert response.raw.consumed < 100_000

    def test_whitespace_is_collapsed(self):
        response = real_response(b"line one\n   line two\r\n\tline three", status=400)
        assert _response_detail(response) == ": line one line two line three"

    def test_a_double_keeps_text(self):
        assert _error_body_text(FakeResponse({}, text="boom")) == "boom"


class TestResponseCapInARun:
    def test_an_oversized_response_fails_the_run(self, http, delta, monkeypatch):
        def respond(self, method, url, **kwargs):
            return real_response(b'{"items": "' + b"x" * 10_000 + b'"}')

        monkeypatch.setattr(http, "request", respond)
        result = run_source(
            {**BASE_CONFIG, "max_response_bytes": 1000}, lakehouse_tables_path="/tmp/Tables"
        )
        assert result.status == "failed"
        assert "ResponseTooLargeError" in result.error_message
        assert "max_response_bytes (1000)" in result.error_message

    def test_a_response_under_the_cap_loads(self, http, delta, monkeypatch):
        def respond(self, method, url, **kwargs):
            return real_response(b'[{"id": 1}, {"id": 2}]')

        monkeypatch.setattr(http, "request", respond)
        result = run_source(BASE_CONFIG, lakehouse_tables_path="/tmp/Tables")
        assert result.status == "success", result.error_message
        assert result.rows_loaded == 2

    def test_the_request_is_streamed(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        run_source(BASE_CONFIG, lakehouse_tables_path="/tmp/Tables")
        assert http.instances[-1].calls[0]["stream"] is True


# ---------------------------------------------------------------------------
# Durée du run
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class TestRunDeadline:
    def test_no_deadline_never_raises(self):
        _check_deadline(None, None)

    def test_a_passed_deadline_raises(self, monkeypatch):
        monkeypatch.setattr("flume_lib.source.time.monotonic", lambda: 100.0)
        with pytest.raises(RunTimeoutError, match=r"max_run_seconds \(10 s\)"):
            _check_deadline(50.0, 10)

    def test_a_deadline_still_ahead_is_fine(self, monkeypatch):
        monkeypatch.setattr("flume_lib.source.time.monotonic", lambda: 10.0)
        _check_deadline(50.0, 10)

    def test_a_slow_drip_is_cut_off_between_chunks(self, monkeypatch):
        """`timeout` borne le silence entre deux octets, pas la durée totale : un
        serveur qui en envoie un toutes les 50 s garde la requête ouverte."""
        clock = _Clock()
        monkeypatch.setattr("flume_lib.source.time.monotonic", clock)

        class Dripping(io.BytesIO):
            def read(self, size=-1):
                clock.now += 40  # chaque lecture « coûte » 40 s
                return super().read(1)

        response = requests.Response()
        response.status_code = 200
        response.raw = Dripping(b'{"a": "' + b"x" * 1000 + b'"}')
        response.headers = CaseInsensitiveDict()
        with pytest.raises(RunTimeoutError):
            _read_json(response, "https://api.test/x", 10_000_000, 100.0, 100)

    def test_the_run_stops_before_the_next_request(self, http, delta, monkeypatch):
        clock = _Clock()
        monkeypatch.setattr("flume_lib.source.time.monotonic", clock)
        pages = [[{"id": 1}, {"id": 2}], [{"id": 3}, {"id": 4}], [{"id": 5}]]

        def request(self, method, url, **kwargs):
            clock.now += 11  # chaque appel dure plus que la limite
            return FakeResponse(pages.pop(0))

        monkeypatch.setattr(http, "request", request)
        config = {
            **BASE_CONFIG,
            "max_run_seconds": 10,
            "batch_size": 2,
            "pagination": {"type": "offset", "limit": 2},
        }

        result = run_source(config, lakehouse_tables_path="/tmp/Tables")

        assert result.status == "failed"
        assert "RunTimeoutError" in result.error_message
        assert "max_run_seconds (10 s)" in result.error_message
        # la première page, lue dans les temps, est écrite : rien n'est perdu
        assert result.rows_loaded == 2
        assert len(pages) == 2  # une seule page a été demandée

    def test_a_run_inside_its_limit_is_untouched(self, http, delta):
        http.next_payloads = [[{"id": 1}]]
        result = run_source(
            {**BASE_CONFIG, "max_run_seconds": 3600}, lakehouse_tables_path="/tmp/Tables"
        )
        assert result.status == "success", result.error_message


class TestCapsConfig:
    @pytest.mark.parametrize("key", ["max_response_bytes", "max_run_seconds"])
    @pytest.mark.parametrize("bad", [0, -1, True, "10", None])
    def test_they_must_be_positive_numbers(self, key, bad):
        with pytest.raises(ConfigError, match=key):
            validate_config(cfg(**{key: bad}))

    def test_max_response_bytes_is_an_integer(self):
        with pytest.raises(ConfigError, match="max_response_bytes"):
            validate_config(cfg(max_response_bytes=1.5))

    def test_max_run_seconds_may_be_fractional(self):
        validate_config(cfg(max_run_seconds=0.5))


# ---------------------------------------------------------------------------
# Verrou single_instance
# ---------------------------------------------------------------------------


def lock(tmp_path, run_id, source="s1", enabled=True):
    return RunLock(str(tmp_path), source, run_id, "flume", None, enabled=enabled)


ON = {"single_instance": True}


class TestRunLock:
    def test_acquire_then_release(self, tmp_path):
        first = lock(tmp_path, "run-1")
        first.acquire(ON)
        uri = f"{tmp_path}/flume/{LOCKS_TABLE}"
        assert list(active_runs(uri, "s1")) == ["run-1"]

        first.release()
        assert active_runs(uri, "s1") == {}

    def test_without_the_option_nothing_is_written(self, tmp_path):
        first = lock(tmp_path, "run-1")
        first.acquire({})
        assert first.held is False
        assert query_table(f"{tmp_path}/flume/{LOCKS_TABLE}", "select * from t", alias="t") == []

    def test_a_disabled_lock_ignores_the_option(self, tmp_path):
        """Le dry run n'écrit rien, verrou compris."""
        first = lock(tmp_path, "run-1", enabled=False)
        first.acquire(ON)
        assert first.held is False

    def test_a_second_run_is_refused_and_says_who_holds_it(self, tmp_path):
        lock(tmp_path, "run-1").acquire(ON)
        second = lock(tmp_path, "run-2")
        with pytest.raises(RunLockedError, match="already being loaded by run run-1") as exc:
            second.acquire(ON)
        assert "lock_ttl_seconds" in str(exc.value)
        assert second.held is False

    def test_a_refusal_at_the_first_check_writes_nothing(self, tmp_path):
        """Le contrôle initial évite d'écrire une ligne que la relecture ferait
        aussitôt retirer : sans lui, le refus coûterait deux commits de plus."""
        lock(tmp_path, "run-1").acquire(ON)
        with pytest.raises(RunLockedError):
            lock(tmp_path, "run-2").acquire(ON)
        rows = query_table(f"{tmp_path}/flume/{LOCKS_TABLE}", "select run_id from t", alias="t")
        assert [r["run_id"] for r in rows] == ["run-1"]

    def test_the_refused_run_does_not_free_the_holders_lock(self, tmp_path):
        lock(tmp_path, "run-1").acquire(ON)
        second = lock(tmp_path, "run-2")
        with pytest.raises(RunLockedError):
            second.acquire(ON)
        second.release()  # sans effet : il ne tient rien
        assert list(active_runs(f"{tmp_path}/flume/{LOCKS_TABLE}", "s1")) == ["run-1"]

    def test_another_source_is_independent(self, tmp_path):
        lock(tmp_path, "run-1", source="s1").acquire(ON)
        lock(tmp_path, "run-2", source="s2").acquire(ON)

    def test_after_a_release_the_next_run_goes(self, tmp_path):
        first = lock(tmp_path, "run-1")
        first.acquire(ON)
        first.release()
        lock(tmp_path, "run-2").acquire(ON)

    def test_an_expired_lock_does_not_block(self, tmp_path):
        """Un run mort ne libère rien : c'est l'échéance qui le fait."""
        lock(tmp_path, "run-1").acquire({"single_instance": True, "lock_ttl_seconds": 0.05})
        time.sleep(0.15)
        lock(tmp_path, "run-2").acquire(ON)

    def test_losing_the_recheck_backs_off_and_frees_its_own_row(self, tmp_path, monkeypatch):
        """Deux runs passent le premier contrôle ; chacun doit alors voir l'autre à
        la relecture. Ici le rival apparaît entre les deux."""
        from flume_lib import locks

        real = locks.active_runs
        calls = {"n": 0}
        rival = {"run-rival": stamp(datetime.now(timezone.utc))}

        def fake(uri, source_name, storage_options=None, now=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return {}
            return {**real(uri, source_name, storage_options, now), **rival}

        monkeypatch.setattr(locks, "active_runs", fake)
        loser = lock(tmp_path, "run-me")
        with pytest.raises(RunLockedError, match="run-rival"):
            loser.acquire(ON)

        assert loser.held is False
        monkeypatch.setattr(locks, "active_runs", real)
        assert real(f"{tmp_path}/flume/{LOCKS_TABLE}", "s1") == {}  # sa ligne est libérée

    @pytest.mark.parametrize("attempt", range(6))
    def test_two_runs_started_together_are_never_both_admitted(self, tmp_path, attempt):
        start = threading.Barrier(2)
        outcome = {}

        def contender(name):
            mine = lock(tmp_path, name)
            start.wait()
            try:
                mine.acquire(ON)
                outcome[name] = "admitted"
            except RunLockedError:
                outcome[name] = "refused"

        threads = [threading.Thread(target=contender, args=(n,)) for n in ("run-a", "run-b")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert list(outcome.values()).count("admitted") <= 1, outcome

    def test_the_ttl_defaults_to_six_hours(self):
        assert DEFAULT_LOCK_TTL_SECONDS == 6 * 3600


class _Session:
    payload: list = []

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        return FakeResponse({"items": _Session.payload})


@pytest.fixture
def real_http(monkeypatch):
    monkeypatch.setattr("flume_lib.source.requests.Session", _Session)
    _Session.payload = [{"id": 1}, {"id": 2}]
    return _Session


def locked_config(**extra):
    return {
        "name": "s1",
        "base_url": "https://api.test/items",
        "target_schema": "bronze",
        "target_table": "items",
        "pagination": {"type": "none", "items_field": "items"},
        "single_instance": True,
        **extra,
    }


class TestSingleInstanceInARun:
    def _held(self, tmp_path):
        return list(active_runs(f"{tmp_path}/flume/{LOCKS_TABLE}", "s1"))

    def test_a_run_takes_then_frees_the_lock(self, real_http, tmp_path):
        result = run_source(locked_config(), lakehouse_tables_path=str(tmp_path))
        assert result.status == "success", result.error_message
        assert self._held(tmp_path) == []
        # et le suivant passe
        assert run_source(locked_config(), lakehouse_tables_path=str(tmp_path)).status == "success"

    def test_a_run_refused_for_a_held_lock_reads_and_writes_nothing(self, real_http, tmp_path):
        lock(tmp_path, "someone-else").acquire(ON)

        result = run_source(locked_config(), lakehouse_tables_path=str(tmp_path))

        assert result.status == "failed"
        assert "RunLockedError" in result.error_message
        assert "someone-else" in result.error_message
        assert result.rows_loaded == 0
        assert query_table(f"{tmp_path}/bronze/items", "select * from t", alias="t") == []
        # le refus est tracé, et le verrou de l'autre n'a pas bougé
        log = query_table(f"{tmp_path}/flume/log_runs", "select status from t", alias="t")
        assert [r["status"] for r in log] == ["failed"]
        assert self._held(tmp_path) == ["someone-else"]

    def test_a_failing_run_frees_the_lock(self, real_http, tmp_path, monkeypatch):
        def boom(self, method, url, **kwargs):
            raise ValueError("boom")

        monkeypatch.setattr(_Session, "request", boom)
        result = run_source(locked_config(), lakehouse_tables_path=str(tmp_path))
        assert result.status == "failed"
        assert self._held(tmp_path) == []

    def test_an_interrupted_run_frees_the_lock(self, real_http, tmp_path, monkeypatch):
        def interrupt(self, method, url, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(_Session, "request", interrupt)
        with pytest.raises(KeyboardInterrupt):
            run_source(locked_config(), lakehouse_tables_path=str(tmp_path))
        assert self._held(tmp_path) == []

    def test_a_release_that_fails_is_a_warning_not_an_error(self, real_http, tmp_path, monkeypatch):
        def refuse(self):
            raise OSError("storage unavailable")

        monkeypatch.setattr(RunLock, "release", refuse)
        result = run_source(locked_config(), lakehouse_tables_path=str(tmp_path))

        assert result.status == "success", result.error_message
        assert any("could not be released" in w and "OSError" in w for w in result.warnings)

    def test_a_dry_run_takes_no_lock(self, real_http, tmp_path):
        result = run_source(locked_config(), lakehouse_tables_path=str(tmp_path), dry_run=True)
        assert result.status == "success", result.error_message
        assert query_table(f"{tmp_path}/flume/{LOCKS_TABLE}", "select * from t", alias="t") == []

    def test_a_source_without_the_option_is_unaffected(self, real_http, tmp_path):
        config = locked_config()
        del config["single_instance"]
        assert run_source(config, lakehouse_tables_path=str(tmp_path)).status == "success"
        assert query_table(f"{tmp_path}/flume/{LOCKS_TABLE}", "select * from t", alias="t") == []


class TestSingleInstanceConfig:
    def test_it_must_be_a_boolean(self):
        with pytest.raises(ConfigError, match="single_instance"):
            validate_config(cfg(single_instance="yes"))

    def test_a_ttl_without_the_lock_is_refused(self):
        with pytest.raises(ConfigError, match='"single_instance": true'):
            validate_config(cfg(lock_ttl_seconds=600))

    @pytest.mark.parametrize("bad", [0, -5, True, "600"])
    def test_the_ttl_must_be_a_positive_number(self, bad):
        with pytest.raises(ConfigError, match="lock_ttl_seconds"):
            validate_config(cfg(single_instance=True, lock_ttl_seconds=bad))

    def test_the_lock_must_outlive_the_longest_allowed_run(self):
        with pytest.raises(ConfigError, match="must exceed max_run_seconds"):
            validate_config(
                cfg(single_instance=True, lock_ttl_seconds=600, max_run_seconds=600)
            )

    def test_the_default_ttl_is_checked_against_the_run_limit_too(self):
        with pytest.raises(ConfigError, match="must exceed max_run_seconds"):
            validate_config(cfg(single_instance=True, max_run_seconds=DEFAULT_LOCK_TTL_SECONDS))

    def test_a_consistent_pair_is_accepted(self):
        validate_config(cfg(single_instance=True, lock_ttl_seconds=7200, max_run_seconds=3600))

    def test_a_run_limit_alone_needs_no_lock_check(self):
        validate_config(cfg(max_run_seconds=10 * DEFAULT_LOCK_TTL_SECONDS))


# ---------------------------------------------------------------------------
# Entretien des tables techniques
# ---------------------------------------------------------------------------


def watermark_rows(tmp_path):
    return query_table(
        f"{tmp_path}/flume/watermark",
        "select source_name, last_value from t order by source_name, updated_ts",
        alias="t",
    )


class TestCompactTechnicalTables:
    def _advance(self, tmp_path, source, values):
        for value in values:
            write_watermark(str(tmp_path), source, value)
            time.sleep(0.002)  # des updated_ts distincts

    def test_only_the_latest_watermark_of_each_source_stays(self, tmp_path):
        self._advance(tmp_path, "a", ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
        self._advance(tmp_path, "b", ["10", "20", "30"])

        report = compact_technical_tables(str(tmp_path))

        assert report["watermark"]["rows_pruned"] == 5
        assert watermark_rows(tmp_path) == [
            {"source_name": "a", "last_value": "2026-01-04"},
            {"source_name": "b", "last_value": "30"},
        ]
        # ce que les runs lisent n'a pas changé
        assert read_watermark(str(tmp_path), "a") == "2026-01-04"
        assert read_watermark(str(tmp_path), "b") == "30"

    def test_a_file_per_run_collapses(self, tmp_path):
        from deltalake import DeltaTable

        uri = f"{tmp_path}/flume/watermark"
        self._advance(tmp_path, "a", [f"2026-01-{d:02d}" for d in range(1, 9)])
        assert len(DeltaTable(uri).file_uris()) == 8  # un fichier par run
        compact_technical_tables(str(tmp_path))
        assert len(DeltaTable(uri).file_uris()) == 1

    def test_a_source_with_one_row_is_left_alone(self, tmp_path):
        self._advance(tmp_path, "solo", ["x1"])
        assert compact_technical_tables(str(tmp_path))["watermark"]["rows_pruned"] == 0
        assert read_watermark(str(tmp_path), "solo") == "x1"

    def test_a_quote_in_a_source_name_is_handled(self, tmp_path):
        self._advance(tmp_path, "o'brien", ["v1", "v2"])
        compact_technical_tables(str(tmp_path))
        assert read_watermark(str(tmp_path), "o'brien") == "v2"

    def test_expired_lock_rows_go_but_live_ones_stay(self, tmp_path):
        now = datetime.now(timezone.utc)
        uri = f"{tmp_path}/flume/{LOCKS_TABLE}"

        def row(run_id, expires):
            return {"source_name": "s", "run_id": run_id, "state": STATE_ACQUIRED,
                    "ts": stamp(now), "expires_ts": stamp(expires)}

        write_records(uri, [row("old", now - timedelta(days=3))])
        write_records(uri, [row("recent-but-expired", now - timedelta(hours=2))])
        write_records(uri, [row("live", now + timedelta(hours=3))])

        report = compact_technical_tables(str(tmp_path))

        assert report[LOCKS_TABLE]["rows_pruned"] == 1
        left = {r["run_id"] for r in query_table(uri, "select run_id from t", alias="t")}
        assert left == {"recent-but-expired", "live"}

    def test_log_runs_is_compacted_never_pruned(self, tmp_path):
        uri = f"{tmp_path}/flume/log_runs"
        for i in range(5):
            write_records(uri, [{"run_id": f"r{i}", "status": "success"}])

        report = compact_technical_tables(str(tmp_path))

        assert report["log_runs"]["rows_pruned"] == 0
        assert report["log_runs"]["files_added"] == 1
        assert len(query_table(uri, "select run_id from t", alias="t")) == 5

    def test_absent_tables_are_not_an_error(self, tmp_path):
        report = compact_technical_tables(str(tmp_path))
        assert report == {
            name: {"rows_pruned": 0, "files_removed": 0, "files_added": 0}
            for name in ("watermark", LOCKS_TABLE, "log_runs")
        }

    def test_vacuum_is_opt_in_and_reported(self, tmp_path):
        self._advance(tmp_path, "a", ["1", "2", "3"])
        assert "files_vacuumed" not in compact_technical_tables(str(tmp_path))["watermark"]
        report = compact_technical_tables(str(tmp_path), vacuum=True)
        assert isinstance(report["watermark"]["files_vacuumed"], int)

    def test_it_is_idempotent(self, tmp_path):
        self._advance(tmp_path, "a", ["1", "2", "3"])
        compact_technical_tables(str(tmp_path))
        second = compact_technical_tables(str(tmp_path))
        assert second["watermark"]["rows_pruned"] == 0
        assert watermark_rows(tmp_path) == [{"source_name": "a", "last_value": "3"}]

    def test_a_custom_schema_is_honoured(self, tmp_path):
        for value in ("1", "2"):
            write_watermark(str(tmp_path), "a", value, schema="ops")
            time.sleep(0.002)
        report = compact_technical_tables(str(tmp_path), schema="ops")
        assert report["watermark"]["rows_pruned"] == 1
        assert read_watermark(str(tmp_path), "a", schema="ops") == "2"
