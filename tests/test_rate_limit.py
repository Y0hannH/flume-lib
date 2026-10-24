"""`max_requests_per_second` et la dispersion du backoff.

L'horloge est factice : un test de cadence qui dort vraiment est lent et
instable, et ce qu'on veut vérifier est l'écart entre deux départs."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from flume_lib import ConfigError, run_source, validate_config
from flume_lib.source import RetryableHTTPError, RunTimeoutError, _build_wait, _Throttle

BASE = {
    "name": "s",
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
    "pagination": {"type": "none", "items_field": "items"},
}


class Clock:
    """Horloge que seul `sleep` fait avancer."""

    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(
        "flume_lib.source.time", SimpleNamespace(monotonic=c.monotonic, sleep=c.sleep)
    )
    return c


class TestThrottle:
    def test_the_first_request_goes_at_once_then_each_waits_its_turn(self, clock):
        throttle = _Throttle(2)  # une requête toutes les 0,5 s
        for _ in range(4):
            throttle.wait()
        assert clock.slept == [0.5, 0.5, 0.5]

    def test_time_spent_elsewhere_counts(self, clock):
        throttle = _Throttle(2)
        throttle.wait()
        clock.now += 0.3  # la requête précédente a pris 0,3 s
        throttle.wait()
        assert clock.slept == [pytest.approx(0.2)]

    def test_an_idle_period_does_not_bank_credit_for_a_burst(self, clock):
        throttle = _Throttle(2)
        throttle.wait()
        clock.now += 60
        throttle.wait()
        throttle.wait()
        throttle.wait()
        assert clock.slept == [0.5, 0.5]

    def test_a_slow_rate_is_a_long_interval(self, clock):
        throttle = _Throttle(0.1)
        throttle.wait()
        throttle.wait()
        assert clock.slept == [pytest.approx(10.0)]

    def test_no_limit_is_a_no_op(self, clock):
        throttle = _Throttle(None)
        for _ in range(5):
            throttle.wait()
        assert clock.slept == []

    def test_a_start_past_the_run_deadline_fails_instead_of_sleeping(self, clock):
        throttle = _Throttle(0.01)  # 100 s entre deux départs
        deadline = clock.now + 5
        throttle.wait(deadline, 5)
        with pytest.raises(
            RunTimeoutError, match=r"max_requests_per_second.*max_run_seconds \(5 s\)"
        ):
            throttle.wait(deadline, 5)
        assert clock.slept == []

    def test_a_start_inside_the_deadline_sleeps(self, clock):
        throttle = _Throttle(1)
        deadline = clock.now + 60
        throttle.wait(deadline, 60)
        throttle.wait(deadline, 60)
        assert clock.slept == [1.0]


class FakeResponse:
    def __init__(self, status, payload, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = str(payload)

    def json(self):
        return self._payload

    def close(self):
        pass


class Recording:
    """Session qui note l'heure (de l'horloge factice) de chaque départ."""

    script: list = []
    starts: list = []
    clock: Clock

    def __init__(self):
        self.headers = {}

    def close(self):
        pass

    def request(self, method, url, **kwargs):
        Recording.starts.append(Recording.clock.now)
        status, payload, headers = Recording.script.pop(0)
        return FakeResponse(status, payload, headers)


@pytest.fixture
def http(monkeypatch, clock):
    Recording.script, Recording.starts, Recording.clock = [], [], clock
    monkeypatch.setattr("flume_lib.source.requests.Session", Recording)


def ok(items):
    return (200, {"items": items}, {})


def run(config, tmp_path):
    return run_source(config, lakehouse_tables_path=str(tmp_path), dry_run=True)


class TestInARun:
    def test_pages_are_spaced(self, http, clock, tmp_path):
        config = {
            **BASE,
            "max_requests_per_second": 4,
            "pagination": {"type": "offset", "limit": 1, "items_field": "items"},
        }
        Recording.script = [ok([{"id": 1}]), ok([{"id": 2}]), ok([{"id": 3}]), ok([])]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        gaps = [b - a for a, b in zip(Recording.starts, Recording.starts[1:], strict=False)]
        assert gaps == [pytest.approx(0.25)] * 3

    def test_a_replay_is_throttled_too(self, http, clock, tmp_path):
        config = {**BASE, "max_requests_per_second": 2}
        Recording.script = [(503, {}, {"Retry-After": "0"}), ok([{"id": 1}])]
        result = run(config, tmp_path)

        assert result.status == "success", result.error_message
        assert Recording.starts[1] - Recording.starts[0] >= 0.5

    def test_without_the_key_nothing_waits(self, http, clock, tmp_path):
        Recording.script = [ok([{"id": 1}])]
        run(BASE, tmp_path)
        assert clock.slept == []

    def test_a_rate_too_slow_for_the_run_limit_fails_the_run_at_once(self, http, clock, tmp_path):
        config = {
            **BASE,
            "max_requests_per_second": 0.01,
            "max_run_seconds": 30,
            "pagination": {"type": "offset", "limit": 1, "items_field": "items"},
        }
        Recording.script = [ok([{"id": 1}]), ok([{"id": 2}])]
        result = run(config, tmp_path)

        assert result.status == "failed"
        assert "RunTimeoutError" in result.error_message
        assert "max_requests_per_second" in result.error_message
        assert len(Recording.starts) == 1
        assert clock.slept == []


class TestValidation:
    @pytest.mark.parametrize("bad", [0, -1, True, "5", None, [2]])
    def test_the_rate_must_be_a_positive_number(self, bad):
        with pytest.raises(ConfigError, match="max_requests_per_second"):
            validate_config({**BASE, "max_requests_per_second": bad})

    @pytest.mark.parametrize("good", [1, 0.5, 20])
    def test_a_positive_rate_is_accepted(self, good):
        validate_config({**BASE, "max_requests_per_second": good})

    @pytest.mark.parametrize("bad", [1, "yes", None])
    def test_jitter_must_be_a_boolean(self, bad):
        with pytest.raises(ConfigError, match="jitter"):
            validate_config({**BASE, "retry": {"jitter": bad}})

    def test_the_old_retry_keys_still_validate(self):
        validate_config({**BASE, "retry": {"max_attempts": 5, "backoff_multiplier": 2}})


def retry_state(exc, attempt=1):
    state = MagicMock()
    state.outcome.exception.return_value = exc
    state.attempt_number = attempt
    return state


class TestBackoffJitter:
    def test_the_delay_is_spread_between_half_and_the_full_backoff(self):
        wait = _build_wait({"backoff_multiplier": 4})
        delays = [wait(retry_state(RetryableHTTPError(503, "u"), attempt=2)) for _ in range(200)]

        assert all(4.0 <= d <= 8.0 for d in delays)  # 4 * 2**(2-1) = 8
        assert len(set(delays)) > 50

    def test_it_never_replays_at_once(self):
        wait = _build_wait({"backoff_multiplier": 1})
        delays = [wait(retry_state(RetryableHTTPError(503, "u"))) for _ in range(200)]
        assert min(delays) >= 0.5

    def test_jitter_can_be_turned_off(self):
        wait = _build_wait({"backoff_multiplier": 4, "jitter": False})
        delays = {wait(retry_state(RetryableHTTPError(503, "u"), attempt=2)) for _ in range(50)}
        assert delays == {8.0}

    def test_the_server_delay_is_never_dispersed(self):
        wait = _build_wait({})
        throttled = RetryableHTTPError(429, "u", retry_after=42.0)
        delays = {wait(retry_state(throttled)) for _ in range(50)}
        assert delays == {42.0}

    def test_the_backoff_still_grows_with_the_attempt(self):
        wait = _build_wait({"jitter": False})
        assert [wait(retry_state(RetryableHTTPError(503, "u"), attempt=n)) for n in (1, 2, 3)] == [
            1, 2, 4,
        ]
