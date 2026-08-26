import base64

import pytest
from include.custom_functions.ballotready_graphql import (
    RateLimiter,
    chunked,
    encode_node_id,
    is_retryable_status,
    retry_wait_seconds,
)


def test_encode_node_id_uses_the_ballot_factory_global_id_format():
    encoded = encode_node_id("Candidacy", 12345)
    assert base64.b64decode(encoded).decode() == "gid://ballot-factory/Candidacy/12345"


def test_encode_node_id_varies_by_node_type():
    assert encode_node_id("Issue", 7) != encode_node_id("Geofence", 7)


def test_chunked_splits_into_fixed_size_batches():
    assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunked_rejects_a_size_below_one():
    with pytest.raises(ValueError, match="chunk size"):
        list(chunked([1, 2], 0))


class FakeClock:
    """A monotonic clock that only advances when sleep() is called."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_rate_limiter_spaces_calls_by_the_configured_interval():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()  # first call is free
    limiter.acquire()  # must wait 0.5s

    assert clock.slept == [pytest.approx(0.5)]


def test_rate_limiter_does_not_sleep_when_enough_time_has_passed():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()
    clock.now += 10.0
    limiter.acquire()

    assert clock.slept == []


def test_pause_for_holds_every_subsequent_acquire():
    """A 429 seen by one worker must stop all of them, not just the one that saw it."""
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=1000.0, sleep=clock.sleep, clock=clock.time)

    limiter.pause_for(30.0)
    limiter.acquire()

    assert clock.slept and clock.slept[0] == pytest.approx(30.0)


def test_rate_limiter_rejects_a_non_positive_rate():
    with pytest.raises(ValueError, match="requests_per_second"):
        RateLimiter(requests_per_second=0)


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retryable_statuses(status):
    assert is_retryable_status(status)


@pytest.mark.parametrize("status", [200, 400, 401, 403, 404, 422])
def test_non_retryable_statuses(status):
    assert not is_retryable_status(status)


def test_retry_wait_honors_a_well_formed_retry_after_header():
    assert retry_wait_seconds({"Retry-After": "12"}, attempt=0) == 12.0


def test_retry_wait_reads_retry_after_case_insensitively():
    assert retry_wait_seconds({"retry-after": "5"}, attempt=0) == 5.0


def test_retry_wait_caps_retry_after_at_max_backoff():
    assert retry_wait_seconds({"Retry-After": "9999"}, attempt=0, max_backoff=60.0) == 60.0


def test_retry_wait_falls_back_to_jittered_backoff_when_retry_after_is_garbage():
    # rng returns its upper bound so the growth is assertable.
    wait = retry_wait_seconds({"Retry-After": "soon"}, attempt=2, base_backoff=1.0, rng=lambda lo, hi: hi)
    assert wait == 4.0


def test_retry_wait_backoff_grows_with_the_attempt_number():
    rng = lambda lo, hi: hi  # noqa: E731
    waits = [retry_wait_seconds({}, attempt=n, base_backoff=1.0, rng=rng) for n in range(4)]
    assert waits == [1.0, 2.0, 4.0, 8.0]
