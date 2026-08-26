import base64

import pytest
from include.custom_functions.ballotready_graphql import RateLimiter, chunked, encode_node_id


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
