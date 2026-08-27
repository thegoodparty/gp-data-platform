import threading
import time

import pytest

from bedrock_clients.throttle import RateLimiter


def test_semaphore_bounds_in_flight():
    limiter = RateLimiter(max_concurrency=2, requests_per_minute=1_000_000, tokens_per_minute=None)
    active = 0
    peak = 0
    lock = threading.Lock()

    def work():
        nonlocal active, peak
        with limiter.acquire():
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.05)
            with lock:
                active -= 1

    threads = [threading.Thread(target=work) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak <= 2


def test_rpm_bucket_blocks_after_burst():
    # rpm=120 -> one request per 0.5s once the initial burst allowance is
    # spent. Three calls against a burst capacity of 1 must take >= ~1s.
    limiter = RateLimiter(max_concurrency=10, requests_per_minute=120, tokens_per_minute=None)
    start = time.monotonic()
    for _ in range(3):
        with limiter.acquire():
            pass
    elapsed = time.monotonic() - start
    assert elapsed >= 2 * limiter.seconds_per_request * 0.8  # generous margin


def test_tpm_bucket_debits_and_reconciles():
    limiter = RateLimiter(max_concurrency=10, requests_per_minute=1_000_000, tokens_per_minute=6_000)
    before = limiter.tokens_available()
    with limiter.acquire(estimated_tokens=10):
        pass
    limiter.reconcile(estimated_tokens=10, actual_tokens=40)
    # 40 real tokens must be debited in total (10 at acquire + 30 at reconcile),
    # modulo refill during the call: at 100 tokens/s the slack of 50 tolerates
    # half a second of scheduler stall on a loaded CI runner.
    assert limiter.tokens_available() <= before - 40 + 50


def test_reconcile_credits_overestimate():
    limiter = RateLimiter(max_concurrency=10, requests_per_minute=1_000_000, tokens_per_minute=6_000)
    before = limiter.tokens_available()
    with limiter.acquire(estimated_tokens=50):
        pass
    limiter.reconcile(estimated_tokens=50, actual_tokens=10)
    assert limiter.tokens_available() >= before - 10 - 50


def test_estimate_beyond_capacity_fails_fast():
    limiter = RateLimiter(max_concurrency=1, requests_per_minute=1000, tokens_per_minute=100)
    with pytest.raises(ValueError, match="never acquire"):
        with limiter.acquire(estimated_tokens=101):
            pass


def test_zero_tokens_per_minute_rejected():
    with pytest.raises(ValueError, match="positive"):
        RateLimiter(max_concurrency=1, requests_per_minute=1000, tokens_per_minute=0)


def test_no_tpm_bucket_ignores_tokens():
    limiter = RateLimiter(max_concurrency=1, requests_per_minute=1_000_000, tokens_per_minute=None)
    with limiter.acquire(estimated_tokens=10**9):
        pass
    limiter.reconcile(estimated_tokens=10**9, actual_tokens=10**9)
    assert limiter.tokens_available() is None
