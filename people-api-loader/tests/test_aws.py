"""core.aws.get_ssm_parameter caches within the process so the parallel fan-out in
copy_s3 / build_indexes doesn't exceed SSM's GetParameter throttle."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from typing import cast

import pytest

from loader.core import aws
from loader.core.config import BaseLoaderConfig

_CFG = cast(BaseLoaderConfig, SimpleNamespace(aws_profile=None, aws_region="us-west-2"))


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    aws._ssm_cache.clear()


class _CountingSsm:
    def __init__(self) -> None:
        self.calls = 0
        self._lock = threading.Lock()

    def get_parameter(self, Name: str, WithDecryption: bool) -> dict:
        with self._lock:
            self.calls += 1
        time.sleep(0.02)  # widen the race window so a lock-free cache would double-fetch
        return {"Parameter": {"Value": f"val-{Name}"}}


def test_get_ssm_parameter_caches_repeated_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _CountingSsm()
    monkeypatch.setattr(aws, "ssm", lambda cfg: fake)
    assert aws.get_ssm_parameter(_CFG, "p") == "val-p"
    assert aws.get_ssm_parameter(_CFG, "p") == "val-p"  # served from cache
    assert fake.calls == 1
    aws.get_ssm_parameter(_CFG, "q")  # a different name is a separate cache entry
    assert fake.calls == 2


def test_get_ssm_parameter_cold_concurrent_burst_makes_one_call(monkeypatch: pytest.MonkeyPatch) -> None:
    # 64 threads hit the same uncached name simultaneously (the copy_s3/build_indexes fan-out
    # shape); the lock must collapse them to exactly one live GetParameter call.
    fake = _CountingSsm()
    monkeypatch.setattr(aws, "ssm", lambda cfg: fake)
    results: list[str] = []
    rlock = threading.Lock()

    def worker() -> None:
        v = aws.get_ssm_parameter(_CFG, "conn")
        with rlock:
            results.append(v)

    threads = [threading.Thread(target=worker) for _ in range(64)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert fake.calls == 1  # cold burst collapsed to a single API call
    assert results == ["val-conn"] * 64


def test_session_without_assume_role_is_plain() -> None:
    import boto3

    aws._session.cache_clear()
    s = aws._session(None, "us-west-2")
    assert isinstance(s, boto3.Session)
    assert s.region_name == "us-west-2"


def test_session_with_assume_role_passes_role_and_external_id(monkeypatch: pytest.MonkeyPatch) -> None:
    import boto3

    aws._session.cache_clear()
    captured: dict[str, object] = {}

    class _Fetcher:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def fetch_credentials(self) -> dict[str, str]:  # deferred — never called here
            raise AssertionError("credentials should be fetched lazily, not at session build")

    monkeypatch.setattr(aws, "AssumeRoleCredentialFetcher", _Fetcher)
    s = aws._session(None, "us-west-2", "arn:aws:iam::333:role/admin", "ext-123")

    assert isinstance(s, boto3.Session)
    assert captured["role_arn"] == "arn:aws:iam::333:role/admin"
    extra = cast(dict, captured["extra_args"])
    assert extra["ExternalId"] == "ext-123"
    assert extra["RoleSessionName"] == "people-api-loader"


def test_session_assume_role_omits_external_id_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    aws._session.cache_clear()
    captured: dict[str, object] = {}

    class _Fetcher:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def fetch_credentials(self) -> dict[str, str]:  # deferred — never called here
            raise AssertionError("credentials should be fetched lazily, not at session build")

    monkeypatch.setattr(aws, "AssumeRoleCredentialFetcher", _Fetcher)
    aws._session(None, "us-west-2", "arn:aws:iam::333:role/admin", None)

    assert "ExternalId" not in cast(dict, captured["extra_args"])
