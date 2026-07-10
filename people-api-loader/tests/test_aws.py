"""core.aws.get_ssm_parameter caches within the process so the parallel fan-out in
copy_s3 / build_indexes doesn't exceed SSM's GetParameter throttle."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from typing import cast

import pytest
from botocore.exceptions import ClientError

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


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "op")


def test_retry_after_settle_happy_path_calls_once() -> None:
    calls: list[int] = []
    settle_calls: list[int] = []
    aws.retry_after_settle(
        lambda: calls.append(1),
        fault_code="InvalidDBInstanceStateFault",
        settle=lambda: settle_calls.append(1),
    )
    assert calls == [1]
    assert settle_calls == []


def test_retry_after_settle_tolerated_fault_settles_then_reissues() -> None:
    calls: list[int] = []
    settle_calls: list[int] = []

    def call() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise _client_error("InvalidDBInstanceStateFault")

    aws.retry_after_settle(
        call, fault_code="InvalidDBInstanceStateFault", settle=lambda: settle_calls.append(1)
    )
    assert len(calls) == 2  # first raised, second is the re-issue after settle
    assert settle_calls == [1]


def test_retry_after_settle_other_client_error_propagates_and_skips_settle() -> None:
    settle_calls: list[int] = []

    def call() -> None:
        raise _client_error("InvalidParameterCombination")

    with pytest.raises(ClientError):
        aws.retry_after_settle(
            call, fault_code="InvalidDBInstanceStateFault", settle=lambda: settle_calls.append(1)
        )
    assert settle_calls == []  # a non-tolerated fault must not trigger settle


def test_retry_after_settle_second_fault_on_retry_propagates() -> None:
    calls: list[int] = []

    def call() -> None:
        calls.append(1)
        raise _client_error("InvalidDBInstanceStateFault")

    with pytest.raises(ClientError):
        aws.retry_after_settle(call, fault_code="InvalidDBInstanceStateFault", settle=lambda: None)
    assert len(calls) == 2  # initial call + one re-issue after settle; the second fault propagates


class _FakeDescribeRds:
    """Minimal RDS double: describe_db_instances replays a fixed sequence, holding the last item."""

    def __init__(self, sequence: list[dict]) -> None:
        self._sequence = sequence
        self.describe_calls = 0

    def describe_db_instances(self, **kw: object) -> dict:
        idx = min(self.describe_calls, len(self._sequence) - 1)
        self.describe_calls += 1
        return {"DBInstances": [self._sequence[idx]]}


def test_wait_instance_class_applied_returns_once_settled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aws.time, "sleep", lambda *a, **k: None)
    fake = _FakeDescribeRds(
        [
            # first poll: modify issued but not yet applied (stale 'available' / pending class)
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": "db.r8g.48xlarge"},
            },
            # second poll: settled on the target class
            {
                "DBInstanceClass": "db.r8g.48xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ]
    )
    aws.wait_instance_class_applied(fake, "writer-1", "db.r8g.48xlarge")  # ty: ignore[invalid-argument-type]
    assert fake.describe_calls == 2


def test_wait_instance_class_applied_raises_when_it_never_settles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aws.time, "sleep", lambda *a, **k: None)
    fake = _FakeDescribeRds(
        [
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": "db.r8g.48xlarge"},
            }
        ]
    )
    with pytest.raises(RuntimeError, match="did not reach class"):
        aws.wait_instance_class_applied(
            fake,  # ty: ignore[invalid-argument-type]
            "writer-1",
            "db.r8g.48xlarge",
            poll_seconds=0,
            max_polls=3,
        )
    assert fake.describe_calls == 3
