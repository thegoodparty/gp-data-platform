"""scale_down: on-failure cost guard — flip the writer to db.serverless, no serve lockdown."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.core import aws
from loader.people_api.config import LoaderConfig
from loader.people_api.steps import scale_down as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        serve_min_acu=0.5,
        serve_max_acu=128.0,
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
    ),
)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aws.time, "sleep", lambda *a, **k: None)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


_SETTLED_SERVERLESS = [
    {"DBInstanceClass": "db.serverless", "DBInstanceStatus": "available", "PendingModifiedValues": {}}
]


class _FakeRds:
    def __init__(
        self,
        raise_on: dict[str, str] | None = None,
        *,
        persistent: bool = False,
        describe_sequence: list[dict] | None = None,
        describe_not_found: bool = False,
    ) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._raise_on = raise_on or {}
        self._persistent = persistent
        self._raised: set[str] = set()
        self._describe_sequence = describe_sequence or _SETTLED_SERVERLESS
        self._describe_calls = 0
        self._describe_not_found = describe_not_found

    def _maybe_raise(self, op: str) -> None:
        code = self._raise_on.get(op)
        if code and (self._persistent or op not in self._raised):
            self._raised.add(op)
            raise ClientError({"Error": {"Code": code, "Message": "in progress"}}, op)

    def modify_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("modify_cluster", kw))
        self._maybe_raise("modify_cluster")

    def modify_db_instance(self, **kw: Any) -> None:
        self.calls.append(("modify_instance", kw))
        self._maybe_raise("modify_instance")

    def describe_db_instances(self, **kw: Any) -> dict:
        self.calls.append(("describe_instance", kw))
        if self._describe_not_found:
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "not found"}}, "DescribeDBInstances"
            )
        idx = min(self._describe_calls, len(self._describe_sequence) - 1)
        self._describe_calls += 1
        return {"DBInstances": [self._describe_sequence[idx]]}

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


def test_scale_down_flips_provisioned_writer_to_serverless(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = _FakeRds(
        describe_sequence=[
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
            *_SETTLED_SERVERLESS,
        ]
    )
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)

    step.run(_CFG, "20260616")

    by = dict(rds_client.calls)
    assert by["modify_cluster"]["ServerlessV2ScalingConfiguration"] == {
        "MinCapacity": 0.5,
        "MaxCapacity": 128.0,
    }
    assert "DBClusterParameterGroupName" not in by["modify_cluster"]
    assert "DeletionProtection" not in by["modify_cluster"]
    assert by["modify_instance"]["DBInstanceClass"] == "db.serverless"
    assert by["modify_instance"]["ApplyImmediately"] is True
    ops = [c[0] for c in rds_client.calls]
    assert "reboot" not in ops


def test_scale_down_noop_when_already_serverless(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = _FakeRds()  # default describe sequence is already db.serverless
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)

    step.run(_CFG, "20260616")

    ops = [c[0] for c in rds_client.calls]
    assert "modify_cluster" not in ops
    assert "modify_instance" not in ops


def test_scale_down_noop_when_instance_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = _FakeRds(describe_not_found=True)
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)

    step.run(_CFG, "20260616")  # must not raise

    ops = [c[0] for c in rds_client.calls]
    assert "modify_cluster" not in ops
    assert "modify_instance" not in ops
