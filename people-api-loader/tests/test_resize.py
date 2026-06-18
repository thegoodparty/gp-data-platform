"""resize: flip to serverless v2 + serve params + lock-down, write manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import resize as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        serve_min_acu=0.5,
        serve_max_acu=128.0,
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
    ),
)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class FakeRds:
    def __init__(self, raise_on: dict[str, str] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._raise_on = raise_on or {}  # op -> ClientError code to raise after recording

    def _maybe_raise(self, op: str) -> None:
        if code := self._raise_on.get(op):
            raise ClientError({"Error": {"Code": code, "Message": "in progress"}}, op)

    def modify_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("modify_cluster", kw))
        self._maybe_raise("modify_cluster")

    def modify_db_instance(self, **kw: Any) -> None:
        self.calls.append(("modify_instance", kw))
        self._maybe_raise("modify_instance")

    def reboot_db_instance(self, **kw: Any) -> None:
        self.calls.append(("reboot", kw))

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


def test_resize_applies_serverless_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = FakeRds()
    captured: dict = {}
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.final_instance_class == "db.serverless"
    assert (manifest.min_acu, manifest.max_acu) == (0.5, 128.0)
    assert manifest.backup_retention_days == 14 and manifest.deletion_protection is True
    by = dict(rds_client.calls)
    assert by["modify_cluster"]["DBClusterParameterGroupName"] == "gp-people-db-20260616-serve"
    assert by["modify_cluster"]["ServerlessV2ScalingConfiguration"] == {
        "MinCapacity": 0.5,
        "MaxCapacity": 128.0,
    }
    assert by["modify_cluster"]["DeletionProtection"] is True
    assert by["modify_instance"]["DBInstanceClass"] == "db.serverless"


def test_resize_swallows_in_progress_modify(monkeypatch: pytest.MonkeyPatch) -> None:
    # A re-run that lands while a prior partial run's modify is still in flight must fall
    # through to the waiter, not hard-fail on InvalidDB*StateFault.
    rds_client = FakeRds(
        raise_on={
            "modify_cluster": "InvalidDBClusterStateFault",
            "modify_instance": "InvalidDBInstanceStateFault",
        }
    )
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    manifest = step.run(_CFG, "20260616")  # must not raise despite both modifies rejecting

    assert manifest.status == "complete"
    assert "reboot" in dict(rds_client.calls)  # proceeded past the swallowed modifies


def test_resize_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done
