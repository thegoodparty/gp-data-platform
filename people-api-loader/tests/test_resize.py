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
    def __init__(self, raise_on: dict[str, str] | None = None, *, persistent: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._raise_on = raise_on or {}  # op -> ClientError code to raise after recording
        self._persistent = persistent  # raise every time (bad state) vs once (in-progress)
        self._raised: set[str] = set()

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


def test_resize_retries_in_progress_modify(monkeypatch: pytest.MonkeyPatch) -> None:
    # A re-run that lands while a prior partial run's modify is still in flight: the first
    # call hits InvalidDB*StateFault, we wait, then RE-ISSUE so the settings are applied.
    rds_client = FakeRds(
        raise_on={
            "modify_cluster": "InvalidDBClusterStateFault",
            "modify_instance": "InvalidDBInstanceStateFault",
        }
    )
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    manifest = step.run(_CFG, "20260616")  # must not raise; modifies retried after settle

    assert manifest.status == "complete"
    ops = [c[0] for c in rds_client.calls]
    # each modify was re-issued after the fault (so its settings actually applied), then reboot
    assert ops.count("modify_cluster") == 2
    assert ops.count("modify_instance") == 2
    assert "reboot" in ops


def test_resize_propagates_persistent_modify_fault(monkeypatch: pytest.MonkeyPatch) -> None:
    # A genuinely bad cluster state (not just in-progress) raises the same fault every time;
    # it must surface, not be masked into a "complete" manifest over a misconfigured cluster.
    rds_client = FakeRds(raise_on={"modify_cluster": "InvalidDBClusterStateFault"}, persistent=True)
    wrote: dict = {}
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")

    with pytest.raises(ClientError):
        step.run(_CFG, "20260616")
    assert "m" not in wrote  # no complete manifest written


def test_resize_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done
