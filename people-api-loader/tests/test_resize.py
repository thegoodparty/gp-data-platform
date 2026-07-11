"""resize: flip to serverless v2 + serve params + lock-down, write manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.core import aws
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


_SETTLED_SERVERLESS = [
    {"DBInstanceClass": "db.serverless", "DBInstanceStatus": "available", "PendingModifiedValues": {}}
]


class FakeRds:
    def __init__(
        self,
        raise_on: dict[str, str] | None = None,
        *,
        persistent: bool = False,
        describe_sequence: list[dict] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._raise_on = raise_on or {}  # op -> ClientError code to raise after recording
        self._persistent = persistent  # raise every time (bad state) vs once (in-progress)
        self._raised: set[str] = set()
        # The serverless-flip wait (loader.core.aws.wait_instance_class_applied) polls
        # describe_db_instances; defaults to already-settled db.serverless so it terminates on
        # the first poll. Held on the last item once exhausted (mirrors a real poll settling).
        self._describe_sequence = describe_sequence or _SETTLED_SERVERLESS
        self._describe_calls = 0

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

    def describe_db_instances(self, **kw: Any) -> dict:
        self.calls.append(("describe_instance", kw))
        idx = min(self._describe_calls, len(self._describe_sequence) - 1)
        self._describe_calls += 1
        return {"DBInstances": [self._describe_sequence[idx]]}

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
    # Two modify_db_cluster calls: resize's own lockdown (param group/backup/deletion
    # protection), and the shared flip_writer_to_serverless conversion (ServerlessV2Scaling).
    cluster_calls = [kw for op, kw in rds_client.calls if op == "modify_cluster"]
    assert len(cluster_calls) == 2
    lockdown = next(c for c in cluster_calls if "DBClusterParameterGroupName" in c)
    conversion = next(c for c in cluster_calls if "ServerlessV2ScalingConfiguration" in c)
    assert lockdown["DBClusterParameterGroupName"] == "gp-people-db-20260616-serve"
    assert lockdown["BackupRetentionPeriod"] == 14
    assert lockdown["DeletionProtection"] is True
    assert conversion["ServerlessV2ScalingConfiguration"] == {"MinCapacity": 0.5, "MaxCapacity": 128.0}
    by = dict(rds_client.calls)
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
    # the lockdown modify_cluster hit the fault once (raise + re-issue = 2 calls); the
    # conversion's own modify_cluster (ServerlessV2Scaling) then succeeds straight away (the fake
    # raises "modify_cluster" only once, regardless of call site) = 3 total. modify_instance
    # (from the conversion) hit its own fault once (raise + re-issue = 2), then reboot.
    assert ops.count("modify_cluster") == 3
    assert ops.count("modify_instance") == 2
    assert "reboot" in ops


def test_resize_reboot_waits_for_class_applied_not_just_available(monkeypatch: pytest.MonkeyPatch) -> None:
    # The race this task fixes: Aurora reports the instance 'available' for a few seconds after
    # the class-change modify before it flips to 'modifying' and reboots. A plain db_instance_
    # available waiter would return on that stale state and let the explicit reboot below race
    # the conversion's own delayed reboot. Assert the describe sequence is actually POLLED past
    # the stale-available / pending-class state before reboot_db_instance is issued.
    monkeypatch.setattr(aws.time, "sleep", lambda *a, **k: None)
    rds_client = FakeRds(
        describe_sequence=[
            # stale: still 'available' on the OLD class immediately after the modify
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {"DBInstanceClass": "db.serverless"},
            },
            # now actually modifying
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": "db.serverless"},
            },
            # settled on the target class
            {
                "DBInstanceClass": "db.serverless",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ]
    )
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.final_instance_class == "db.serverless"
    ops = [c[0] for c in rds_client.calls]
    reboot_idx = ops.index("reboot")
    # at least 3 describes happened before reboot (rode through stale-available + modifying)
    assert ops[:reboot_idx].count("describe_instance") >= 3
    assert ops[reboot_idx - 1] == "describe_instance"  # the poll immediately preceding reboot


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


class _ClusterSettleWaiter(_FakeWaiter):
    """Cluster waiter stub that flips the fake cluster back to 'available' and records the wait,
    so tests can assert *when* it happened relative to modify_db_cluster calls."""

    def __init__(self, rds_client: _StrictSettleFakeRds) -> None:
        self._rds = rds_client

    def wait(self, **kwargs: object) -> None:
        self._rds.calls.append(("wait_cluster", dict(kwargs)))
        self._rds.cluster_modifying = False


class _StrictSettleFakeRds(FakeRds):
    """Models the real AWS behavior the sequencing bug depends on: a modify_db_cluster call that
    lands while the cluster is still 'modifying' from a prior modify_db_cluster raises
    InvalidDBClusterStateFault. Unlike the base FakeRds (which raises a fixed number of times
    regardless of cluster state), this fake tracks actual cluster state, so it only faults if a
    second modify_db_cluster is issued WITHOUT an intervening cluster-settle wait -- exactly the
    gap this task fixes.
    """

    def __init__(self) -> None:
        super().__init__()
        self.cluster_modifying = False

    def modify_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("modify_cluster", kw))
        if self.cluster_modifying:
            raise ClientError(
                {"Error": {"Code": "InvalidDBClusterStateFault", "Message": "still modifying"}},
                "ModifyDBCluster",
            )
        self.cluster_modifying = True

    def get_waiter(self, name: str) -> _FakeWaiter:
        if name == "db_cluster_available":
            return _ClusterSettleWaiter(self)
        return _FakeWaiter()


def test_resize_waits_for_cluster_settle_before_conversion_modify(monkeypatch: pytest.MonkeyPatch) -> None:
    # Models the sequencing bug: resize's lockdown modify_db_cluster leaves the cluster
    # 'modifying'; flip_writer_to_serverless's own modify_db_cluster (ServerlessV2Scaling) would
    # fault with InvalidDBClusterStateFault if issued before the cluster settles back to
    # 'available'. retry_after_settle inside the helper WOULD absorb that fault (wait + retry
    # once), but that turns a rare diagnostic warning into routine per-run noise. Assert resize's
    # explicit cluster-settle wait keeps the conversion's modify a single-shot call (no fault, no
    # retry) on the normal run.
    rds_client = _StrictSettleFakeRds()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    ops = [op for op, _ in rds_client.calls]
    cluster_modify_idxs = [i for i, op in enumerate(ops) if op == "modify_cluster"]
    # Exactly two modify_db_cluster calls total (lockdown + conversion): if the explicit wait
    # were missing, the conversion's modify would fault and retry_after_settle would re-issue it,
    # producing a THIRD modify_cluster call.
    assert len(cluster_modify_idxs) == 2
    # An explicit cluster-settle wait happened strictly between the lockdown modify and the
    # conversion modify -- i.e. resize waited proactively, rather than relying on the helper's
    # fault-triggered retry.
    assert "wait_cluster" in ops[cluster_modify_idxs[0] + 1 : cluster_modify_idxs[1]]


def test_resize_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done
