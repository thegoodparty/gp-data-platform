"""resize: flip writer to the provisioned serving class + serve params + lock-down, write manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.core import aws
from loader.people_api.config import LoaderConfig
from loader.people_api.steps import resize as step
from tests._fakes import FakeConn, executed_sql, fake_connect

# Deliberately distinct from both DEFAULT_SERVE_INSTANCE_CLASS ("db.r6g.4xlarge") and the dev
# override ("db.t4g.medium") so these tests catch resize reading a hardcoded default instead of
# cfg.serve_instance_class.
_TEST_SERVE_CLASS = "db.r6g.2xlarge"

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        serve_instance_class=_TEST_SERVE_CLASS,
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
    ),
)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


_SETTLED_PROVISIONED = [
    {"DBInstanceClass": _TEST_SERVE_CLASS, "DBInstanceStatus": "available", "PendingModifiedValues": {}}
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
        # The class-change wait (loader.core.aws.wait_instance_class_applied) polls
        # describe_db_instances; defaults to already-settled at the target class so it
        # terminates on the first poll. Held on the last item once exhausted (mirrors a real
        # poll settling).
        self._describe_sequence = describe_sequence or _SETTLED_PROVISIONED
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


def test_resize_applies_provisioned_class_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = FakeRds()
    captured: dict = {}
    conn = FakeConn().queue_result((1,))
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.final_instance_class == _TEST_SERVE_CLASS
    assert manifest.backup_retention_days == 14 and manifest.deletion_protection is True
    assert not hasattr(manifest, "min_acu")
    assert not hasattr(manifest, "max_acu")
    # Exactly one modify_db_cluster call: resize's own lockdown (param group/backup/deletion
    # protection). flip_writer_to_provisioned only touches the instance — no
    # ServerlessV2ScalingConfiguration / cluster-level call for a provisioned class.
    cluster_calls = [kw for op, kw in rds_client.calls if op == "modify_cluster"]
    assert len(cluster_calls) == 1
    lockdown = cluster_calls[0]
    assert lockdown["DBClusterParameterGroupName"] == "gp-people-db-20260616-serve"
    assert lockdown["BackupRetentionPeriod"] == 14
    assert lockdown["DeletionProtection"] is True
    assert all("ServerlessV2ScalingConfiguration" not in kw for _op, kw in rds_client.calls)
    by = dict(rds_client.calls)
    assert by["modify_instance"]["DBInstanceClass"] == _TEST_SERVE_CLASS
    assert by["modify_instance"]["ApplyImmediately"] is True
    # Post-resize smoke check: a trivial SELECT 1 against the resized cluster, run after the
    # reboot settles and before the manifest is written.
    assert executed_sql(conn) == ["SELECT 1"]


def test_resize_smoke_check_failure_propagates_and_blocks_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # If the post-resize connectivity smoke check fails (the resized writer is unreachable), that
    # must surface as a resize failure — not a silently "complete" manifest over a cluster the
    # loader can't actually reach.
    rds_client = FakeRds()
    wrote: dict = {}

    def _boom(*a: object, **k: object):
        raise RuntimeError("could not connect to resized writer")

    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "connect_new", _boom)

    with pytest.raises(RuntimeError, match="could not connect"):
        step.run(_CFG, "20260616")
    assert "m" not in wrote  # no complete manifest written over an unreachable resize


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
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result((1,))))

    manifest = step.run(_CFG, "20260616")  # must not raise; modifies retried after settle

    assert manifest.status == "complete"
    ops = [c[0] for c in rds_client.calls]
    # The lockdown modify_cluster hit the fault once (raise + re-issue = 2 calls); there is no
    # separate conversion-level modify_cluster for a provisioned class (flip_writer_to_provisioned
    # only touches the instance), so 2 total. modify_instance (the class-change conversion) hit
    # its own fault once (raise + re-issue = 2), then reboot.
    assert ops.count("modify_cluster") == 2
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
                "PendingModifiedValues": {"DBInstanceClass": _TEST_SERVE_CLASS},
            },
            # now actually modifying
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": _TEST_SERVE_CLASS},
            },
            # settled on the target class
            {
                "DBInstanceClass": _TEST_SERVE_CLASS,
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ]
    )
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result((1,))))

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.final_instance_class == _TEST_SERVE_CLASS
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


def test_resize_waits_for_cluster_settle_before_instance_class_modify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # resize's lockdown modify_db_cluster leaves the cluster 'modifying'. Assert resize's
    # explicit cluster-settle wait happens strictly between that lockdown modify_db_cluster and
    # the writer's class-change modify_db_instance — i.e. resize waits proactively before
    # touching the instance, rather than relying on a fault-triggered retry.
    class _ClusterSettleWaiter(_FakeWaiter):
        def __init__(self, rds_client: FakeRds) -> None:
            self._rds = rds_client

        def wait(self, **kwargs: object) -> None:
            self._rds.calls.append(("wait_cluster", dict(kwargs)))

    class _WaitTrackingFakeRds(FakeRds):
        def get_waiter(self, name: str) -> _FakeWaiter:
            if name == "db_cluster_available":
                return _ClusterSettleWaiter(self)
            return _FakeWaiter()

    rds_client = _WaitTrackingFakeRds()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result((1,))))

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    ops = [op for op, _ in rds_client.calls]
    modify_cluster_idx = ops.index("modify_cluster")
    modify_instance_idx = ops.index("modify_instance")
    assert modify_cluster_idx < modify_instance_idx
    assert "wait_cluster" in ops[modify_cluster_idx + 1 : modify_instance_idx]


def test_resize_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done
