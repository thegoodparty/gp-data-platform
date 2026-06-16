"""provision: create cluster + instance + param groups, attach role, write manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import provision as step
from tests._fakes import FakeConn, fake_connect

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        engine_version="16.8",
        db_subnet_group="subnets",
        security_group_id="sg-x",
        prod_db_user="people_admin",
        prod_db_name="people_prod",
        kms_key_arn="arn:kms",
        load_instance_class="db.r7g.16xlarge",
        s3_import_role_arn="arn:aws:iam::1:role/rds-s3-import",
        aws_region="us-west-2",
        vpc_id="vpc-1",
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_load_param_group=lambda rd: f"gp-people-db-{rd}-load",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
        new_master_secret_id=lambda rd: f"gp-people-db/{rd}/master",
        tags_as_aws=lambda: [{"Key": "Project", "Value": "gp-api"}],
    ),
)


def _not_found(op: str) -> ClientError:
    return ClientError({"Error": {"Code": "DBClusterNotFoundFault", "Message": "nf"}}, op)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class FakeRds:
    def __init__(self, existing: dict[str, dict] | None = None, instances: set[str] | None = None) -> None:
        self.clusters: dict[str, dict] = dict(existing or {})
        self.instances: set[str] = set(instances or set())
        self.calls: list[tuple[str, dict]] = []

    def describe_db_clusters(self, DBClusterIdentifier: str) -> dict:
        if DBClusterIdentifier not in self.clusters:
            raise _not_found("DescribeDBClusters")
        return {"DBClusters": [{"Endpoint": self.clusters[DBClusterIdentifier]["Endpoint"]}]}

    def describe_db_instances(self, DBInstanceIdentifier: str) -> dict:
        if DBInstanceIdentifier not in self.instances:
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "nf"}}, "DescribeDBInstances"
            )
        return {"DBInstances": [{"DBInstanceIdentifier": DBInstanceIdentifier}]}

    def create_db_cluster_parameter_group(self, **kw: Any) -> None:
        self.calls.append(("param_group", kw))

    def create_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("cluster", kw))
        self.clusters[kw["DBClusterIdentifier"]] = {"Endpoint": f"{kw['DBClusterIdentifier']}.rds.aws"}

    def create_db_instance(self, **kw: Any) -> None:
        self.calls.append(("instance", kw))
        self.instances.add(kw["DBInstanceIdentifier"])

    def add_role_to_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("role", kw))

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()

    def names(self) -> list[str]:
        return [c[0] for c in self.calls]


class FakeEc2:
    def __init__(self, vpce_id: str = "vpce-abc") -> None:
        self._id = vpce_id

    def describe_vpc_endpoints(self, Filters: list) -> dict:
        return {"VpcEndpoints": [{"VpcEndpointId": self._id}] if self._id else []}


def _patch(monkeypatch: pytest.MonkeyPatch, rds_client: FakeRds, ec2_client: FakeEc2) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ec2", lambda cfg: ec2_client)
    monkeypatch.setattr(
        step, "put_secret", lambda cfg, sid, val, description="": captured.setdefault("secret", sid)
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    return captured


def test_provision_creates_cluster_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client, ec2_client = FakeRds(), FakeEc2()
    captured = _patch(monkeypatch, rds_client, ec2_client)
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert manifest.cluster_id == "gp-people-db-20260616"
    assert manifest.writer_endpoint == "gp-people-db-20260616.rds.aws"
    assert manifest.vpc_endpoint_id == "vpce-abc"
    assert manifest.master_secret_id == "gp-people-db/20260616/master"
    # both param groups, the cluster, the instance, and the role attach happened
    assert rds_client.names().count("param_group") == 2
    assert (
        "cluster" in rds_client.names() and "instance" in rds_client.names() and "role" in rds_client.names()
    )
    assert captured["secret"] == "gp-people-db/20260616/master"


def test_provision_idempotent_reuses_existing_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    # Cluster + instance already exist -> no create, no new master password; still attaches role.
    rds_client = FakeRds(
        existing={"gp-people-db-20260616": {"Endpoint": "existing.rds.aws"}},
        instances={"gp-people-db-20260616-writer"},
    )
    captured = _patch(monkeypatch, rds_client, FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    manifest = step.run(_CFG, "20260616")

    assert manifest.writer_endpoint == "existing.rds.aws"
    assert "cluster" not in rds_client.names()  # not re-created
    assert "instance" not in rds_client.names()  # not re-created
    assert "secret" not in captured  # no new password stored
    assert "role" in rds_client.names()  # role attach is still idempotently ensured


def test_provision_recovers_missing_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    # Partial prior run: cluster exists but its writer instance does not. Re-run must create
    # the instance (not skip it and then fail the availability waiter) without a new password.
    rds_client = FakeRds(existing={"gp-people-db-20260616": {"Endpoint": "existing.rds.aws"}})
    captured = _patch(monkeypatch, rds_client, FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    step.run(_CFG, "20260616")

    assert "cluster" not in rds_client.names()  # cluster reused
    assert "instance" in rds_client.names()  # missing instance created
    assert "secret" not in captured  # no new password (cluster's master is unchanged)


def test_provision_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260616") is done


def test_provision_warns_when_s3_vpce_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    rds_client = FakeRds()
    _patch(monkeypatch, rds_client, FakeEc2(vpce_id=""))  # no S3 VPCE in the VPC
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    manifest = step.run(_CFG, "20260616")
    assert manifest.vpc_endpoint_id == ""  # recorded empty, not a hard failure
    assert manifest.status == "complete"
