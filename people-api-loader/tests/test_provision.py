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
        prod_db_port=5432,
        kms_key_arn="arn:kms",
        load_instance_class="db.r7g.16xlarge",
        s3_import_role_arn="arn:aws:iam::1:role/rds-s3-import",
        aws_region="us-west-2",
        vpc_id="vpc-1",
        new_cluster_id=lambda rd: f"gp-people-db-{rd}",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-writer",
        new_load_param_group=lambda rd: f"gp-people-db-{rd}-load",
        new_serve_param_group=lambda rd: f"gp-people-db-{rd}-serve",
        new_conn_param=lambda rd: f"people-db-connection-string-dev-{rd}",
        tags_as_aws=lambda: [{"Key": "Project", "Value": "gp-api"}],
    ),
)


def _not_found(op: str) -> ClientError:
    return ClientError({"Error": {"Code": "DBClusterNotFoundFault", "Message": "nf"}}, op)


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class FakeRds:
    def __init__(
        self,
        existing: dict[str, dict] | None = None,
        instances: set[str] | None = None,
        *,
        delete_raises: bool = False,
        param_group_exists: bool = False,
    ) -> None:
        self.clusters: dict[str, dict] = dict(existing or {})
        self.instances: set[str] = set(instances or set())
        self.calls: list[tuple[str, dict]] = []
        self._delete_raises = delete_raises
        self._param_group_exists = param_group_exists

    def describe_db_clusters(self, DBClusterIdentifier: str) -> dict:
        if DBClusterIdentifier not in self.clusters:
            raise _not_found("DescribeDBClusters")
        cluster = self.clusters[DBClusterIdentifier]
        base: dict = {"Endpoint": cluster["Endpoint"]}
        if "AssociatedRoles" in cluster:
            base["AssociatedRoles"] = cluster["AssociatedRoles"]
        return {"DBClusters": [base]}

    def describe_db_instances(self, DBInstanceIdentifier: str) -> dict:
        if DBInstanceIdentifier not in self.instances:
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "nf"}}, "DescribeDBInstances"
            )
        return {"DBInstances": [{"DBInstanceIdentifier": DBInstanceIdentifier}]}

    def create_db_cluster_parameter_group(self, **kw: Any) -> None:
        self.calls.append(("param_group", kw))
        if self._param_group_exists:
            raise ClientError(
                {"Error": {"Code": "DBParameterGroupAlreadyExists", "Message": "exists"}},
                "CreateDBClusterParameterGroup",
            )

    def create_db_cluster(self, **kw: Any) -> dict:
        self.calls.append(("cluster", kw))
        endpoint = f"{kw['DBClusterIdentifier']}.rds.aws"
        self.clusters[kw["DBClusterIdentifier"]] = {"Endpoint": endpoint}
        return {"DBCluster": {"Endpoint": endpoint}}

    def create_db_instance(self, **kw: Any) -> None:
        self.calls.append(("instance", kw))
        self.instances.add(kw["DBInstanceIdentifier"])

    def add_role_to_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("role", kw))

    def delete_db_cluster(self, **kw: Any) -> None:
        self.calls.append(("delete_cluster", kw))
        if self._delete_raises:
            # Aurora is still 'creating', so the rollback delete is rejected.
            raise ClientError(
                {"Error": {"Code": "InvalidDBClusterStateFault", "Message": "creating"}}, "DeleteDBCluster"
            )
        self.clusters.pop(kw["DBClusterIdentifier"], None)

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
        step, "put_ssm_parameter", lambda cfg, name, value, **k: captured.update(param=name, conninfo=value)
    )
    # The reuse branch reads the param back to fail fast; default it present.
    monkeypatch.setattr(step, "get_ssm_parameter", lambda cfg, name, **k: "postgresql://u:p@h:5432/d")
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
    assert manifest.conn_param == "people-db-connection-string-dev-20260616"
    # both param groups, the cluster, the instance, and the role attach happened
    assert rds_client.names().count("param_group") == 2
    assert (
        "cluster" in rds_client.names() and "instance" in rds_client.names() and "role" in rds_client.names()
    )
    # the connection string was stored in SSM with the endpoint + master password embedded,
    # and forces TLS so the password can't traverse a plaintext-negotiated channel
    assert captured["param"] == "people-db-connection-string-dev-20260616"
    assert captured["conninfo"].startswith("postgresql://people_admin:")
    assert "@gp-people-db-20260616.rds.aws:5432/people_prod?sslmode=require" in captured["conninfo"]


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
    assert "param" not in captured  # no new connection string stored (password unknown on reuse)
    assert "role" in rds_client.names()  # role attach is still idempotently ensured


def test_provision_skips_role_attach_when_already_active(monkeypatch: pytest.MonkeyPatch) -> None:
    # An ACTIVE role association is treated as done -> no add_role_to_db_cluster (which would
    # need iam:PassRole). A non-ACTIVE/absent role would fall through to the attach.
    rds_client = FakeRds(
        existing={
            "gp-people-db-20260616": {
                "Endpoint": "existing.rds.aws",
                "AssociatedRoles": [{"RoleArn": "arn:aws:iam::1:role/rds-s3-import", "Status": "ACTIVE"}],
            }
        },
        instances={"gp-people-db-20260616-writer"},
    )
    _patch(monkeypatch, rds_client, FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    step.run(_CFG, "20260616")

    assert "role" not in rds_client.names()  # ACTIVE role -> no re-attach


def test_provision_recovers_missing_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    # Partial prior run: cluster exists but its writer instance does not. Re-run must create
    # the instance (not skip it and then fail the availability waiter) without a new password.
    rds_client = FakeRds(existing={"gp-people-db-20260616": {"Endpoint": "existing.rds.aws"}})
    captured = _patch(monkeypatch, rds_client, FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    step.run(_CFG, "20260616")

    assert "cluster" not in rds_client.names()  # cluster reused
    assert "instance" in rds_client.names()  # missing instance created
    assert "param" not in captured  # no new connection string (cluster's master is unchanged)


def test_provision_rolls_back_cluster_when_conn_param_write_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # The generated master password lives only in memory until the SSM write. If that write
    # fails, the cluster must be deleted so a re-run regenerates rather than looping on
    # ParameterNotFound. Verify the cluster is created then rolled back and the error propagates.
    rds_client = FakeRds()
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ec2", lambda cfg: FakeEc2())
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    def _boom(cfg: object, name: str, value: str, **k: object) -> None:
        raise RuntimeError("ssm put denied")

    monkeypatch.setattr(step, "put_ssm_parameter", _boom)

    with pytest.raises(RuntimeError, match="ssm put denied"):
        step.run(_CFG, "20260616")

    assert "cluster" in rds_client.names()  # cluster was created
    assert "delete_cluster" in rds_client.names()  # then rolled back
    assert "instance" not in rds_client.names()  # bailed before creating the instance


def test_provision_reraises_write_error_even_if_rollback_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # When the cluster is still 'creating', the rollback delete is itself rejected. We must
    # surface the original SSM write error (logging the orphaned cluster), not the delete error.
    rds_client = FakeRds(delete_raises=True)
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ec2", lambda cfg: FakeEc2())
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    def _boom(cfg: object, name: str, value: str, **k: object) -> None:
        raise RuntimeError("ssm put denied")

    monkeypatch.setattr(step, "put_ssm_parameter", _boom)

    with pytest.raises(RuntimeError, match="ssm put denied"):  # original error, not the delete fault
        step.run(_CFG, "20260616")

    assert "delete_cluster" in rds_client.names()  # rollback was attempted despite failing


def test_provision_fails_fast_when_conn_param_missing_on_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    # Orphaned cluster: it exists but its connection param was never written (prior run's SSM
    # write and rollback both failed). The re-run must fail fast in the reuse branch, before
    # the multi-minute instance waiter, rather than surfacing an opaque ParameterNotFound later.
    rds_client = FakeRds(existing={"gp-people-db-20260616": {"Endpoint": "existing.rds.aws"}})
    monkeypatch.setattr(step, "rds", lambda cfg: rds_client)
    monkeypatch.setattr(step, "ec2", lambda cfg: FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    def _missing(cfg: object, name: str, **k: object) -> str:
        raise ClientError({"Error": {"Code": "ParameterNotFound", "Message": "nf"}}, "GetParameter")

    monkeypatch.setattr(step, "get_ssm_parameter", _missing)

    with pytest.raises(ClientError):
        step.run(_CFG, "20260616")

    assert "instance" not in rds_client.names()  # bailed before the instance waiter


def test_provision_tolerates_existing_param_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    # Partial prior run left the param groups behind: create_db_cluster_parameter_group raises
    # DBParameterGroupAlreadyExists, which _ensure_cluster_param_group must swallow so the run
    # completes (exercises the ignore_client_errors guard + the param_group_exists log branch).
    rds_client = FakeRds(param_group_exists=True)
    _patch(monkeypatch, rds_client, FakeEc2())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)

    manifest = step.run(_CFG, "20260616")

    assert manifest.status == "complete"
    assert rds_client.names().count("param_group") == 2  # both attempted; both already-exists swallowed


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
