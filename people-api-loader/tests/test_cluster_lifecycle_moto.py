"""Cluster-lifecycle integration tests against moto (fake AWS, in-process).

Unlike the Postgres integration test, moto needs no Docker/daemon, so these run in CI
unconditionally. They exercise the real boto3 call sequences (and idempotent re-runs),
catching wrong boto3 argument shapes the fake-client unit tests can't — e.g. they already
caught a wrong RDS error code and a FeatureName-validation gap. The SELECT 1 (no real DB
behind moto's RDS) and manifest S3 IO are patched.

moto limitations worked around: it doesn't model the aurora-iopt1 storage type, so
provision's _STORAGE_TYPE is overridden to a moto-supported value (the real value lives in
the constant), and it doesn't model the provisioned->serverless instance-state transition,
so resize's reboot-ordering is covered by static review + the unit test, not here.
"""

from __future__ import annotations

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import provision as provision_step
from loader.people_api.steps import resize as resize_step
from loader.people_api.steps import teardown as teardown_step
from tests._fakes import FakeConn, fake_connect

_REGION = "us-west-2"
_DATE = "20260616"


def _provision(monkeypatch: pytest.MonkeyPatch) -> LoaderConfig:
    """Stand up durable infra in moto, run provision, return the resolved cfg.

    Shared by all three lifecycle tests so resize/teardown run against a real
    moto-provisioned cluster rather than hand-built fixtures. Tests make their own
    boto3 clients (the @mock_aws state is shared) to assert against.
    """
    ec2 = boto3.client("ec2", region_name=_REGION)
    vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")["Subnet"]["SubnetId"]
    sg_id = ec2.create_security_group(GroupName="loader-sg", Description="loader", VpcId=vpc_id)["GroupId"]
    ec2.create_vpc_endpoint(
        VpcId=vpc_id, ServiceName=f"com.amazonaws.{_REGION}.s3", VpcEndpointType="Gateway"
    )
    kms_arn = boto3.client("kms", region_name=_REGION).create_key()["KeyMetadata"]["Arn"]
    rds = boto3.client("rds", region_name=_REGION)
    rds.create_db_subnet_group(
        DBSubnetGroupName="loader-subnets", DBSubnetGroupDescription="loader", SubnetIds=[subnet_id]
    )

    for k, v in {
        "AWS_REGION": _REGION,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "LOADER_VPC_ID": vpc_id,
        "LOADER_DB_SUBNET_GROUP": "loader-subnets",
        "LOADER_SECURITY_GROUP_ID": sg_id,
        "LOADER_PROD_DB_USER": "people_admin",
        "LOADER_PROD_DB_NAME": "people_prod",
        "LOADER_KMS_KEY_ARN": kms_arn,
        "LOADER_S3_IMPORT_ROLE_ARN": "arn:aws:iam::123456789012:role/rds-s3-import",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    cfg = LoaderConfig.from_env()

    monkeypatch.setattr(provision_step, "_STORAGE_TYPE", "gp2")  # moto doesn't model aurora-iopt1
    monkeypatch.setattr(provision_step, "connect_new", fake_connect(FakeConn()))  # no real DB behind moto
    monkeypatch.setattr(provision_step, "read_manifest", lambda *a: None)
    monkeypatch.setattr(provision_step, "write_manifest", lambda cfg, m: "uri")
    provision_step.run(cfg, _DATE)
    return cfg


@mock_aws
def test_provision_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _provision(monkeypatch)
    rds = boto3.client("rds", region_name=_REGION)
    cluster_id = cfg.new_cluster_id(_DATE)

    assert rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"]
    assert rds.describe_db_instances(DBInstanceIdentifier=cfg.new_writer_instance_id(_DATE))["DBInstances"]
    # the connection string was written to SSM as a postgresql:// URL (password embedded)
    ssm = boto3.client("ssm", region_name=_REGION)
    conninfo = ssm.get_parameter(Name=cfg.new_conn_param(_DATE), WithDecryption=True)["Parameter"]["Value"]
    assert conninfo.startswith("postgresql://people_admin:")
    # tagged with Environment so the loader's IAM permissions boundary allows Get/Describe
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=cfg.new_conn_param(_DATE))[
        "TagList"
    ]
    assert {"Key": "Environment", "Value": "dev"} in tags
    # the s3Import role attached with the right FeatureName + ARN (moto stores any
    # FeatureName, so this catches a typo that would only fail in real AWS)
    roles = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["AssociatedRoles"]
    assert any(r["RoleArn"] == cfg.s3_import_role_arn and r["FeatureName"] == "s3Import" for r in roles)

    provision_step.run(cfg, _DATE)  # idempotent re-run: reuse, no error


@mock_aws
def test_resize_applies_lockdown(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _provision(monkeypatch)
    rds = boto3.client("rds", region_name=_REGION)
    monkeypatch.setattr(resize_step, "read_manifest", lambda *a: None)
    monkeypatch.setattr(resize_step, "write_manifest", lambda cfg, m: "uri")

    manifest = resize_step.run(cfg, _DATE)

    assert manifest.final_instance_class == "db.serverless"
    cluster = rds.describe_db_clusters(DBClusterIdentifier=cfg.new_cluster_id(_DATE))["DBClusters"][0]
    assert cluster["BackupRetentionPeriod"] == 14
    assert cluster["DeletionProtection"] is True


@mock_aws
def test_teardown_deletes_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _provision(monkeypatch)
    rds = boto3.client("rds", region_name=_REGION)
    cluster_id = cfg.new_cluster_id(_DATE)

    teardown_step.run(cfg, _DATE, confirm=True)

    with pytest.raises(ClientError):
        rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    with pytest.raises(ClientError):
        rds.describe_db_instances(DBInstanceIdentifier=cfg.new_writer_instance_id(_DATE))
    with pytest.raises(ClientError):
        boto3.client("ssm", region_name=_REGION).get_parameter(Name=cfg.new_conn_param(_DATE))


@mock_aws
def test_put_ssm_parameter_overwrites_and_retags(monkeypatch: pytest.MonkeyPatch) -> None:
    # provision skips put_ssm_parameter on a cluster-reuse run, so the ParameterAlreadyExists
    # overwrite+retag branch (aws.py) isn't covered by the end-to-end test. Exercise it directly:
    # a second write to the same name must hit that branch, overwrite the value, and keep the tag.
    from loader.core.aws import put_ssm_parameter

    for k, v in {
        "AWS_REGION": _REGION,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    cfg = LoaderConfig.from_env()
    name = cfg.new_conn_param(_DATE)

    put_ssm_parameter(cfg, name, "postgresql://u:p@h:5432/d?sslmode=require")
    put_ssm_parameter(cfg, name, "postgresql://u:p2@h:5432/d?sslmode=require")  # ParameterAlreadyExists path

    ssm = boto3.client("ssm", region_name=_REGION)
    assert ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"].endswith(
        "p2@h:5432/d?sslmode=require"
    )
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)["TagList"]
    assert {"Key": "Environment", "Value": "dev"} in tags
