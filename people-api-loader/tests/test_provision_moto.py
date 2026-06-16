"""provision integration test against moto (fake AWS, in-process).

Unlike the Postgres integration test, moto needs no Docker/daemon, so this runs in CI
unconditionally. It exercises the real boto3 call sequence (param groups, cluster +
instance create, role attach, describe endpoint, find S3 VPCE) and idempotent re-run —
catching wrong boto3 argument shapes that the fake-client unit tests can't. The SELECT 1
(no real DB behind moto's RDS) and the manifest S3 IO are patched.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import provision as step
from tests._fakes import FakeConn, fake_connect

_REGION = "us-west-2"


@mock_aws
def test_provision_end_to_end_against_moto(monkeypatch: pytest.MonkeyPatch) -> None:
    # Durable infra moto must see for create_db_cluster to be accepted.
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

    # moto doesn't model the aurora-iopt1 storage type; use a moto-supported value so the
    # rest of the boto3 flow can be exercised (the real value is covered by the constant).
    monkeypatch.setattr(step, "_STORAGE_TYPE", "gp2")
    # moto's RDS has no real Postgres; patch the readiness SELECT 1 and the manifest S3 IO.
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "read_manifest", lambda *a: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    manifest = step.run(cfg, "20260616")

    assert manifest.cluster_id == "gp-people-db-20260616"
    assert manifest.vpc_endpoint_id  # the S3 gateway endpoint we created was found
    assert manifest.master_secret_id == "gp-people-db/20260616/master"
    # the cluster + writer instance really exist in moto
    assert rds.describe_db_clusters(DBClusterIdentifier="gp-people-db-20260616")["DBClusters"]
    assert rds.describe_db_instances(DBInstanceIdentifier="gp-people-db-20260616-writer")["DBInstances"]
    # the master password was stored
    boto3.client("secretsmanager", region_name=_REGION).get_secret_value(
        SecretId="gp-people-db/20260616/master"
    )
    # the s3Import role was attached with the correct FeatureName and ARN (catches a
    # FeatureName typo that moto would otherwise store without complaint)
    roles = rds.describe_db_clusters(DBClusterIdentifier="gp-people-db-20260616")["DBClusters"][0][
        "AssociatedRoles"
    ]
    assert any(r["RoleArn"] == cfg.s3_import_role_arn and r["FeatureName"] == "s3Import" for r in roles)

    # idempotent re-run reuses the existing cluster without error
    manifest2 = step.run(cfg, "20260616")
    assert manifest2.cluster_id == manifest.cluster_id
