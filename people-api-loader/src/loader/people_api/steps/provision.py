"""Step 2 — provision a fresh Aurora PostgreSQL cluster for the load (DATA-1909).

Creates, per run_date, the date-stamped cluster + writer instance + the load/serve
cluster parameter groups, writes the connection string (generated master password
embedded) to an SSM SecureString, attaches the pre-existing rds-s3-import role (for
aws_s3 imports), polls to ready and runs SELECT 1, recording everything in a
ProvisionManifest.

Durable infra is NOT created here — the rds-s3-import IAM role, the S3 gateway VPC
endpoint, the loader-owned security group, and the DB subnet group are one-time
platform resources (DATA-1856, in gp-terraform-dataplatform). provision references them
(IDs come from LOADER_* env / config) and verifies the S3 VPC endpoint exists.

Parameter groups are created empty (defaults): the heavy load tuning is applied
per-session by copy/build_indexes (synchronous_commit=off, maintenance_work_mem, ...),
so nothing durability-hazardous is baked into the load group. The groups exist so resize
(step 6) can swap load -> serve on the cluster.

Idempotent: a completed manifest short-circuits; an existing same-named cluster is
reused (no re-create, no new password).
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from urllib.parse import quote

from botocore.exceptions import ClientError

from loader.core.aws import ec2, ignore_client_errors, put_ssm_parameter, rds
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new
from loader.people_api.manifests import (
    ProvisionManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

_ENGINE = "aurora-postgresql"
# IO-optimized storage for the one-shot bulk-load + heavy-index-build workload.
_STORAGE_TYPE = "aurora-iopt1"


def _cluster_exists(client: object, cluster_id: str) -> bool:
    try:
        client.describe_db_clusters(DBClusterIdentifier=cluster_id)  # ty: ignore[unresolved-attribute]
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBClusterNotFoundFault":
            return False
        raise
    return True


def _instance_exists(client: object, instance_id: str) -> bool:
    try:
        client.describe_db_instances(DBInstanceIdentifier=instance_id)  # ty: ignore[unresolved-attribute]
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            return False
        raise
    return True


def _ensure_cluster_param_group(client: object, name: str, family: str, tags: list[dict[str, str]]) -> None:
    with ignore_client_errors("DBParameterGroupAlreadyExists"):
        client.create_db_cluster_parameter_group(  # ty: ignore[unresolved-attribute]
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily=family,
            Description=f"people-api-loader {name}",
            Tags=tags,
        )
        log.info("provision.param_group_created", name=name, family=family)
        return
    log.info("provision.param_group_exists", name=name)


def _attach_s3_import_role(client: object, cluster_id: str, role_arn: str) -> None:
    with ignore_client_errors("DBClusterRoleAlreadyExists"):
        client.add_role_to_db_cluster(  # ty: ignore[unresolved-attribute]
            DBClusterIdentifier=cluster_id, RoleArn=role_arn, FeatureName="s3Import"
        )
        log.info("provision.role_attached", cluster=cluster_id, role=role_arn)
        return
    log.info("provision.role_already_attached", cluster=cluster_id, role=role_arn)


def _find_s3_vpc_endpoint(client: object, region: str, vpc_id: str) -> str:
    """Return the S3 gateway VPC endpoint id in the VPC, or "" if absent.

    provision does not create it (it's durable platform infra); this verifies it exists
    and records its id. Absence is a warning, not a hard failure.
    """
    resp = client.describe_vpc_endpoints(  # ty: ignore[unresolved-attribute]
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "service-name", "Values": [f"com.amazonaws.{region}.s3"]},
        ]
    )
    endpoints = resp.get("VpcEndpoints", [])
    return endpoints[0]["VpcEndpointId"] if endpoints else ""


def run(cfg: LoaderConfig, run_date: str) -> ProvisionManifest:
    bind(run_date=run_date, step="provision")
    existing = read_manifest(cfg, run_date, "provision", ProvisionManifest)
    if existing and existing.status == "complete":
        log.info(
            "provision.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "provision")
        )
        return existing

    cluster_id = cfg.new_cluster_id(run_date)
    instance_id = cfg.new_writer_instance_id(run_date)
    load_pg = cfg.new_load_param_group(run_date)
    serve_pg = cfg.new_serve_param_group(run_date)
    conn_param = cfg.new_conn_param(run_date)
    family = f"{_ENGINE}{cfg.engine_version.split('.')[0]}"  # e.g. 16.8 -> aurora-postgresql16

    started = datetime.now(UTC)
    log.info("provision.start", cluster=cluster_id, instance=instance_id)

    rds_client = rds(cfg)
    ec2_client = ec2(cfg)
    tags = cfg.tags_as_aws()

    # Param groups first (the cluster references the load group at create time).
    _ensure_cluster_param_group(rds_client, load_pg, family, tags)
    _ensure_cluster_param_group(rds_client, serve_pg, family, tags)

    # Cluster, then writer instance — each guarded independently so a partial prior run
    # (cluster created but instance not) self-heals on re-run. An existing cluster is reused
    # with its already-stored connection string (no regenerate): the generated password
    # exists only on the run that creates the cluster, so on reuse we skip straight past the
    # SSM write (the param was written on the first run).
    if not _cluster_exists(rds_client, cluster_id):
        password = secrets.token_urlsafe(32)
        created = rds_client.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine=_ENGINE,
            EngineVersion=cfg.engine_version,
            DBSubnetGroupName=cfg.db_subnet_group,
            VpcSecurityGroupIds=[cfg.security_group_id],
            MasterUsername=cfg.prod_db_user,
            MasterUserPassword=password,
            DatabaseName=cfg.prod_db_name,
            StorageEncrypted=True,
            KmsKeyId=cfg.kms_key_arn,
            DBClusterParameterGroupName=load_pg,
            BackupRetentionPeriod=1,
            DeletionProtection=False,
            StorageType=_STORAGE_TYPE,
            Tags=tags,
        )
        log.info("provision.cluster_created", cluster=cluster_id)
        # Persist the connection string immediately — the cluster writer endpoint is assigned
        # at creation. Writing it now (not after the availability wait) keeps the window where
        # the password could be lost to a crash near zero: a re-run then reuses the param.
        endpoint = created["DBCluster"]["Endpoint"]
        conninfo = (
            f"postgresql://{cfg.prod_db_user}:{quote(password, safe='')}"
            f"@{endpoint}:{cfg.prod_db_port}/{cfg.prod_db_name}"
        )
        put_ssm_parameter(cfg, conn_param, conninfo)
        log.info("provision.conn_param_written", name=conn_param)
    else:
        log.info("provision.cluster_exists", cluster=cluster_id)

    if not _instance_exists(rds_client, instance_id):
        rds_client.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBClusterIdentifier=cluster_id,
            Engine=_ENGINE,
            DBInstanceClass=cfg.load_instance_class,
            Tags=tags,
        )
        log.info("provision.instance_created", instance=instance_id)
    else:
        log.info("provision.instance_exists", instance=instance_id)

    _attach_s3_import_role(rds_client, cluster_id, cfg.s3_import_role_arn)

    # "Available" via the API, then prove the DB actually accepts connections.
    rds_client.get_waiter("db_instance_available").wait(
        DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 80}
    )
    endpoint = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["Endpoint"]
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    log.info("provision.ready", endpoint=endpoint)

    vpce_id = _find_s3_vpc_endpoint(ec2_client, cfg.aws_region, cfg.vpc_id)
    if not vpce_id:
        log.warning("provision.s3_vpce_missing", vpc=cfg.vpc_id)

    manifest = ProvisionManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        cluster_id=cluster_id,
        writer_instance_id=instance_id,
        writer_endpoint=endpoint,
        iam_role_arn=cfg.s3_import_role_arn,
        vpc_endpoint_id=vpce_id,
        load_parameter_group=load_pg,
        serve_parameter_group=serve_pg,
        conn_param=conn_param,
    )
    uri = write_manifest(cfg, manifest)
    log.info("provision.complete", uri=uri, cluster=cluster_id, endpoint=endpoint)
    return manifest
