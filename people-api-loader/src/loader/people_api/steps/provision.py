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

from loader.core.aws import ec2, get_ssm_parameter, ignore_client_errors, put_ssm_parameter, rds
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
    """Attach the rds-s3-import role, describe-first, skipping only an ACTIVE association.

    `add_role_to_db_cluster` requires `iam:PassRole`, and AWS evaluates that BEFORE the
    already-attached check — so a caller lacking PassRole gets `AccessDenied` (not
    `DBClusterRoleAlreadyExists`) when the role is already attached, which `ignore_client_errors`
    can't swallow. Describing first (a read, no PassRole) lets a re-run continue when an admin
    pre-attached the role, the path PLAN_LOADER.md documents for a loader without PassRole.

    Only an `ACTIVE` association is treated as done; a `PENDING`/`INVALID` role (e.g. from an
    interrupted prior run) still gets a (re)attach so s3 imports aren't silently left broken.
    The add is wrapped so a present-but-not-yet-ACTIVE role racing to ACTIVE doesn't hard-fail
    on `DBClusterRoleAlreadyExists`; `AccessDenied` (no PassRole) still surfaces.
    """
    resp = client.describe_db_clusters(DBClusterIdentifier=cluster_id)  # ty: ignore[unresolved-attribute]
    attached = resp["DBClusters"][0].get("AssociatedRoles", [])
    if any(r.get("RoleArn") == role_arn and r.get("Status") == "ACTIVE" for r in attached):
        log.info("provision.role_already_attached", cluster=cluster_id, role=role_arn)
        return
    with ignore_client_errors("DBClusterRoleAlreadyExists"):
        client.add_role_to_db_cluster(  # ty: ignore[unresolved-attribute]
            DBClusterIdentifier=cluster_id, RoleArn=role_arn, FeatureName="s3Import"
        )
        log.info("provision.role_attached", cluster=cluster_id, role=role_arn)
        return
    log.info("provision.role_attach_already_exists", cluster=cluster_id, role=role_arn)


def _find_s3_vpc_endpoint(client: object, region: str, vpc_id: str) -> str:
    """Return the S3 gateway VPC endpoint id in the VPC, or "" if absent.

    provision does not create it (it's durable platform infra); this verifies it exists
    and records its id. Absence is a warning, not a hard failure. Filtered to `available`
    so a deleting/failed/expired endpoint isn't recorded as a working one (which would give
    false confidence and surface only later as a copy/import failure with no diagnostic).
    """
    resp = client.describe_vpc_endpoints(  # ty: ignore[unresolved-attribute]
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "service-name", "Values": [f"com.amazonaws.{region}.s3"]},
            {"Name": "vpc-endpoint-state", "Values": ["available"]},
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

    # Fail fast (with the env-var names) if any provision-only infra value is unset, rather than
    # surfacing them one at a time as opaque AWS errors (e.g. "MasterUsername must not be blank").
    required = {
        "LOADER_DB_USER": cfg.db_user,
        "LOADER_DB_NAME": cfg.db_name,
        "LOADER_DB_SUBNET_GROUP": cfg.db_subnet_group,
        "LOADER_SECURITY_GROUP_ID": cfg.security_group_id,
        "LOADER_KMS_KEY_ARN": cfg.kms_key_arn,
        "LOADER_VPC_ID": cfg.vpc_id,
        "LOADER_S3_IMPORT_ROLE_ARN": cfg.s3_import_role_arn,
    }
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        raise RuntimeError(f"provision requires these env vars, which are unset/blank: {', '.join(missing)}")

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
            MasterUsername=cfg.db_user,
            MasterUserPassword=password,
            DatabaseName=cfg.db_name,
            StorageEncrypted=True,
            KmsKeyId=cfg.kms_key_arn,
            DBClusterParameterGroupName=load_pg,
            BackupRetentionPeriod=1,
            DeletionProtection=False,
            StorageType=_STORAGE_TYPE,
            Tags=tags,
        )
        log.info("provision.cluster_created", cluster=cluster_id)
        # Persist the connection string immediately — the cluster writer endpoint is assigned at
        # creation. The generated master password lives only in memory, so if this write fails
        # (IAM/throttle/crash) it is unrecoverable, and the reuse branch on a re-run would then
        # loop forever on ParameterNotFound. Roll the cluster back on any write failure so a
        # clean re-run regenerates. `sslmode=require` forces TLS: psycopg defaults to `prefer`
        # (plaintext fallback) and the cluster's default param group does not set rds.force_ssl.
        endpoint = created["DBCluster"]["Endpoint"]
        conninfo = (
            f"postgresql://{cfg.db_user}:{quote(password, safe='')}"
            f"@{endpoint}:{cfg.db_port}/{cfg.db_name}?sslmode=require"
        )
        try:
            put_ssm_parameter(cfg, conn_param, conninfo)
        except Exception:
            log.error("provision.conn_param_write_failed", cluster=cluster_id, name=conn_param)
            # Best-effort rollback. The delete frequently fails with InvalidDBClusterStateFault
            # because Aurora is still in 'creating' for minutes after create_db_cluster returns
            # — that's exactly the case the operator must know about (the param was never
            # written, so a re-run hits the reuse branch and loops on ParameterNotFound). Log
            # the rollback failure with a manual-intervention hint rather than swallow it, then
            # re-raise the original write error.
            try:
                rds_client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            except Exception as del_exc:
                log.error(
                    "provision.rollback_failed",
                    cluster=cluster_id,
                    hint="manually delete the cluster before re-running",
                    error=str(del_exc),
                )
            raise
        log.info("provision.conn_param_written", name=conn_param)
    else:
        # Reuse: the create response (and the generated password) aren't in hand, so read the
        # writer endpoint back. The create branch already has it from the create response.
        cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        endpoint = cluster_info["Endpoint"]
        log.info("provision.cluster_exists", cluster=cluster_id, status=cluster_info.get("Status"))
        # Fail fast on the orphaned-cluster case: cluster exists but the connection param was
        # never written (a prior run's SSM write and its rollback both failed). Otherwise the
        # missing param surfaces only after the multi-minute instance waiter below, as an
        # opaque ParameterNotFound from connect_new's SELECT 1.
        try:
            get_ssm_parameter(cfg, conn_param)
        except ClientError as ssm_exc:
            if ssm_exc.response["Error"]["Code"] == "ParameterNotFound":
                log.error(
                    "provision.conn_param_missing_on_reuse",
                    cluster=cluster_id,
                    name=conn_param,
                    hint="cluster exists but its connection param was never written; "
                    "manually delete the cluster before re-running",
                )
            raise

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

    # "Available" via the API, then prove the DB actually accepts connections.
    rds_client.get_waiter("db_instance_available").wait(
        DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 80}
    )

    # Attach the s3-import role only AFTER the cluster is available. add_role_to_db_cluster is
    # rejected while the cluster is still `creating` (InvalidDBClusterStateFault), so attaching
    # right after create failed attempt 1 on every fresh cluster and leaned on the DAG's retry to
    # recover (~5 min wasted, and broken outright if retries were ever 0). The instance-available
    # waiter implies the cluster is available.
    _attach_s3_import_role(rds_client, cluster_id, cfg.s3_import_role_arn)
    try:
        with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        # The cluster is up and the connection param is written; the DB is just unreachable
        # (loader SG / VPC routing). This is recoverable WITHOUT discarding anything: the reuse
        # branch re-runs this SELECT 1, so fixing connectivity and re-running self-heals (or
        # `loader teardown` discards the cluster). Surface it rather than leaving an opaque
        # psycopg error with no manifest. We deliberately do NOT delete the param — that would
        # force a destructive teardown for a fixable network issue and never regenerates.
        log.error(
            "provision.connect_failed",
            cluster=cluster_id,
            endpoint=endpoint,
            hint="DB unreachable after the waiter; check the loader security group + VPC "
            "routing, then re-run to retry (or `loader teardown` to discard the cluster)",
        )
        raise
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
