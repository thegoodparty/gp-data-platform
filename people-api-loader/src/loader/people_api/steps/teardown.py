"""Delete loader-created resources for a run_date (DATA-1912). Dry-run by default.

Describe-first and idempotent: walks only cfg-derived (date-stamped) resource names, so
it can't touch the serving cluster or shared infra. Deletion order:
  writer instance -> cluster (deletion-protection disabled first) -> load/serve param
  groups -> master secret -> (opt-in) the voter_export_{date}/ S3 prefix.

Durable infra is never deleted: the rds-s3-import role, the loader SG / DB subnet group,
the KMS key, and the S3 gateway VPC endpoint are platform-owned (DATA-1856). `--delete-vpce`
is a documented no-op — we reference a shared endpoint, never our own.
"""

from __future__ import annotations

from loader.core.aws import ignore_client_errors, rds, s3, secrets
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig

log = get_logger(__name__)


def _delete_s3_prefix(cfg: LoaderConfig, run_date: str) -> None:
    prefix = f"{cfg.export_prefix(run_date)}/"
    client = s3(cfg)
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=cfg.s3_bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=cfg.s3_bucket, Delete={"Objects": keys})
            deleted += len(keys)
    log.info("teardown.s3_deleted", prefix=prefix, objects=deleted)


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    confirm: bool = False,
    delete_s3: bool = False,
    delete_vpce: bool = False,
) -> None:
    bind(run_date=run_date, step="teardown")
    cluster_id = cfg.new_cluster_id(run_date)
    instance_id = cfg.new_writer_instance_id(run_date)
    load_pg = cfg.new_load_param_group(run_date)
    serve_pg = cfg.new_serve_param_group(run_date)
    secret_id = cfg.new_master_secret_id(run_date)

    plan = [
        f"instance:{instance_id}",
        f"cluster:{cluster_id}",
        f"param-groups:{load_pg},{serve_pg}",
        f"secret:{secret_id}",
    ]
    if delete_s3:
        plan.append(f"s3:{cfg.export_prefix(run_date)}/")
    # --delete-vpce is always a no-op: the S3 VPC endpoint is shared/durable platform infra.
    if delete_vpce:
        log.warning("teardown.vpce_noop", reason="S3 VPC endpoint is shared/durable; not loader-owned")

    if not confirm:
        log.info("teardown.dry_run", would_delete=plan)
        return

    rds_client = rds(cfg)

    # 1. Writer instance, then wait for it to be gone (cluster can't delete with instances).
    with ignore_client_errors("DBInstanceNotFound"):
        rds_client.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
    rds_client.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=instance_id)

    # 2. Cluster — disable deletion protection (resize enabled it) before deleting.
    with ignore_client_errors("DBClusterNotFoundFault"):
        rds_client.modify_db_cluster(
            DBClusterIdentifier=cluster_id, DeletionProtection=False, ApplyImmediately=True
        )
    with ignore_client_errors("DBClusterNotFoundFault"):
        rds_client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    rds_client.get_waiter("db_cluster_deleted").wait(DBClusterIdentifier=cluster_id)

    # 3. Parameter groups (only deletable once no cluster references them).
    for pg in (load_pg, serve_pg):
        with ignore_client_errors("DBParameterGroupNotFound"):
            rds_client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=pg)

    # 4. Master password secret (per-run; recoverable for a week).
    with ignore_client_errors("ResourceNotFoundException"):
        secrets(cfg).delete_secret(SecretId=secret_id, RecoveryWindowInDays=7)

    # 5. Opt-in: the run's S3 artifacts (kept by default for forensics).
    if delete_s3:
        _delete_s3_prefix(cfg, run_date)

    log.info("teardown.complete", cluster=cluster_id)
