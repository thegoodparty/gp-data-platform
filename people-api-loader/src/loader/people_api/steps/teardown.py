"""Delete loader-created resources for a run_date (DATA-1912). Dry-run by default.

Safety model is name-scoping, not a tag guard: it walks only cfg-derived, date-stamped
names (gp-people-db-{date}*), which by construction can never match the serving cluster
(gp-people-db-prod) or shared infra. Describe-first and idempotent. Deletion order:
  writer instance -> cluster (deletion-protection disabled first) -> load/serve param
  groups -> connection-string SSM parameter -> (opt-in) the voter_export_{date}/ S3 prefix.

Durable infra is never deleted: the rds-s3-import role, the loader SG / DB subnet group,
the KMS key, and the S3 gateway VPC endpoint are platform-owned (DATA-1856). `--delete-vpce`
is a documented no-op — we reference a shared endpoint, never our own.
"""

from __future__ import annotations

from loader.core.aws import ignore_client_errors, rds, s3, ssm
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
            resp = client.delete_objects(Bucket=cfg.s3_bucket, Delete={"Objects": keys})
            if errors := resp.get("Errors", []):
                # delete_objects reports per-key failures here without raising.
                raise RuntimeError(f"S3 delete_objects partial failure in {cfg.s3_bucket}/{prefix}: {errors}")
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
    conn_param = cfg.new_conn_param(run_date)

    plan = [
        f"instance:{instance_id}",
        f"cluster:{cluster_id}",
        f"param-groups:{load_pg},{serve_pg}",
        f"ssm-param:{conn_param}",
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
    #    Also tolerate InvalidDBClusterStateFault: an orphan stuck in `creating` (the
    #    rollback-failed scenario provision documents) never reached resize, so protection is
    #    still False — there's nothing to disable, and this modify is safely skipped so teardown
    #    proceeds to the delete (the documented `loader teardown --confirm` recovery path).
    with ignore_client_errors("DBClusterNotFoundFault", "InvalidDBClusterStateFault"):
        rds_client.modify_db_cluster(
            DBClusterIdentifier=cluster_id, DeletionProtection=False, ApplyImmediately=True
        )
    with ignore_client_errors("DBClusterNotFoundFault"):
        rds_client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    rds_client.get_waiter("db_cluster_deleted").wait(DBClusterIdentifier=cluster_id)

    # 3. Parameter groups (only deletable once no cluster references them).
    #    DeleteDBClusterParameterGroup's modeled not-found code is DBParameterGroupNotFound
    #    (per the botocore RDS service model); we also tolerate the cluster-specific variant
    #    defensively, so a partial-teardown re-run stays idempotent regardless.
    for pg in (load_pg, serve_pg):
        with ignore_client_errors("DBParameterGroupNotFound", "DBClusterParameterGroupNotFound"):
            rds_client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=pg)

    # 4. Connection-string SSM parameter (per-run; holds the embedded master password).
    with ignore_client_errors("ParameterNotFound"):
        ssm(cfg).delete_parameter(Name=conn_param)

    # 5. Opt-in: the run's S3 artifacts (kept by default for forensics).
    if delete_s3:
        _delete_s3_prefix(cfg, run_date)

    log.info("teardown.complete", cluster=cluster_id)
