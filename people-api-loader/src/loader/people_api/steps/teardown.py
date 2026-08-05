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

from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from loader.core.aws import ignore_client_errors, rds, retry_after_settle, s3, ssm
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig

if TYPE_CHECKING:
    from botocore.client import BaseClient

log = get_logger(__name__)


def _describe_cluster(rds_client: BaseClient, cluster_id: str) -> dict[str, object] | None:
    """The cluster's describe payload, or None if it no longer exists.

    Lets teardown be genuinely describe-first: a re-run after a completed retire finds the cluster
    already gone and skips the compute-deletion block below, rather than issuing a modify_db_cluster
    that would come back AccessDenied. The rds:ModifyDBCluster grant is scoped by resource tags
    (Environment/managedBy), and a deleted cluster has no tags to satisfy that condition — so AWS
    denies the call rather than reporting it not-found, and the DBClusterNotFoundFault guard below
    would not catch it.
    """
    try:
        return rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBClusterNotFoundFault":
            return None
        raise


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
    snapshot: bool = False,
    keep_ssm: bool = False,
    keep_param_groups: bool = False,
) -> None:
    bind(run_date=run_date, step="teardown")
    cluster_id = cfg.new_cluster_id(run_date)
    instance_id = cfg.new_writer_instance_id(run_date)
    load_pg = cfg.new_load_param_group(run_date)
    serve_pg = cfg.new_serve_param_group(run_date)
    conn_param = cfg.new_conn_param(run_date)
    # Retire-with-restore-safety: take a final cluster snapshot and keep it (RDS retains it until
    # manually deleted). Snapshot id is deterministic per cluster; if one already exists from a
    # prior retire, that snapshot IS the restore point, so the delete below reuses it rather than
    # crashing or replacing it.
    snapshot_id = f"{cluster_id}-final"

    plan = [
        f"instance:{instance_id}",
        f"cluster:{cluster_id}" + (f" (final snapshot: {snapshot_id})" if snapshot else " (no snapshot)"),
        (
            f"KEEP param-groups:{load_pg},{serve_pg}"
            if keep_param_groups
            else f"param-groups:{load_pg},{serve_pg}"
        ),
        (f"KEEP ssm-param:{conn_param}" if keep_ssm else f"ssm-param:{conn_param}"),
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

    # Describe-first, so a re-run after a completed retire is a clean no-op rather than an
    # AccessDenied (see _describe_cluster). A gone cluster took its writer with it, so skip the whole
    # compute-deletion block and fall through to the still-idempotent param-group / SSM / S3 cleanup.
    cluster = _describe_cluster(rds_client, cluster_id)
    if cluster is None:
        log.info("teardown.cluster_absent", cluster=cluster_id, reason="already retired")
    else:
        # 1. Writer instance, then wait for it to be gone (cluster can't delete with instances).
        with ignore_client_errors("DBInstanceNotFound"):
            rds_client.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
        rds_client.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=instance_id)

        # 2. Cluster — disable deletion protection (resize enabled it) before deleting, but only when
        #    it is actually on. A failed build left on serverless never reached resize, and an orphan
        #    stuck in `creating` likewise; both are already unprotected, so skip the modify entirely
        #    rather than issue a needless tag-scoped call. Tolerate InvalidDBClusterStateFault for a
        #    protection flip racing a transitional state.
        if cluster.get("DeletionProtection"):
            with ignore_client_errors("DBClusterNotFoundFault", "InvalidDBClusterStateFault"):
                rds_client.modify_db_cluster(
                    DBClusterIdentifier=cluster_id, DeletionProtection=False, ApplyImmediately=True
                )
        # Delete. A cluster still in `creating` can't be deleted yet (InvalidDBClusterStateFault);
        # since this is the recovery tool for that orphan, wait for it to settle, then delete —
        # rather than aborting (operator must retry) or swallowing (the cluster is never deleted
        # and the deleted-waiter below would just time out). Mirrors resize's wait-then-retry.
        delete_cluster_kwargs: dict[str, object] = (
            {
                "DBClusterIdentifier": cluster_id,
                "SkipFinalSnapshot": False,
                "FinalDBSnapshotIdentifier": snapshot_id,
            }
            if snapshot
            else {"DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": True}
        )

        def _delete_cluster() -> None:
            try:
                rds_client.delete_db_cluster(**delete_cluster_kwargs)
            except ClientError as exc:
                # A prior retire already captured this cluster's final snapshot — that snapshot IS
                # the restore point, so reuse it and delete the cluster WITHOUT a duplicate. (A re-run
                # after the cluster is fully gone is handled by the describe-first guard above and
                # never reaches here, so a retry-after-success can't delete the restore point.)
                if snapshot and exc.response["Error"]["Code"] == "DBClusterSnapshotAlreadyExistsFault":
                    rds_client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
                else:
                    raise

        with ignore_client_errors("DBClusterNotFoundFault"):
            retry_after_settle(
                _delete_cluster,
                fault_code="InvalidDBClusterStateFault",
                settle=lambda: rds_client.get_waiter("db_cluster_available").wait(
                    DBClusterIdentifier=cluster_id, WaiterConfig={"Delay": 30, "MaxAttempts": 80}
                ),
            )
        rds_client.get_waiter("db_cluster_deleted").wait(DBClusterIdentifier=cluster_id)

    # 3. Parameter groups (only deletable once no cluster references them). Kept when
    #    keep_param_groups (retire-for-restore); they cost nothing and round out the restore set.
    #    DeleteDBClusterParameterGroup's modeled not-found code is DBParameterGroupNotFound
    #    (per the botocore RDS service model); we also tolerate the cluster-specific variant
    #    defensively, so a partial-teardown re-run stays idempotent regardless.
    if not keep_param_groups:
        for pg in (load_pg, serve_pg):
            with ignore_client_errors("DBParameterGroupNotFound", "DBClusterParameterGroupNotFound"):
                rds_client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=pg)

    # 4. Connection-string SSM parameter (per-run; holds the embedded master password). Kept when
    #    keep_ssm so a restore-from-snapshot still has its connection string.
    if not keep_ssm:
        with ignore_client_errors("ParameterNotFound"):
            ssm(cfg).delete_parameter(Name=conn_param)

    # 5. Opt-in: the run's S3 artifacts (kept by default for forensics).
    if delete_s3:
        _delete_s3_prefix(cfg, run_date)

    log.info("teardown.complete", cluster=cluster_id)
