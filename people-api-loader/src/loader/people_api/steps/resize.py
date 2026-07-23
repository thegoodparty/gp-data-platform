"""Step 6 — resize the loaded cluster to its serving instance class + serve params, lock down.

After the load/index phase on the provisioned load instance, flip the writer to
`cfg.serve_instance_class` (a provisioned class, prod default db.r6g.4xlarge), swap the load
parameter group for the serve group (reboot applies it), bump backup retention to prod's 14
days, and enable deletion protection. The rds-s3-import role is intentionally left attached
(future incremental loads reuse it).

resize is now the pipeline's LAST step (validate runs before it — see steps/validate.py), so
after the reboot settles we run one trivial `SELECT 1` against the resized cluster and log a
confirmation. This is a connectivity smoke check only (is the freshly-resized writer actually
reachable and serving), not a data check — that's validate's job, run earlier while the writer
was still on the large index instance.

Idempotent: a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import flip_writer_to_provisioned, rds, retry_after_settle
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new
from loader.people_api.manifests import (
    ResizeManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

_BACKUP_RETENTION_DAYS = 14


def run(cfg: LoaderConfig, run_date: str) -> ResizeManifest:
    bind(run_date=run_date, step="resize")
    existing = read_manifest(cfg, run_date, "resize", ResizeManifest)
    if existing and existing.status == "complete":
        log.info("resize.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "resize"))
        return existing

    cluster_id = cfg.new_cluster_id(run_date)
    instance_id = cfg.new_writer_instance_id(run_date)
    serve_pg = cfg.new_serve_param_group(run_date)
    started = datetime.now(UTC)
    log.info("resize.start", cluster=cluster_id, instance=instance_id)

    rds_client = rds(cfg)
    instance_waiter = rds_client.get_waiter("db_instance_available")
    cluster_waiter = rds_client.get_waiter("db_cluster_available")

    def _wait() -> None:
        instance_waiter.wait(DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    def _wait_cluster() -> None:
        # The writer instance can read `available` while the cluster is still `modifying`,
        # so cluster-level modifies must be gated on the cluster waiter, not the instance one.
        cluster_waiter.wait(DBClusterIdentifier=cluster_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    # Serve param group + lock-down (backup retention, deletion protection), applied now. This is
    # resize's extra lockdown on top of the shared conversion below — retry_after_settle tolerates
    # only a still-in-progress modify from a partial re-run (InvalidDBClusterStateFault -> wait for
    # the cluster to settle -> re-issue so our settings are actually applied); a genuinely bad state
    # re-raises on the retry.
    retry_after_settle(
        lambda: rds_client.modify_db_cluster(
            DBClusterIdentifier=cluster_id,
            DBClusterParameterGroupName=serve_pg,
            BackupRetentionPeriod=_BACKUP_RETENTION_DAYS,
            DeletionProtection=True,
            ApplyImmediately=True,
        ),
        fault_code="InvalidDBClusterStateFault",
        settle=_wait_cluster,
    )
    # The lockdown modify above puts the cluster into 'modifying'; wait for it to settle before
    # issuing the writer's class-change modify_db_instance below. This keeps the class-change
    # modify from landing while the cluster is still applying the lockdown settings.
    _wait_cluster()
    # Shared conversion (writer instance class only — no cluster-level call for a provisioned
    # class, unlike the Serverless v2 flip scale_down still uses on failure).
    flip_writer_to_provisioned(rds_client, cluster_id, instance_id, instance_class=cfg.serve_instance_class)
    # A single db_instance_available waiter is NOT enough here: Aurora keeps reporting the
    # instance 'available' for a few seconds after a class-change modify before it flips to
    # 'modifying' and reboots, so a plain `_wait()` can return on the stale state — and the
    # explicit reboot below would then race the conversion's own delayed reboot
    # (InvalidDBInstanceStateFault). flip_writer_to_provisioned already rides through the
    # class-change reboot via wait_instance_class_applied; reboot here applies the serve
    # parameter group, then wait for available again.
    rds_client.reboot_db_instance(DBInstanceIdentifier=instance_id)
    _wait()
    log.info("resize.applied", instance_class=cfg.serve_instance_class)

    # Lightweight post-resize smoke check: the resized/rebooted writer actually accepts a
    # connection and serves a trivial query. NOT a data check (row counts/schema/indexes were
    # already validated pre-resize); this only guards against the resize itself leaving the
    # cluster unreachable (e.g. a security-group or param-group misstep, or a reboot that didn't
    # fully clear). A failure here raises and no "complete" manifest is written, same as any other
    # resize failure.
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    log.info("resize.smoke_check_ok")

    manifest = ResizeManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        final_instance_class=cfg.serve_instance_class,
        backup_retention_days=_BACKUP_RETENTION_DAYS,
        deletion_protection=True,
    )
    uri = write_manifest(cfg, manifest)
    log.info("resize.complete", uri=uri)
    return manifest
