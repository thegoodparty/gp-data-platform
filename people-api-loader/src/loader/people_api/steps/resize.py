"""Step 6 — resize the loaded cluster to Serverless v2 + serve params, lock down (DATA-1854).

After the load/index phase on the provisioned load instance, flip the writer to
db.serverless with the prod-matching ACU range, swap the load parameter group for the
serve group (reboot applies it), bump backup retention to prod's 14 days, and enable
deletion protection. The rds-s3-import role is intentionally left attached (future
incremental loads reuse it).

Idempotent: a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import flip_writer_to_serverless, rds, retry_after_settle
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import (
    ResizeManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

_SERVERLESS_CLASS = "db.serverless"
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
    # issuing the shared conversion's own modify_db_cluster (ServerlessV2ScalingConfiguration).
    # Without this, the conversion's modify hits InvalidDBClusterStateFault on essentially every
    # normal run — retry_after_settle inside it self-heals that, but that path exists for genuine
    # partial-rerun collisions, not to absorb an expected fault on every happy-path run.
    _wait_cluster()
    # Shared conversion (cluster ServerlessV2ScalingConfiguration + writer instance class), same
    # sequence scale_down uses.
    flip_writer_to_serverless(
        rds_client, cluster_id, instance_id, min_acu=cfg.serve_min_acu, max_acu=cfg.serve_max_acu
    )
    # A single db_instance_available waiter is NOT enough here: Aurora keeps reporting the
    # instance 'available' for a few seconds after a class-change modify before it flips to
    # 'modifying' and reboots, so a plain `_wait()` can return on the stale state — and the
    # explicit reboot below would then race the conversion's own delayed reboot
    # (InvalidDBInstanceStateFault). flip_writer_to_serverless already rides through the
    # class-change reboot via wait_instance_class_applied; reboot here applies the serve
    # parameter group, then wait for available again.
    rds_client.reboot_db_instance(DBInstanceIdentifier=instance_id)
    _wait()
    log.info(
        "resize.applied",
        instance_class=_SERVERLESS_CLASS,
        min_acu=cfg.serve_min_acu,
        max_acu=cfg.serve_max_acu,
    )

    manifest = ResizeManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        final_instance_class=_SERVERLESS_CLASS,
        min_acu=cfg.serve_min_acu,
        max_acu=cfg.serve_max_acu,
        backup_retention_days=_BACKUP_RETENTION_DAYS,
        deletion_protection=True,
    )
    uri = write_manifest(cfg, manifest)
    log.info("resize.complete", uri=uri)
    return manifest
