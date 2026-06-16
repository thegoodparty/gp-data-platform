"""Step 6 — resize the loaded cluster to Serverless v2 + serve params, lock down (DATA-1854).

After the load/index phase on a provisioned db.r7g instance, flip the writer to
db.serverless with the prod-matching ACU range, swap the load parameter group for the
serve group (reboot applies it), bump backup retention to prod's 14 days, and enable
deletion protection. The rds-s3-import role is intentionally left attached (future
incremental loads reuse it).

Idempotent: a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import rds
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
    # Serve param group + Serverless v2 scaling (cluster-level) + lock-down, applied now.
    rds_client.modify_db_cluster(
        DBClusterIdentifier=cluster_id,
        DBClusterParameterGroupName=serve_pg,
        ServerlessV2ScalingConfiguration={"MinCapacity": cfg.serve_min_acu, "MaxCapacity": cfg.serve_max_acu},
        BackupRetentionPeriod=_BACKUP_RETENTION_DAYS,
        DeletionProtection=True,
        ApplyImmediately=True,
    )
    # Flip the writer instance to serverless (instance-level class).
    rds_client.modify_db_instance(
        DBInstanceIdentifier=instance_id, DBInstanceClass=_SERVERLESS_CLASS, ApplyImmediately=True
    )
    # Reboot so the serve parameter group takes effect, then wait for available.
    rds_client.reboot_db_instance(DBInstanceIdentifier=instance_id)
    rds_client.get_waiter("db_instance_available").wait(
        DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40}
    )
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
