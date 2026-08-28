"""On-failure cost guard (trigger_rule=one_failed): flip the run's writer to db.serverless so a
failed/aborted run does not strand a large provisioned instance. Unlike `resize`, it does NOT apply
the serve param group, backup retention, deletion protection, or reboot — it only stops the
provisioned-instance cost and KEEPS the cluster + loaded data for resume/forensics. Idempotent:
no-op if the writer is already serverless or the cluster/instance no longer exists.
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from loader.core.aws import flip_writer_to_serverless, rds
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig

log = get_logger(__name__)

_SERVERLESS_CLASS = "db.serverless"


def run(cfg: LoaderConfig, run_date: str) -> None:
    bind(run_date=run_date, step="scale_down")
    cluster_id = cfg.new_cluster_id(run_date)
    instance_id = cfg.new_writer_instance_id(run_date)
    rds_client = rds(cfg)

    # No writer (failure before provision, or already torn down) -> nothing to stop.
    try:
        current = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0][
            "DBInstanceClass"
        ]
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            log.info("scale_down.skip", reason="no writer instance", instance=instance_id)
            return
        raise
    if current == _SERVERLESS_CLASS:
        log.info("scale_down.already_serverless", instance=instance_id)
        return

    log.info("scale_down.start", instance=instance_id, from_class=current)
    flip_writer_to_serverless(
        rds_client, cluster_id, instance_id, min_acu=cfg.scale_down_min_acu, max_acu=cfg.scale_down_max_acu
    )
    log.info("scale_down.applied", instance=instance_id, instance_class=_SERVERLESS_CLASS)
