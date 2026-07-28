"""Step 8 — promote the freshly-loaded cluster to serving (manual cutover).

Copies this run's connection string from the dated parameter that provision wrote
(`people-db-connection-string-{env}-{run_date}`) into the single serving parameter
(`people-db-connection-string-{env}`), which creates a new SSM version, and labels that version
`refresh-{run_date}`. people-api's DatabaseUrlProvider re-reads the serving parameter every
~5 minutes and hot-swaps its Prisma client, so the cutover takes effect without a restart.

Manual and gated: this step is NOT in the monthly DAG's automatic chain. An operator triggers
it (the `promote_people_api` DAG) after reviewing the run's validate + analyze. It refuses to
promote unless this run's validate manifest is complete and every check passed — serving must
never be pointed at a cluster the run did not fully validate.

ROLLBACK: each refresh's value stays in the serving parameter's version history under its own
`refresh-{date}` label. To roll back, read the previous label's value and re-put it as a new
version; people-api swaps back within its revalidate interval. This only works while the
PREVIOUS cluster still exists, so do NOT teardown the prior cluster until the new one is
confirmed healthy.

Idempotent: a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import get_ssm_parameter, label_ssm_parameter_version, put_ssm_parameter
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import (
    PromoteManifest,
    ValidateManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)


def run(cfg: LoaderConfig, run_date: str) -> PromoteManifest:
    bind(run_date=run_date, step="promote")
    existing = read_manifest(cfg, run_date, "promote", PromoteManifest)
    if existing and existing.status == "complete":
        log.info(
            "promote.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "promote")
        )
        return existing

    # Gate: never point serving at a cluster this run did not fully validate.
    validated = read_manifest(cfg, run_date, "validate", ValidateManifest)
    if validated is None or validated.status != "complete" or not validated.all_passed:
        raise RuntimeError(
            f"promote refused: validate for {run_date} is not complete with all checks passed "
            "(the serving cutover must only follow a green validate)"
        )

    dated_param = cfg.new_conn_param(run_date)
    serving_param = cfg.db_conn_param
    label = f"refresh-{run_date}"  # SSM labels cannot start with a digit — the prefix is required.

    started = datetime.now(UTC)
    log.info("promote.start", serving_param=serving_param, dated_param=dated_param, label=label)

    conn_str = get_ssm_parameter(cfg, dated_param)
    version = put_ssm_parameter(cfg, serving_param, conn_str)
    label_ssm_parameter_version(cfg, serving_param, version, [label])
    log.info("promote.done", serving_param=serving_param, version=version, label=label)

    manifest = PromoteManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        serving_param=serving_param,
        version=version,
        label=label,
    )
    uri = write_manifest(cfg, manifest)
    log.info("promote.complete", uri=uri, version=version, label=label)
    return manifest
