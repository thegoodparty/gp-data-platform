"""Final step — promote the freshly-loaded cluster to serving (labeled-version cutover).

Copies this run's connection string from the dated parameter that provision wrote
(`people-db-connection-string-{env}-{run_date}`) into the single serving parameter
(`people-db-connection-string-{env}`), which creates a new SSM version. That version is then
tagged with two labels:

  - `build-{run_date}` — an immutable, per-refresh anchor (traceability + a named version to
    roll `live` back to). The `build-` prefix is required: SSM labels cannot start with a digit.
  - `live` — the moving pointer to the version people-api actually serves. Re-applying `live`
    moves it off whatever version previously held it, so this is the cutover.

people-api resolves `people-db-connection-string-{env}:live` and re-reads it every ~5 minutes,
hot-swapping its Prisma client, so moving `live` cuts over (and rolling it back) without a
service restart.

Automated + gated: this is the last task in `load_people_api`, after `resize` and `analyze`, so a
successful monthly refresh promotes itself. It refuses unless this run's `validate` manifest is
complete with every check passed — `live` must never point at a cluster the run did not fully
validate.

ROLLBACK: the prior refresh's version keeps its own `build-{prev_date}` label. To roll back, move
`live` to that label (a label move, no new version). This only works while the PREVIOUS cluster
still exists, so do NOT teardown the prior cluster until the new one is confirmed healthy.

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

# The moving pointer people-api reads (`people-db-connection-string-{env}:live`). Re-applying it to
# a new version moves it off the prior one — that move IS the cutover.
_LIVE_LABEL = "live"


def run(cfg: LoaderConfig, run_date: str) -> PromoteManifest:
    bind(run_date=run_date, step="promote")
    existing = read_manifest(cfg, run_date, "promote", PromoteManifest)
    if existing and existing.status == "complete":
        log.info(
            "promote.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "promote")
        )
        return existing

    # Gate: never point `live` at a cluster this run did not fully validate.
    validated = read_manifest(cfg, run_date, "validate", ValidateManifest)
    if validated is None or validated.status != "complete" or not validated.all_passed:
        raise RuntimeError(
            f"promote refused: validate for {run_date} is not complete with all checks passed "
            "(the serving cutover must only follow a green validate)"
        )

    dated_param = cfg.new_conn_param(run_date)
    serving_param = cfg.db_conn_param
    # Per-refresh anchor + the moving live pointer. `build-` prefix required (labels can't lead
    # with a digit). Applying both in one call moves `live` to this new version — the cutover.
    labels = [f"build-{run_date}", _LIVE_LABEL]

    started = datetime.now(UTC)
    log.info("promote.start", serving_param=serving_param, dated_param=dated_param, labels=labels)

    conn_str = get_ssm_parameter(cfg, dated_param)
    version = put_ssm_parameter(cfg, serving_param, conn_str)
    label_ssm_parameter_version(cfg, serving_param, version, labels)
    log.info("promote.done", serving_param=serving_param, version=version, labels=labels)

    manifest = PromoteManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        serving_param=serving_param,
        version=version,
        labels=labels,
    )
    uri = write_manifest(cfg, manifest)
    log.info("promote.complete", uri=uri, version=version, labels=labels)
    return manifest
