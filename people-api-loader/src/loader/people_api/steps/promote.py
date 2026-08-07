"""Final step — promote the freshly-loaded cluster to serving (overwrite-latest cutover).

Copies this run's connection string from the dated parameter that provision wrote
(`people-db-connection-string-{env}-{run_date}`) into the single serving parameter
(`people-db-connection-string-{env}`), which creates a new SSM version. people-api reads the
serving parameter's LATEST version and re-reads it every ~5 minutes (hot-swapping its Prisma
client), so this overwrite IS the cutover — no service restart, no label pointer.

The new version is then tagged with a `build-{run_date}` label. This is best-effort bookkeeping
only: an operator-visible anchor mapping a version to the cluster it points at (the connection
string itself embeds the dated cluster name, so the label is a convenience, not the source of
truth). Labeling never fails or reverts the cutover — the overwrite above already did the real
work — so a label error (including SSM's per-version 10-label cap, reported via `InvalidLabels`)
is logged and swallowed. The `build-` prefix is required: SSM labels cannot start with a digit.

Automated + gated: this is the last task in `load_people_api`, after `resize` and `analyze`, so a
successful monthly refresh promotes itself. It refuses unless this run's `validate` manifest is
complete with every check passed — serving must never point at a cluster the run did not fully
validate.

ROLLBACK is a manual, operator (AWS admin group) action: write the desired prior connection string
back as a new version of the serving parameter (clickops). people-api swaps to it within its
revalidate interval. This only works while that prior cluster still exists, so do NOT teardown the
previous cluster until the new one is confirmed healthy. (The `build-{date}` labels help find the
right version to copy from.)

Idempotent: a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from botocore.exceptions import ClientError

from loader.core.aws import get_ssm_parameter, label_ssm_parameter_version, overwrite_ssm_parameter
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
    label = f"build-{run_date}"  # `build-` prefix required: SSM labels can't start with a digit.

    started = datetime.now(UTC)
    log.info("promote.start", serving_param=serving_param, dated_param=dated_param, label=label)

    conn_str = get_ssm_parameter(cfg, dated_param)
    # The cutover: a version-preserving overwrite (NOT put_ssm_parameter, which delete+recreates and
    # would wipe the serving param's prior versions). people-api reads the latest version.
    version = overwrite_ssm_parameter(cfg, serving_param, conn_str)

    # Best-effort bookkeeping label. The overwrite above is already the cutover, so a label failure
    # must not fail promote or undo it — swallow both a raised error and a rejected label
    # (InvalidLabels, e.g. SSM's per-version 10-label cap).
    applied: list[str] = []
    try:
        invalid = label_ssm_parameter_version(cfg, serving_param, version, [label])
        if invalid:
            log.warning("promote.label_rejected", version=version, label=label, invalid=invalid)
        else:
            applied = [label]
    except ClientError as exc:
        log.warning("promote.label_failed", version=version, label=label, error=str(exc))
    log.info("promote.done", serving_param=serving_param, version=version, labels=applied)

    manifest = PromoteManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        serving_param=serving_param,
        version=version,
        labels=applied,
    )
    uri = write_manifest(cfg, manifest)
    log.info("promote.complete", uri=uri, version=version, labels=applied)
    return manifest
