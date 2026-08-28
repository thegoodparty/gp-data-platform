"""Parse sem_*.yml files into MetricRecord objects.

Governance metadata is read from config.meta (never top-level config). Metrics
without config.meta parse cleanly as ungoverned/pending. Exposures carry their
definition in config.meta.definition and their source in `url`.

`ratified` is the one governance key that does NOT live here: it is authored in
the ratification sidecar (semantic_catalog.ratifications) so recording a
sign-off never re-requests the reviewers who gave it. A file-level parse
therefore yields a definition alone; parse_semantic_tree joins the sign-offs on
top. A `ratified` left behind in config.meta is a hard error, so the habit
cannot quietly come back, except when deliberately parsing pre-DATA-2249
history (see `legacy_ratified`).
"""

from __future__ import annotations

from pathlib import Path

import yaml

from semantic_catalog import ratifications
from semantic_catalog.records import MetricRecord


def _meta(block: dict, path: Path, name: str, legacy_ratified: bool) -> dict:
    meta = (block.get("config") or {}).get("meta") or {}
    if "ratified" in meta and not legacy_ratified:
        raise ValueError(
            f"{path}: {name} has config.meta.ratified. Ratification moved to "
            f"{ratifications.DEFAULT_PATH.name} (DATA-2249); record the sign-off there."
        )
    return meta


def _clean(text: str | None) -> str:
    # yaml folded/blocked scalars arrive with newlines; collapse to one line.
    return " ".join((text or "").split())


def _dimensions_for(models: list[dict]) -> tuple[str, ...]:
    dims: list[str] = []
    for model in models:
        for dim in model.get("dimensions") or []:
            dims.append(dim["name"])
    return tuple(dims)


def parse_semantic_file(path: Path, legacy_ratified: bool = False) -> list[MetricRecord]:
    """Parse one sem_*.yml.

    `legacy_ratified` is for parsing HISTORY, never the working tree. Commits
    before DATA-2249 carry the date in config.meta, and back then that WAS the
    ratification, so the before side of a diff should read it rather than reject
    it. Rejecting instead would crash the publish job on the very merge that
    introduces the sidecar.
    """
    doc = yaml.safe_load(path.read_text()) or {}
    models = doc.get("semantic_models") or []
    # A sem_*.yml holds one primary source; use the first model's `model:` ref
    # as the source for its metrics. Dimensions are the union across models.
    default_source = models[0].get("model", "") if models else ""
    dims = _dimensions_for(models)

    records: list[MetricRecord] = []

    for metric in doc.get("metrics") or []:
        meta = _meta(metric, path, metric["name"], legacy_ratified)
        records.append(
            MetricRecord(
                name=metric["name"],
                label=metric.get("label", metric["name"]),
                definition=_clean(metric.get("description")),
                metric_type=metric.get("type", "simple"),
                source=default_source,
                dimensions=dims,
                filter=_clean(metric["filter"]) if metric.get("filter") else None,
                owner=meta.get("owner"),
                ratified=str(meta["ratified"]) if meta.get("ratified") else None,
                detail_doc=meta.get("detail_doc"),
                retired=str(meta["retired"]) if meta.get("retired") else None,
                yaml_file=str(path),
                kind="metric",
            )
        )

    for exposure in doc.get("exposures") or []:
        meta = _meta(exposure, path, exposure["name"], legacy_ratified)
        records.append(
            MetricRecord(
                name=exposure["name"],
                label=exposure.get("label", exposure["name"]),
                definition=_clean(meta.get("definition") or exposure.get("description")),
                metric_type="exposure",
                source=exposure.get("url", ""),
                dimensions=(),
                filter=None,
                owner=meta.get("owner"),
                ratified=str(meta["ratified"]) if meta.get("ratified") else None,
                detail_doc=meta.get("detail_doc"),
                retired=str(meta["retired"]) if meta.get("retired") else None,
                yaml_file=str(path),
                kind="exposure",
            )
        )

    return records


def parse_semantic_tree(
    roots: list[Path],
    ratifications_path: Path | None = None,
    legacy_ratified: bool = False,
) -> list[MetricRecord]:
    """Parse every sem_*.yml under `roots` and join the sidecar's sign-offs.

    `ratifications_path` must be passed explicitly whenever `roots` points at a
    base worktree: the sidecar lives under analytics/, outside the dbt tree, so
    defaulting it there would read the CURRENT sign-offs onto the before side of
    a diff. Every ratification would then compare equal to itself, the pending
    to dated edge would vanish from the Slack summary, and no Sigma build task
    would ever fire (DATA-2199).

    Pass `legacy_ratified` alongside it, for the same reason: a base tree older
    than DATA-2249 keeps its dates in config.meta, where they are history to be
    read rather than an error to reject. A base tree that has both is resolved
    sidecar-first, since the sidecar is what that commit meant.
    """
    records: list[MetricRecord] = []
    for root in roots:
        for path in sorted(root.rglob("sem_*.yml")):
            records.extend(parse_semantic_file(path, legacy_ratified=legacy_ratified))
    records = sorted(records, key=lambda r: r.name)

    sign_offs = ratifications.load(ratifications_path)
    orphans = ratifications.orphaned_keys(records, sign_offs)
    if orphans:
        raise ValueError(
            f"{ratifications_path or ratifications.DEFAULT_PATH}: no metric named "
            f"{', '.join(orphans)}. Fix the key, or drop the entry if the metric is gone."
        )
    return ratifications.apply(records, sign_offs)
