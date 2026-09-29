"""Parse sem_*.yml files into MetricRecord objects.

Governance metadata is read from config.meta (never top-level config). Metrics
without config.meta parse cleanly as ungoverned/pending. Exposures carry their
definition in config.meta.definition and their source in `url`.

Sign-offs are the one governance field that does NOT live here: they are
authored in the ratification sidecar (semantic_catalog.ratifications) so
recording an approval never re-requests the reviewers who gave it. A file-level
parse therefore yields a definition alone; parse_semantic_tree joins the
sign-offs on top. A `ratified` left behind in config.meta is a hard error, so
the habit cannot quietly come back, except when deliberately parsing
pre-sidecar history (see `legacy_ratified`).

A metric definition is three layers with three owners (see `records`). Two of
them are read here and sealed separately: `config.meta.business_rule` is the
rule, `config.meta.anchored_on` plus the build fields are the implementation and
the build. The prose `description` is neither — it documents them.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from semantic_catalog import layers, ratifications
from semantic_catalog.records import SCHEME_LEGACY, MetricRecord


def _legacy_halves(meta: dict, legacy_ratified: bool) -> dict:
    """A pre-sidecar `config.meta.ratified` date, mapped onto both halves.

    History only. Back then one date certified the whole definition, so it is
    read onto both halves and marked `legacy`, which is what lets a diff tell a
    seal-scheme recomputation apart from a real change. Both halves read stale
    because a legacy sha can never match either of the two that replaced it.
    """
    if not (legacy_ratified and meta.get("ratified")):
        return {}
    date = str(meta["ratified"])
    return {
        "rule_approved": date,
        "rule_stale": True,
        "build_approved": date,
        "build_stale": True,
        "seal_scheme": SCHEME_LEGACY,
    }


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


def _layers(meta: dict, path: Path, name: str) -> tuple[str | None, str | None]:
    """The rule and the implementation, canonically rendered and cross-checked.

    Dimensions are deliberately NOT read into either layer. They are declared
    once per file, so a naive seal over them would expire every metric in a file
    whenever one dimension was added anywhere. That hole is documented in the
    SOP alongside the other known one: a change upstream of the sem file moves
    no seal at all.
    """
    declared_anchors = meta.get("anchored_on")
    rule = layers.render_business_rule(meta.get("business_rule"), name)
    leaked = layers.rule_names_an_implementation(rule, declared_anchors)
    if leaked:
        raise ValueError(
            f"{path}: {name} business_rule names the event(s) {', '.join(leaked)}. "
            "The rule says what the number means; which events satisfy it belongs in "
            "anchored_on, and mixing them means the business group is being asked to "
            "rule on instrumentation."
        )
    return rule, layers.render_anchored_on(declared_anchors, name)


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
        business_rule, anchored_on = _layers(meta, path, metric["name"])
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
                detail_doc=meta.get("detail_doc"),
                retired=str(meta["retired"]) if meta.get("retired") else None,
                yaml_file=str(path),
                kind="metric",
                business_rule=business_rule,
                anchored_on=anchored_on,
                measure=(metric.get("type_params") or {}).get("measure"),
                **_legacy_halves(meta, legacy_ratified),
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
                detail_doc=meta.get("detail_doc"),
                retired=str(meta["retired"]) if meta.get("retired") else None,
                yaml_file=str(path),
                kind="exposure",
                **_legacy_halves(meta, legacy_ratified),
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
    a diff. Every ratification would then compare equal to itself and the
    pending to dated edge would vanish from the Slack summary (DATA-2199).

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
