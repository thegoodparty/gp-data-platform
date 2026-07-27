"""Parse sem_*.yml files into MetricRecord objects.

Governance metadata is read from config.meta (never top-level config). Metrics
without config.meta parse cleanly as ungoverned/pending. Exposures carry their
definition in config.meta.definition and their source in `url`.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from semantic_catalog.records import MetricRecord


def _meta(block: dict) -> dict:
    return (block.get("config") or {}).get("meta") or {}


def _clean(text: str | None) -> str:
    # yaml folded/blocked scalars arrive with newlines; collapse to one line.
    return " ".join((text or "").split())


def _dimensions_for(models: list[dict]) -> tuple[str, ...]:
    dims: list[str] = []
    for model in models:
        for dim in model.get("dimensions") or []:
            dims.append(dim["name"])
    return tuple(dims)


def parse_semantic_file(path: Path) -> list[MetricRecord]:
    doc = yaml.safe_load(path.read_text()) or {}
    models = doc.get("semantic_models") or []
    # A sem_*.yml holds one primary source; use the first model's `model:` ref
    # as the source for its metrics. Dimensions are the union across models.
    default_source = models[0].get("model", "") if models else ""
    dims = _dimensions_for(models)

    records: list[MetricRecord] = []

    for metric in doc.get("metrics") or []:
        meta = _meta(metric)
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
        meta = _meta(exposure)
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


def parse_semantic_tree(roots: list[Path]) -> list[MetricRecord]:
    records: list[MetricRecord] = []
    for root in roots:
        for path in sorted(root.rglob("sem_*.yml")):
            records.extend(parse_semantic_file(path))
    return sorted(records, key=lambda r: r.name)
