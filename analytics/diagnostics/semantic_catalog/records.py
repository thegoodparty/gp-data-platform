"""Immutable record types for the semantic-layer catalog generator.

One MetricRecord per governed concept — a dbt metric or an exposure. The
generator's renderers consume these; nothing downstream re-reads YAML.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricRecord:
    name: str
    label: str
    definition: str
    metric_type: str
    source: str
    dimensions: tuple[str, ...]
    filter: str | None
    owner: str | None
    ratified: str | None
    detail_doc: str | None
    retired: str | None
    yaml_file: str
    kind: str  # "metric" | "exposure"
