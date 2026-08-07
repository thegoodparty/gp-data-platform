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
    # True when the sidecar's definition_sha no longer matches the definition
    # in the yml: the date certifies a definition that has since changed
    # (DATA-2249). Defaults False so a record built without a sign-off, in a
    # test or in the before-side of a diff, needs no extra argument.
    ratified_stale: bool = False


def ratified_cell(rec: MetricRecord) -> str:
    """The Ratified column, rendered identically for every projection.

    Both the markdown catalog and the ClickUp page call this, so a date can
    never read as approved in one surface and stale in the other.
    """
    text = rec.ratified or "pending"
    if rec.ratified and rec.ratified_stale:
        text = f"{text} (stale: definition changed since ratification)"
    if rec.retired:
        text = f"{text} (retired {rec.retired})"
    return text
