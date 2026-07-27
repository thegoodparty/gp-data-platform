"""Render the thin canonical_metrics.md routing rows from metric records.

The file keeps its "resolve a concept here first" contract: Concept, one-line
definition, source, owns-detail link, ratified. Generated rows live inside the
begin/end markers so hand-authored (not-yet-encoded) rows can coexist during the
epic.
"""

from __future__ import annotations

from semantic_catalog.records import MetricRecord

BEGIN_MARK = "<!-- semantic-catalog:begin -->"
END_MARK = "<!-- semantic-catalog:end -->"

_HEADER = (
    "| Concept | Governed definition (one line) | Source | Owns detail | Ratified |\n" "|---|---|---|---|---|"
)


def _cell(text: str | None) -> str:
    # Escape pipes so a filter/definition never breaks the table.
    return (text or "").replace("|", "\\|")


def _row(rec: MetricRecord) -> str:
    detail = f"[{_cell(rec.detail_doc)}]({_cell(rec.detail_doc)})" if rec.detail_doc else ""
    ratified = rec.ratified or "pending"
    if rec.retired:
        ratified = f"{ratified} (retired {rec.retired})"
    concept = f"**{_cell(rec.label)}**"
    return f"| {concept} | {_cell(rec.definition)} | {_cell(rec.source)} " f"| {detail} | {_cell(ratified)} |"


def render_rows(records: list[MetricRecord]) -> str:
    lines = [_HEADER]
    lines.extend(_row(r) for r in records)
    return "\n".join(lines)


def render_region(records: list[MetricRecord]) -> str:
    return f"{BEGIN_MARK}\n{render_rows(records)}\n{END_MARK}"


def splice_region(existing: str, region: str) -> str:
    has_begin = BEGIN_MARK in existing
    has_end = END_MARK in existing
    if has_begin != has_end:
        raise ValueError("file has exactly one semantic-catalog marker; refusing to splice")
    if not has_begin:
        return existing.rstrip("\n") + "\n\n" + region + "\n"
    pre = existing.split(BEGIN_MARK, 1)[0]
    post = existing.split(END_MARK, 1)[1]
    return pre + region + post
