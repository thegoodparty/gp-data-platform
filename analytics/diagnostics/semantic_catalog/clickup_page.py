"""Render the three-part ClickUp reference page.

Part 1 (static): the SOP walkthrough, passed in from the repo template.
Part 2 (static): decision-makers. Team slugs from owners.yml; human names only
if an untracked `people` map is supplied at publish time (repo rule: no names in
tracked files).
Part 3 (dynamic): the per-metric catalog, generated from records + git lifecycle,
inside CATALOG_BEGIN/CATALOG_END so any out-of-band page context is preserved.
Part 4 (static, optional): a footer rendered after the catalog (governance
diagram + reader explainer), passed in from the repo template.

This is the page where the company reads whether a number can be trusted, so it
is where instrument evidence lands: a metric whose declared events have gone
dormant renders its BUILD approval as needing re-verification, with the reason
underneath. That check is a cross-repo network read, so it applies here and not
in the offline catalog-freshness gate — and when it cannot run, the page says so
rather than quietly reading green.
"""

from __future__ import annotations

from semantic_catalog.lifecycle import Lifecycle
from semantic_catalog.records import MetricRecord, ratified_cell

CATALOG_BEGIN = "<!-- catalog:begin -->"
CATALOG_END = "<!-- catalog:end -->"


def _cell(text: str | None) -> str:
    # Escape pipes so a filter/definition never breaks the table.
    return (text or "").replace("|", "\\|")


def _decision_makers(owners: dict, people: dict | None) -> str:
    lines = ["## Decision-makers", ""]
    for key, team in owners.get("teams", {}).items():
        slug = team["slug"]
        role = team.get("role", "")
        if people and people.get(slug):
            who = ", ".join(people[slug])
            lines.append(f"- **{key.title()} group** ({slug}): {who}. {role}")
        else:
            lines.append(f"- **{key.title()} group** ({slug}). {role}")
    return "\n".join(lines)


def _lifecycle_cell(lc: Lifecycle | None) -> str:
    if lc is None:
        return " | "
    created = f"{lc.created or ''}" + (f" (#{lc.created_pr})" if lc.created_pr else "")
    updated = f"{lc.last_updated or ''}" + (f" (#{lc.last_updated_pr})" if lc.last_updated_pr else "")
    return f"{created} | {updated}"


def _evidence_notes(records: list[MetricRecord], problems: list[str]) -> str:
    """Why any metric needs re-verification, and any reason the check did not run."""
    flagged = [r for r in records if r.needs_reverification]
    if not flagged and not problems:
        return ""
    lines = ["### Instrument evidence", ""]
    for rec in flagged:
        lines.append(
            f"- **{_cell(rec.label)}** — build approval needs re-verification: {rec.needs_reverification}"
        )
    for problem in problems:
        # Never silent. A check that disables itself without saying so is the
        # failure this evidence link exists to remove, rebuilt one layer up.
        lines.append(f"- :warning: {problem}")
    return "\n".join(lines)


def _catalog(records: list[MetricRecord], lifecycles: dict[str, Lifecycle]) -> str:
    header = (
        "| Metric | Definition | Type | Source | Owner | Ratified | Created | Last updated | Detail |\n"
        "|---|---|---|---|---|---|---|---|---|"
    )
    rows = [header]
    for rec in records:
        lc = lifecycles.get(rec.yaml_file)
        detail = f"[{_cell(rec.detail_doc)}]({_cell(rec.detail_doc)})" if rec.detail_doc else ""
        rows.append(
            f"| {_cell(rec.label)} | {_cell(rec.definition)} | {rec.metric_type} | {_cell(rec.source)} "
            f"| {_cell(rec.owner or '')} | {_cell(ratified_cell(rec))} | {_lifecycle_cell(lc)} | {detail} |"
        )
    return f"{CATALOG_BEGIN}\n" + "\n".join(rows) + f"\n{CATALOG_END}"


def render_page(
    records: list[MetricRecord],
    lifecycles: dict[str, Lifecycle],
    sop_md: str,
    owners: dict,
    people: dict | None = None,
    footer_md: str = "",
    evidence_problems: list[str] | None = None,
) -> str:
    parts = [
        "# Semantic layer reference",
        "",
        sop_md,
        "",
        _decision_makers(owners, people),
        "",
        "## Metric catalog",
        "",
        _catalog(records, lifecycles),
    ]
    notes = _evidence_notes(records, evidence_problems or [])
    if notes:
        parts += ["", notes]
    if footer_md:
        parts += ["", footer_md]
    return "\n".join(parts) + "\n"
