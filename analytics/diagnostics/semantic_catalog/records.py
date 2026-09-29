"""Immutable record types for the semantic-layer catalog generator.

One MetricRecord per governed concept — a dbt metric or an exposure. The
generator's renderers consume these; nothing downstream re-reads YAML.

A metric definition is three layers with three owners, and the record carries
each separately so they can be sealed, diffed and routed apart:

  rule           what we mean, in words a non-engineer can rule on. Owned by
                 the business group. Sealed by `rule_sha`.
  implementation which raw events satisfy the rule (`anchored_on`). Owned by
                 product analytics. Sealed inside `build_sha`, because it is
                 also what the build compiles from.
  build          source table, measure, aggregation, type, filter. Owned by the
                 data group. Sealed by `build_sha`.

`definition` — the prose description — is in neither seal. It is documentation
of the three layers, not one of them, and sealing it meant a wording fix expired
an approval while an edit to which events count expired nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

# Sidecar seal schemes, carried on the record so a diff can tell a recomputation
# apart from a real change. "legacy" is the pre-two-seal single `definition_sha`,
# which only ever appears on the BEFORE side of a diff against old history.
SCHEME_TWO_SEAL = "two-seal"
SCHEME_LEGACY = "legacy"


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
    detail_doc: str | None
    retired: str | None
    yaml_file: str
    kind: str  # "metric" | "exposure"
    # Canonical one-line renderings of the two sealed layers. Text rather than
    # structure so a record stays hashable and comparable whole, which is what
    # the Slack diff relies on.
    business_rule: str | None = None
    anchored_on: str | None = None
    measure: str | None = None
    # The two halves of the sign-off, joined from the sidecar. Each carries its
    # own date and its own staleness, because a rule and a build go stale for
    # different reasons and on different days.
    rule_approved: str | None = None
    rule_stale: bool = False
    build_approved: str | None = None
    build_stale: bool = False
    # What the metric counted on the day the build was signed off. Proves
    # someone looked; never that the number is right.
    value_at_signing: int | None = None
    seal_scheme: str = SCHEME_TWO_SEAL
    # Why this metric's BUILD approval needs re-verifying, or None. Set from
    # evidence about the instrument rather than from the files: a seal compares
    # two documents and cannot see that the events it names stopped arriving.
    needs_reverification: str | None = None


def _half_cell(label: str, approved: str | None, stale: bool, reverify: bool = False) -> str:
    if not approved:
        return f"{label} pending"
    # Re-verification wins the cell over staleness. Both can be true at once, and
    # "the instrument feeding this stopped firing" is the more urgent sentence.
    if reverify:
        return f"{label} {approved} (NEEDS RE-VERIFICATION)"
    return f"{label} {approved}" + (" (stale)" if stale else "")


def ratified_cell(rec: MetricRecord) -> str:
    """The Ratified column, rendered identically for every projection.

    Both the markdown catalog and the ClickUp page call this, so a half can
    never read as approved in one surface and stale in the other.
    """
    if not rec.rule_approved and not rec.build_approved:
        text = "pending"
    else:
        text = " · ".join(
            [
                _half_cell("rule", rec.rule_approved, rec.rule_stale),
                _half_cell(
                    "build",
                    rec.build_approved,
                    rec.build_stale,
                    reverify=bool(rec.needs_reverification),
                ),
            ]
        )
    if rec.retired:
        text = f"{text} (retired {rec.retired})"
    return text


def by_basename(records: list[MetricRecord]) -> dict[str, MetricRecord]:
    """Records keyed by name, with `yaml_file` reduced to its basename.

    Every before/after comparison goes through this. The before-set is parsed
    from a base worktree, so the absolute path differs on EVERY record, and
    whole-record equality would otherwise report the entire catalog as changed.
    The basename still catches a real change: a metric moving between sem files.

    `needs_reverification` is dropped for a related reason. It is evidence about
    the instrument NOW, not part of the definition under review, and only the
    after-side is ever annotated with it — so leaving it in would report every
    metric with a dormant anchor as changed by a merge that did not touch it,
    with no diff line able to say why.
    """
    return {r.name: replace(r, yaml_file=Path(r.yaml_file).name, needs_reverification=None) for r in records}
