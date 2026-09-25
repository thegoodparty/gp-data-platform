"""Write earned sign-offs into the sidecar, and describe them for a PR body.

This is the WRITE path. It is deliberately not part of `ratifications`, which
`parser` imports on every parse including the blocking catalog-freshness gate;
that module should stay small and read-only. Only one CLI command reaches here.

It is also where the value a build sign-off records comes from. CI cannot
compute a metric's value — that needs a live warehouse query — so the number is
declared by the author in the PR body and parsed back out here. That keeps the
requirement honest: a human states what the change does to the number, in the
place reviewers read, and the record keeps what they stated.
"""

from __future__ import annotations

import re

from semantic_catalog.ratifications import Ratification, upsert
from semantic_catalog.records import MetricRecord

# `upsert` writes this text as a comment line prefixed with
# `ratifications.AUTO_NOTE_PREFIX` ("auto-recorded: "), so the rendered line
# reads "auto-recorded: Sign-off landed on the merge of #800, ...". Phrased so
# that prefix isn't redundant with the template's own opening words.
NOTE_TEMPLATE = (
    "Sign-off landed on the merge of #{pr}, from the group whose lane the change was in. "
    "Each date is when that group's first approval landed."
)

# How a PR declares what a metric counts after the change. An HTML comment so it
# renders as nothing, and one line per metric so a multi-metric PR cannot state
# one number and have it silently apply to all of them.
VALUE_MARKER_RE = re.compile(r"<!--\s*semantic-value:\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(-?[\d,]+)\s*-->")
VALUE_MARKER_EXAMPLE = "<!-- semantic-value: win_activated_users = 1260 -->"


def declared_values(pr_body: str) -> dict[str, int]:
    """Metric values the PR body declares, as `{name: count}`.

    Last one wins if a name is declared twice: an edited PR body keeps its
    history above the correction often enough that first-wins would record the
    superseded number.
    """
    return {name: int(digits.replace(",", "")) for name, digits in VALUE_MARKER_RE.findall(pr_body or "")}


def unvalued(earned_build: list[str], values: dict[str, int]) -> list[str]:
    """Metrics whose build half was otherwise earned but declared no value."""
    return sorted(name for name in earned_build if name not in values)


def apply(sidecar_text: str, earned: dict[str, Ratification], pr_number: int) -> str:
    """Write every earned entry into the sidecar text.

    Sorted by metric name so a re-run of the same merge produces byte-identical
    output, which is what lets the workflow force-push the same branch without
    churning the diff.
    """
    note = NOTE_TEMPLATE.format(pr=pr_number)
    for name in sorted(earned):
        sidecar_text = upsert(sidecar_text, name, earned[name], note=note)
    return sidecar_text


def manifest(earned: dict[str, Ratification], records: list[MetricRecord], pr_number: int) -> dict:
    """What the workflow reads to decide whether to open a PR, and what to say.

    There is no single `date` any more: the two halves are approved by different
    people on different days, so each earned half carries its own.
    """
    labels = {r.name: r.label for r in records}
    metrics = []
    for name in sorted(earned):
        rule, data = earned[name].rule, earned[name].data
        metrics.append(
            {
                "name": name,
                "label": labels.get(name, name),
                "rule": {"approved": rule.approved, "sha": rule.sha} if rule else None,
                "data": (
                    {"approved": data.approved, "sha": data.sha, "value_at_signing": data.value}
                    if data
                    else None
                ),
            }
        )
    return {"pr": pr_number, "metrics": metrics}


_BODY = """\
## What

Records the sign-offs for {count}, earned by [#{pr}](https://github.com/{repo}/pull/{pr}).

| Metric | Half | Approved | Seal | Value at signing |
|---|---|---|---|---|
{rows}

## Why this is safe to approve on its own

Each half recorded here was approved on #{pr} by the group that owns it: the
business group signs what the number means, the data group signs how it is
computed. The date is when that group's first approval landed. All you are
agreeing to is that each row matches the review record on that PR.

Routing does not cover the ratification sidecar, so this PR re-requests no
review team. Recording a sign-off is bookkeeping, and it should not re-tag the
people who already gave it.

## How this was produced

Opened automatically by the semantic-layer publish job on the merge of #{pr}.
Each seal is computed from the merged definition, so if the rule or the build
changes later without a new sign-off, every catalog projection renders that half
as stale. The value at signing is the number the author declared in #{pr}'s
body; it proves someone looked, not that the number is right.
"""


def pr_body(manifest: dict, repo: str) -> str:
    """Render the body of the PR that carries earned sign-off records."""
    metrics = manifest["metrics"]
    if not metrics:
        raise ValueError(
            "pr_body requires at least one earned sign-off: rendering a body for an "
            "empty manifest would open a PR that records nothing."
        )
    count = f"{len(metrics)} metric" + ("s" if len(metrics) != 1 else "")
    rows = []
    for m in metrics:
        if m["rule"]:
            rows.append(f"| `{m['name']}` | rule | {m['rule']['approved']} | `{m['rule']['sha']}` | n/a |")
        if m["data"]:
            rows.append(
                f"| `{m['name']}` | build | {m['data']['approved']} | `{m['data']['sha']}` "
                f"| {m['data']['value_at_signing']} |"
            )
    return _BODY.format(count=count, pr=manifest["pr"], repo=repo, rows="\n".join(rows))
