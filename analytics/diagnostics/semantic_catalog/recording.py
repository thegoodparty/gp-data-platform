"""Write earned sign-offs into the sidecar, and describe them for a PR body.

This is the WRITE path. It is deliberately not part of `ratifications`, which
`parser` imports on every parse including the blocking catalog-freshness gate;
that module should stay small and read-only. Only one CLI command reaches here.
"""

from __future__ import annotations

from semantic_catalog.ratifications import Ratification, upsert
from semantic_catalog.records import MetricRecord

# `upsert` writes this text as a comment line prefixed with
# `ratifications.AUTO_NOTE_PREFIX` ("auto-recorded: "), so the rendered line
# reads "auto-recorded: Sign-off landed on the merge of #800, ...". Phrased so
# that prefix isn't redundant with the template's own opening words.
NOTE_TEMPLATE = (
    "Sign-off landed on the merge of #{pr}, once both review groups approved. "
    "The date is when the second group's approval landed."
)


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


def manifest(
    earned: dict[str, Ratification],
    records: list[MetricRecord],
    date: str | None,
    pr_number: int,
) -> dict:
    """What the workflow reads to decide whether to open a PR, and what to say."""
    labels = {r.name: r.label for r in records}
    return {
        "pr": pr_number,
        "date": date,
        "metrics": [
            {"name": name, "label": labels.get(name, name), "definition_sha": earned[name].definition_sha}
            for name in sorted(earned)
        ],
    }


_BODY = """\
## What

Records the ratification for {count}, earned by [#{pr}](https://github.com/{repo}/pull/{pr}).

| Metric | Ratified | Definition fingerprint |
|---|---|---|
{rows}

## Why this is safe to approve on its own

Both review groups approved #{pr}, so the definition itself is already ratified.
The date recorded here, {date}, is when the second group's approval landed. All
you are agreeing to is that the date matches the approval record on that PR.

CODEOWNERS does not cover the ratification sidecar, so this PR re-requests no
review team. Recording a sign-off is bookkeeping, and it should not re-tag the
people who already gave it.

## How this was produced

Opened automatically by the semantic-layer publish job on the merge of #{pr}.
The fingerprint is computed from the merged definition, so if the definition
changes later without a new sign-off, every catalog projection will render this
date as stale.
"""


def pr_body(manifest: dict, repo: str) -> str:
    """Render the body of the PR that carries an earned ratification record."""
    if manifest["date"] is None:
        raise ValueError(
            "pr_body requires a date: manifest['date'] is None, which means no "
            "ratification was actually earned. Rendering a PR body would silently "
            "surface 'None' instead of failing loudly on a bad write."
        )
    metrics = manifest["metrics"]
    count = f"{len(metrics)} metric" + ("s" if len(metrics) != 1 else "")
    rows = "\n".join(f"| `{m['name']}` | {manifest['date']} | `{m['definition_sha']}` |" for m in metrics)
    return _BODY.format(count=count, pr=manifest["pr"], repo=repo, date=manifest["date"], rows=rows)
