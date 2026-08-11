"""Ratification sign-offs, authored outside the CODEOWNERS-covered YAML.

`ratified` deliberately does NOT live in `sem_*.yml` (DATA-2249). CODEOWNERS
covers those files, so recording an approval there re-requests the very
reviewers whose approval is being recorded: you would have to write the date
before the thing it records exists. This sidecar sits outside that glob, so a
sign-off is recorded without re-tagging anyone, and the date can be the real one
rather than a guess made at authoring time.

The cost of splitting a definition from its sign-off is silent decoupling: the
definition is edited while the sidecar still asserts the old approval.
`definition_sha` closes that. It fingerprints the semantic content the reviewer
signed off on; the generator recomputes it on every run and renders the date as
stale on mismatch. This happens offline, with no API call and no token, so it is
safe inside the blocking catalog-freshness gate.

`approved_by_pr` is human provenance only. It is deliberately NOT carried on
MetricRecord: records are compared whole to build the Slack change diff, so a
provenance-only edit would report the metric as changed with no diff line able
to explain why.

`upsert`'s optional `note` is written as a comment line prefixed with
`AUTO_NOTE_PREFIX` ("auto-recorded: "), so the hook's own provenance note can
be told apart from a human's hand-written reasoning in the same block. On the
edit path (a metric whose sign-off went stale and is being re-earned) `upsert`
strips any prior line carrying that prefix before writing the fresh one, so
re-recording replaces the note instead of accumulating a second one; a human
comment lacking the prefix is left untouched. A later module, `recording.py`,
relies on this exact prefix constant to write its own auto-generated notes.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

from semantic_catalog.records import MetricRecord

DEFAULT_PATH = Path(__file__).parent / "config" / "ratifications.yml"

# The semantic content a reviewer actually signs off on. `dimensions` is
# excluded on purpose: it is a file-level union, so adding one dimension to a
# semantic model would falsely un-ratify every metric in that file. `label` is
# display text, and the governance fields are not the definition. The parser
# whitespace-collapses `definition`, so re-wrapping a YAML block scalar leaves
# the fingerprint unchanged.
FINGERPRINT_FIELDS = ("definition", "metric_type", "source", "filter")

# Short enough to read and retype, far past collision risk at this catalog size.
SHA_LEN = 7
_SHA_RE = re.compile(rf"^[0-9a-f]{{{SHA_LEN}}}$")


@dataclass(frozen=True)
class Ratification:
    ratified: str
    definition_sha: str
    approved_by_pr: int | None = None


def definition_sha(rec: MetricRecord) -> str:
    """Fingerprint of the definition fields, as of the record passed in."""
    payload = "\n".join(f"{field}={getattr(rec, field) or ''}" for field in FINGERPRINT_FIELDS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:SHA_LEN]


def _read_sha(path: Path, name: str, raw: object) -> str:
    if not isinstance(raw, str):
        # An unquoted hash that happens to be all digits (roughly one in
        # twenty-five) is read as an integer, and any leading zero is then gone
        # for good. Demand the quotes rather than silently comparing a mangled
        # value and reporting a healthy metric as stale.
        raise ValueError(
            f"{path}: {name} definition_sha must be quoted. YAML read {raw!r} as "
            f"{type(raw).__name__}, which would drop any leading zero."
        )
    if not _SHA_RE.match(raw):
        raise ValueError(
            f"{path}: {name} definition_sha must be {SHA_LEN} lowercase hex characters, got {raw!r}"
        )
    return raw


def load(path: Path | None = None) -> dict[str, Ratification]:
    """Read the sidecar. A missing file means nothing is ratified yet.

    Absence must stay legal: every diff parses a base worktree for its before
    side, and any base commit predating this sidecar simply has no file.
    """
    path = path or DEFAULT_PATH
    if not path.exists():
        return {}
    doc = yaml.safe_load(path.read_text()) or {}
    out: dict[str, Ratification] = {}
    for name, entry in doc.items():
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: {name} must be a mapping with ratified and definition_sha")
        missing = [key for key in ("ratified", "definition_sha") if entry.get(key) is None]
        if missing:
            # definition_sha is mandatory, not optional: an entry without one
            # is a date nothing can ever check, which is the state this whole
            # sidecar exists to eliminate.
            raise ValueError(f"{path}: {name} is missing {' and '.join(missing)}")
        out[name] = Ratification(
            ratified=str(entry["ratified"]),
            definition_sha=_read_sha(path, name, entry["definition_sha"]),
            approved_by_pr=entry.get("approved_by_pr"),
        )
    return out


def apply(records: list[MetricRecord], sign_offs: dict[str, Ratification]) -> list[MetricRecord]:
    """Attach each record's sign-off, flagging one whose definition has moved since."""
    out: list[MetricRecord] = []
    for rec in records:
        sign_off = sign_offs.get(rec.name)
        if sign_off is None:
            out.append(rec)
            continue
        out.append(
            replace(
                rec,
                ratified=sign_off.ratified,
                ratified_stale=definition_sha(rec) != sign_off.definition_sha,
            )
        )
    return out


def ratified_by_merge(
    before: list[MetricRecord],
    after: list[MetricRecord],
    date: str,
    pr_number: int,
) -> dict[str, Ratification]:
    """Sign-offs a merge earns, given that both review groups approved its PR.

    A metric qualifies only when it has no trustworthy sign-off already (pending
    or stale) AND its definition actually moved in this merge, judged by
    fingerprint against the base, or it is new. Without the second condition a
    PR editing one metric would ratify every other pending metric that happens
    to live in the same file, which reviewers never looked at.

    Deliberately conservative: a PR that changes only `owner` or `detail_doc` on
    a pending metric earns nothing, because no definition was put in front of
    anyone.
    """
    prev = {r.name: r for r in before}
    earned: dict[str, Ratification] = {}
    for rec in after:
        if rec.retired:
            continue
        if rec.ratified and not rec.ratified_stale:
            continue
        old = prev.get(rec.name)
        if old is not None and definition_sha(old) == definition_sha(rec):
            continue
        earned[rec.name] = Ratification(
            ratified=date,
            definition_sha=definition_sha(rec),
            approved_by_pr=pr_number,
        )
    return earned


_WRITTEN_FIELDS = ("ratified", "definition_sha", "approved_by_pr")

# Marks a comment line `upsert` wrote itself, as opposed to a human's reasoning
# typed into the same block. Greppable and stable on purpose: `recording.py`
# writes a note on every sign-off a merge earns, including a re-earned stale
# one that already has a block, and needs to replace its own prior note there
# without disturbing whatever a human wrote alongside it.
AUTO_NOTE_PREFIX = "auto-recorded: "
_AUTO_NOTE_RE = re.compile(rf"^\s{{2}}#\s*{re.escape(AUTO_NOTE_PREFIX)}")


def _field_line(key: str, sign_off: Ratification) -> str:
    if key == "ratified":
        return f"  ratified: {sign_off.ratified}\n"
    if key == "definition_sha":
        # Always quoted: an all-digit hash left bare reads back as an integer.
        return f"  definition_sha: '{sign_off.definition_sha}'\n"
    # `null`, not the bare word None: PyYAML has no notion of Python's None
    # literal, so `approved_by_pr: None` would read back as the STRING "None"
    # rather than as a missing PR number.
    pr = sign_off.approved_by_pr if sign_off.approved_by_pr is not None else "null"
    return f"  approved_by_pr: {pr}\n"


def upsert(text: str, name: str, sign_off: Ratification, note: str = "") -> str:
    """Write one entry into the sidecar's TEXT, leaving every other byte alone.

    Deliberately not a YAML round-trip. This file carries the reasoning behind
    each sign-off in comments, and dumping the parsed document would delete all
    of it. An existing entry is edited field by field, which matters because a
    metric whose sign-off went stale already has a block here; appending a
    second one would produce a duplicate key that YAML resolves silently to the
    last occurrence.
    """
    lines = text.splitlines(keepends=True)
    start = next((i for i, line in enumerate(lines) if re.match(rf"^{re.escape(name)}:\s*$", line)), None)

    if start is None:
        block = f"{name}:\n"
        if note:
            block += f"  # {AUTO_NOTE_PREFIX}{note}\n"
        block += "".join(_field_line(k, sign_off) for k in _WRITTEN_FIELDS)
        separator = "" if text.endswith("\n\n") or not text else "\n"
        return text + separator + block

    # The block runs to the next top-level key, ignoring comments and indented lines.
    end = next((j for j in range(start + 1, len(lines)) if re.match(r"^[^\s#]", lines[j])), len(lines))

    rewritten: list[str] = []
    seen: set[str] = set()
    for line in lines[start:end]:
        match = re.match(r"^\s{2}(\w+):", line)
        key = match.group(1) if match else None
        if key in _WRITTEN_FIELDS:
            if key in seen:
                continue
            seen.add(key)
            rewritten.append(_field_line(key, sign_off))
        elif _AUTO_NOTE_RE.match(line):
            # Drop a prior auto-note so re-recording replaces it rather than
            # accumulating a second one. A human's comment doesn't carry this
            # prefix, so it never matches here and is left in `rewritten` as-is.
            continue
        else:
            rewritten.append(line)

    if note:
        # rewritten[0] is always the block's own header line (`lines[start]`
        # copied verbatim, since the loop above runs over lines[start:end]),
        # so the note goes right after it -- inside the block, not above it.
        rewritten.insert(1, f"  # {AUTO_NOTE_PREFIX}{note}\n")

    missing = [k for k in _WRITTEN_FIELDS if k not in seen]
    if missing:
        # Insert after the last field written, so new keys land inside the block
        # rather than after any trailing blank line. A block carrying only
        # comments has no field line to anchor on, so fall back to index 0,
        # which is always this block's own header.
        field_lines = [i for i, line in enumerate(rewritten) if re.match(r"^\s{2}\w+:", line)]
        last = max(field_lines) if field_lines else 0
        rewritten[last + 1 : last + 1] = [_field_line(k, sign_off) for k in missing]

    return "".join(lines[:start] + rewritten + lines[end:])


def orphaned_keys(records: list[MetricRecord], sign_offs: dict[str, Ratification]) -> list[str]:
    """Sidecar keys matching no metric: a typo, or a metric renamed without its entry."""
    names = {rec.name for rec in records}
    return sorted(set(sign_offs) - names)
