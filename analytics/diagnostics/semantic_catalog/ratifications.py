"""Ratification sign-offs, authored outside the CODEOWNERS-covered YAML.

Sign-offs deliberately do NOT live in `sem_*.yml` (DATA-2249). Routing covers
those files, so recording an approval there re-requests the very reviewers whose
approval is being recorded: you would have to write the date before the thing it
records exists. This sidecar sits outside that scope, so a sign-off is recorded
without re-tagging anyone, and the date can be the real one rather than a guess
made at authoring time.

The cost of splitting a definition from its sign-off is silent decoupling: the
definition is edited while the sidecar still asserts the old approval. A seal
closes that. It fingerprints the content the reviewer signed off on; the
generator recomputes it on every run and renders the date as stale on mismatch.
This happens offline, with no API call and no token, so it is safe inside the
blocking catalog-freshness gate.

TWO seals, not one (DATA-2422). The single `definition_sha` was wrong in both
directions at once: it covered the prose paragraph, so rewording expired an
approval nobody had changed, and it did not cover the list of raw events, so
changing which users a metric counts expired nothing.

  rule_sha   over `business_rule` alone. The business group rules on what we
             mean, and only a change to that should ask them again.
  build_sha  over `anchored_on`, `filter`, `measure`, `metric_type` and
             `source`. The data group and product analytics own how the number
             is computed and which events satisfy the rule.

The data half additionally carries `value_at_signing` and it is REQUIRED, the
same way a seal is: you cannot record that a build was signed off without saying
what it counted the day you signed it. It is enforced here, in the loader behind
the blocking freshness gate, and never as a PR check — CI cannot compute a
metric's value, so a PR check could only prove that some number is present. The
number is a snapshot and takes the date of the entry it sits in. It proves
someone looked, never that the number is right.

`approved_by_pr` is human provenance only. It is deliberately NOT carried on
MetricRecord: records are compared whole to build the Slack change diff, so a
provenance-only edit would report the metric as changed with no diff line able
to explain why.

`upsert`'s optional `note` is written as a comment line prefixed with
`AUTO_NOTE_PREFIX` ("auto-recorded: "), so the hook's own provenance note can
be told apart from a human's hand-written reasoning in the same block. On the
edit path (a half whose sign-off went stale and is being re-earned) `upsert`
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

from semantic_catalog.records import SCHEME_LEGACY, MetricRecord

DEFAULT_PATH = Path(__file__).parent / "config" / "ratifications.yml"

# What the business group signs: the rule, and nothing else. A wording fix to
# the prose `description` moves nothing here, which is the whole point.
RULE_FIELDS = ("business_rule",)

# What the data group signs. `anchored_on` is in here rather than under the rule
# because which events satisfy a rule is implementation, owned by product
# analytics, and it is also what the build compiles from. `dimensions` stays out
# on purpose: it is a file-level union, so adding one dimension to a semantic
# model would falsely un-ratify every metric in that file. `label` is display
# text, and the governance fields are not the definition.
BUILD_FIELDS = ("anchored_on", "filter", "measure", "metric_type", "source")

# Short enough to read and retype, far past collision risk at this catalog size.
SHA_LEN = 7
_SHA_RE = re.compile(rf"^[0-9a-f]{{{SHA_LEN}}}$")


@dataclass(frozen=True)
class SignOff:
    """One half of a sign-off. `value` is set on the data half only: a business
    approval signs the rule and carries no number."""

    approved: str
    sha: str
    value: int | None = None


@dataclass(frozen=True)
class Ratification:
    """Both halves of a metric's sign-off. Either may be absent (pending)."""

    rule: SignOff | None = None
    data: SignOff | None = None
    approved_by_pr: int | None = None
    scheme: str | None = None


def _sha_over(rec: MetricRecord, fields: tuple[str, ...]) -> str:
    payload = "\n".join(f"{field}={getattr(rec, field) or ''}" for field in fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:SHA_LEN]


def rule_sha(rec: MetricRecord) -> str:
    """Fingerprint of what the metric means, as of the record passed in."""
    return _sha_over(rec, RULE_FIELDS)


def build_sha(rec: MetricRecord) -> str:
    """Fingerprint of how the metric is computed, as of the record passed in."""
    return _sha_over(rec, BUILD_FIELDS)


def _read_sha(path: Path, name: str, key: str, raw: object) -> str:
    if not isinstance(raw, str):
        # An unquoted hash that happens to be all digits (roughly one in
        # twenty-five) is read as an integer, and any leading zero is then gone
        # for good. Demand the quotes rather than silently comparing a mangled
        # value and reporting a healthy metric as stale.
        raise ValueError(
            f"{path}: {name} {key} must be quoted. YAML read {raw!r} as "
            f"{type(raw).__name__}, which would drop any leading zero."
        )
    if not _SHA_RE.match(raw):
        raise ValueError(f"{path}: {name} {key} must be {SHA_LEN} lowercase hex characters, got {raw!r}")
    return raw


def _read_half(path: Path, name: str, half: str, entry: object, sha_key: str) -> SignOff | None:
    """One half of an entry. Absent is legal and means that half is pending."""
    if entry is None:
        return None
    if not isinstance(entry, dict):
        raise ValueError(f"{path}: {name}.{half} must be a mapping with approved and {sha_key}")
    missing = [key for key in ("approved", sha_key) if entry.get(key) is None]
    if missing:
        # A seal is mandatory, not optional: a date nothing can ever check is
        # the state this whole sidecar exists to eliminate.
        raise ValueError(f"{path}: {name}.{half} is missing {' and '.join(missing)}")
    value = entry.get("value_at_signing")
    if half == "data":
        if value is None:
            raise ValueError(
                f"{path}: {name}.data is missing value_at_signing. A build sign-off must "
                "say what the metric counted on the day it was signed; a date with no "
                "number cannot be checked by anyone later."
            )
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{path}: {name}.data.value_at_signing must be a whole number, got {value!r}")
    elif value is not None:
        raise ValueError(
            f"{path}: {name}.business carries value_at_signing. A business approval signs "
            "the rule and has no value attached; the number belongs on the data half."
        )
    return SignOff(
        approved=str(entry["approved"]),
        sha=_read_sha(path, name, f"{half}.{sha_key}", entry[sha_key]),
        value=value if half == "data" else None,
    )


def _read_legacy(path: Path, name: str, entry: dict) -> Ratification:
    """A pre-two-seal entry: one date, one `definition_sha` over both layers.

    Only the BEFORE side of a diff against old history reaches this. It is read
    onto both halves so the dates stay truthful, and marked `legacy` so a diff
    can say "re-sealed" rather than reporting five metrics as newly stale. The
    legacy sha can never match either replacement, so both halves read stale —
    which is accurate: nothing under the new scheme has been signed yet.
    """
    sha = _read_sha(path, name, "definition_sha", entry["definition_sha"])
    date = str(entry["ratified"])
    return Ratification(
        rule=SignOff(approved=date, sha=sha),
        # No value was ever recorded under the old scheme, and inventing one
        # here would be a number nobody looked at. The requirement binds new
        # entries; history is read as it was written.
        data=SignOff(approved=date, sha=sha, value=None),
        approved_by_pr=entry.get("approved_by_pr"),
        scheme=SCHEME_LEGACY,
    )


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
            raise ValueError(f"{path}: {name} must be a mapping with a business or data half")
        if "definition_sha" in entry or "ratified" in entry:
            if "definition_sha" not in entry or "ratified" not in entry:
                raise ValueError(
                    f"{path}: {name} mixes the single-seal shape with the two-seal one. "
                    "Write it as business/data halves, or as ratified + definition_sha, "
                    "never half of each."
                )
            out[name] = _read_legacy(path, name, entry)
            continue
        unknown = sorted(set(entry) - {"business", "data", "approved_by_pr"})
        if unknown:
            raise ValueError(
                f"{path}: {name} has unknown key(s) {', '.join(unknown)}. An entry is a "
                "'business' half, a 'data' half, and approved_by_pr."
            )
        rule = _read_half(path, name, "business", entry.get("business"), "rule_sha")
        data = _read_half(path, name, "data", entry.get("data"), "build_sha")
        if rule is None and data is None:
            raise ValueError(
                f"{path}: {name} has neither half. Absence of the whole entry is how a "
                "metric reads pending; an empty entry says nothing."
            )
        out[name] = Ratification(rule=rule, data=data, approved_by_pr=entry.get("approved_by_pr"))
    return out


def apply(records: list[MetricRecord], sign_offs: dict[str, Ratification]) -> list[MetricRecord]:
    """Attach each record's sign-offs, flagging a half whose content has moved since."""
    out: list[MetricRecord] = []
    for rec in records:
        sign_off = sign_offs.get(rec.name)
        if sign_off is None:
            out.append(rec)
            continue
        fields: dict = {}
        if sign_off.rule is not None:
            fields["rule_approved"] = sign_off.rule.approved
            fields["rule_stale"] = rule_sha(rec) != sign_off.rule.sha
        if sign_off.data is not None:
            fields["build_approved"] = sign_off.data.approved
            fields["build_stale"] = build_sha(rec) != sign_off.data.sha
            fields["value_at_signing"] = sign_off.data.value
        if sign_off.scheme:
            fields["seal_scheme"] = sign_off.scheme
        out.append(replace(rec, **fields))
    return out


def earned_by_merge(
    before: list[MetricRecord],
    after: list[MetricRecord],
    group_dates: dict[str, str | None],
    pr_number: int,
    values: dict[str, int] | None = None,
) -> dict[str, Ratification]:
    """Sign-offs a merge earns, half by half.

    Each half is earned on its own terms, which is what lane routing requires: a
    data-only change approved by the data group records a build half and does
    not wait for a business approval that was never asked for. Under the single
    seal this needed BOTH groups, so a correctly routed change would have sat
    pending forever.

    A half qualifies only when it has no trustworthy sign-off already (pending
    or stale) AND its own seal actually moved in this merge, or the metric is
    new. Without the second condition a PR editing one metric would ratify every
    other pending metric that happens to live in the same file, which reviewers
    never looked at.

    `values` carries the counts the PR body declared. A build half with no
    declared value is NOT recorded: the loader requires one, so writing the
    entry anyway would produce a sidecar that fails to load and take the
    blocking freshness gate down with it.
    """
    prev = {r.name: r for r in before}
    values = values or {}
    business_date, data_date = group_dates.get("business"), group_dates.get("data")
    earned: dict[str, Ratification] = {}
    for rec in after:
        if rec.retired:
            continue
        old = prev.get(rec.name)
        rule = data = None
        if (
            business_date
            # A metric with no `business_rule` has nothing for that group to
            # rule on, and every such metric seals identically over emptiness.
            # Recording one would assert an approval of nothing, and would do it
            # for every ruleless metric at once.
            and rec.business_rule
            and not (rec.rule_approved and not rec.rule_stale)
            and (old is None or rule_sha(old) != rule_sha(rec))
        ):
            rule = SignOff(approved=business_date, sha=rule_sha(rec))
        if (
            data_date
            and not (rec.build_approved and not rec.build_stale)
            and (old is None or build_sha(old) != build_sha(rec))
            and rec.name in values
        ):
            data = SignOff(approved=data_date, sha=build_sha(rec), value=values[rec.name])
        if rule or data:
            earned[rec.name] = Ratification(rule=rule, data=data, approved_by_pr=pr_number)
    return earned


# Marks a comment line `upsert` wrote itself, as opposed to a human's reasoning
# typed into the same block. Greppable and stable on purpose: `recording.py`
# writes a note on every sign-off a merge earns, including a re-earned stale
# one that already has a block, and needs to replace its own prior note there
# without disturbing whatever a human wrote alongside it.
AUTO_NOTE_PREFIX = "auto-recorded: "
_AUTO_NOTE_RE = re.compile(rf"^\s{{2}}#\s*{re.escape(AUTO_NOTE_PREFIX)}")


def render_entry(name: str, sign_off: Ratification, note: str = "") -> str:
    """One whole sidecar block, halves and all."""
    lines = [f"{name}:\n"]
    if note:
        lines.append(f"  # {AUTO_NOTE_PREFIX}{note}\n")
    if sign_off.rule is not None:
        lines += [
            "  business:\n",
            f"    approved: {sign_off.rule.approved}\n",
            # Always quoted: an all-digit hash left bare reads back as an integer.
            f"    rule_sha: '{sign_off.rule.sha}'\n",
        ]
    if sign_off.data is not None:
        lines += [
            "  data:\n",
            f"    approved: {sign_off.data.approved}\n",
            f"    build_sha: '{sign_off.data.sha}'\n",
            f"    value_at_signing: {sign_off.data.value}\n",
        ]
    # `null`, not the bare word None: PyYAML has no notion of Python's None
    # literal, so `approved_by_pr: None` would read back as the STRING "None"
    # rather than as a missing PR number.
    pr = sign_off.approved_by_pr if sign_off.approved_by_pr is not None else "null"
    lines.append(f"  approved_by_pr: {pr}\n")
    return "".join(lines)


def _merge_halves(existing: Ratification, earned: Ratification) -> Ratification:
    """An earned half replaces its counterpart; an unearned one is left alone.

    A data-only merge must not silently restate or drop the business half that
    someone gave on a different PR months earlier.
    """
    return Ratification(
        rule=earned.rule or existing.rule,
        data=earned.data or existing.data,
        approved_by_pr=earned.approved_by_pr or existing.approved_by_pr,
    )


def upsert(text: str, name: str, sign_off: Ratification, note: str = "") -> str:
    """Write one entry into the sidecar's TEXT, leaving every other byte alone.

    Deliberately not a YAML round-trip. This file carries the reasoning behind
    each sign-off in comments, and dumping the parsed document would delete all
    of it. An existing entry is replaced block for block rather than appended
    to, which matters because appending a second block would produce a duplicate
    key that YAML resolves silently to the last occurrence. The block's
    human-written comments are carried across; only the auto-note is replaced.
    """
    lines = text.splitlines(keepends=True)
    start = next((i for i, line in enumerate(lines) if re.match(rf"^{re.escape(name)}:\s*$", line)), None)

    if start is None:
        block = render_entry(name, sign_off, note)
        separator = "" if text.endswith("\n\n") or not text else "\n"
        return text + separator + block

    # The block runs to the next top-level key, ignoring comments and indented lines.
    end = next((j for j in range(start + 1, len(lines)) if re.match(r"^[^\s#]", lines[j])), len(lines))

    existing = load_entry_text("".join(lines[start:end]), name)
    merged = _merge_halves(existing, sign_off) if existing else sign_off

    # Human reasoning is kept; the auto-note is rewritten rather than stacked.
    # Comments keep their side of the fields, because block-end detection
    # sweeps up trailing comments that belong to whatever comes next — moving
    # them above the fields would silently reorder someone else's note.
    body = lines[start + 1 : end]
    fields = [i for i, line in enumerate(body) if re.match(r"^\s{2,}\S", line)]
    split = fields[-1] + 1 if fields else 0
    before_fields = [
        line for line in body[:split] if line.lstrip().startswith("#") and not _AUTO_NOTE_RE.match(line)
    ]
    # Everything after the last field is copied verbatim, blank lines included:
    # it is whatever followed this entry, not part of it.
    after_fields = [line for line in body[split:] if not _AUTO_NOTE_RE.match(line)]
    rendered = render_entry(name, merged, note).splitlines(keepends=True)
    rewritten = [rendered[0]] + before_fields + rendered[1:] + after_fields
    return "".join(lines[:start] + rewritten + lines[end:])


def load_entry_text(block: str, name: str) -> Ratification | None:
    """Parse one already-isolated block.

    Returns None when the block holds no sign-off to preserve — it is absent, or
    it is a header with only comments. RAISES when a block is there but does not
    parse, because the two cases must not be confused: `upsert` writes only the
    half a merge earned, so treating an unreadable block as absent would replace
    it with a structurally valid one-half entry and destroy whichever half WAS
    readable alongside the corrupt one. `load` would then accept the result
    without complaint and the evidence would be gone. Failing the publish job is
    the cheaper outcome; the sidecar is small and a human fixes it by hand.
    """
    try:
        doc = yaml.safe_load(block) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{name}: existing sidecar block is not valid YAML ({exc})") from exc
    entry = doc.get(name)
    if entry is None:
        return None
    if not isinstance(entry, dict):
        raise ValueError(f"{name}: existing sidecar block is not a mapping")
    rule = _read_half(Path(name), name, "business", entry.get("business"), "rule_sha")
    data = _read_half(Path(name), name, "data", entry.get("data"), "build_sha")
    if rule is None and data is None:
        return None
    return Ratification(rule=rule, data=data, approved_by_pr=entry.get("approved_by_pr"))


def orphaned_keys(records: list[MetricRecord], sign_offs: dict[str, Ratification]) -> list[str]:
    """Sidecar keys matching no metric: a typo, or a metric renamed without its entry."""
    names = {rec.name for rec in records}
    return sorted(set(sign_offs) - names)
