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


def orphaned_keys(records: list[MetricRecord], sign_offs: dict[str, Ratification]) -> list[str]:
    """Sidecar keys matching no metric: a typo, or a metric renamed without its entry."""
    names = {rec.name for rec in records}
    return sorted(set(sign_offs) - names)
