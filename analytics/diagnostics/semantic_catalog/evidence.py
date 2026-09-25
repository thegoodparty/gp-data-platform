"""Read omni's latched dormant anchors, so a seal cannot outlive its instrument.

A seal compares a file against a record. Both are documents. Nothing compared the
declared events against whether they are still arriving, which is how the Active
Candidates OKR was signed off on 2026-08-05 against an instrument that had been
broken since 2026-07-31, read approved for five weeks, and under-counted 377
against the 941 the repair produced.

omni already knows this. Its health monitor latches a declared leg that goes
dormant and ranks it top of the weekly digest. But that lands in a Slack channel
and never reaches the catalog page where the company reads whether a number can
be trusted. This module carries it across: a latched anchor marks that metric's
BUILD approval as needing re-verification, and the page says why.

It clears the way the monitor's own latch clears — the event recovers, or
`anchored_on` adopts the successor. There is no dismissal here, deliberately:
this is evidence, not an opinion, and the way to silence it is to fix the
instrument or change what the metric is anchored on.

Cross-repo read, the mirror of omni's own read of this repo's sem files. It is a
NETWORK read, so it deliberately does not run inside `parse_semantic_tree`: the
blocking catalog-freshness gate must stay offline and deterministic. Only the
surfaces that already reach the network — the ClickUp page and the merge summary
— apply it.

Degrading is loud, never silent. A read that fails returns the reason alongside,
because a guard that disables itself quietly rebuilds the original bug inside
the alarm.
"""

from __future__ import annotations

import http.client
import json
import os
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass, replace

from semantic_catalog.records import MetricRecord

TOKEN_ENV = "OMNI_READ_TOKEN"
GH_FALLBACK_ENV = "OMNI_EVIDENCE_NO_GH"
REPO = "thegoodparty/omni"
STATE_PATH = "packages/runbooks/scripts/python/instrumentation_data/analytics_event_health_state.json"
_API = "https://api.github.com/repos/{repo}/contents/{path}"
_TIMEOUT = 20


@dataclass(frozen=True)
class Latch:
    """One declared leg omni has judged dormant.

    `leg_key` is the series key, which for a qualified leg names the slice rather
    than the whole event (`Viewed[path=/dashboard]`). `since` is the FIRST broken
    week of the run, not when the latch fired, so it is the truthful "not seen
    since" a reader needs.
    """

    leg_key: str
    metric: str
    since: str


def parse_latches(text: str) -> dict[str, list[Latch]]:
    """Map metric name -> its latched legs, from omni's health state file.

    Only `latched` entries count. A leg one broken week into a run is recorded
    there but not yet latched, and acting on it would re-create the false alarms
    the latch's two-week threshold exists to avoid.
    """
    doc = json.loads(text or "{}")
    latches = doc.get("latches") or {}
    if not isinstance(latches, dict):
        raise ValueError("'latches' is not a mapping")
    out: dict[str, list[Latch]] = {}
    for leg_key, entry in latches.items():
        if not isinstance(entry, dict) or not entry.get("latched"):
            continue
        metric, since = entry.get("metric"), entry.get("since")
        if not metric or not since:
            raise ValueError(f"latch on '{leg_key}' has no metric or no since date")
        out.setdefault(str(metric), []).append(
            Latch(leg_key=str(leg_key), metric=str(metric), since=str(since))
        )
    return {metric: sorted(legs, key=lambda x: x.leg_key) for metric, legs in out.items()}


def _fetch(token: str) -> str:
    request = urllib.request.Request(
        _API.format(repo=REPO, path=STATE_PATH),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json",
            "User-Agent": "gp-semantic-catalog-evidence",
        },
    )
    with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
        return response.read().decode()


def _fetch_via_gh() -> str:
    """The operator's own GitHub auth, for a run from a laptop that has no CI token."""
    proc = subprocess.run(
        ["gh", "api", "-H", "Accept: application/vnd.github.raw+json", f"repos/{REPO}/contents/{STATE_PATH}"],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"gh api exited {proc.returncode}")
    return proc.stdout


def load_latches(token: str | None = None) -> tuple[dict[str, list[Latch]], list[str]]:
    """Latched anchors per metric, plus any reason the read did not happen.

    Never raises. A cross-repo read failing must not take down the publish job,
    and an empty result with no explanation would be the silent-green state this
    module exists to remove — so the reason comes back alongside and every
    surface prints it.
    """
    token = token if token is not None else os.environ.get(TOKEN_ENV)
    use_gh = not token and not os.environ.get(GH_FALLBACK_ENV)
    if not token and not use_gh:
        return {}, [
            f"{TOKEN_ENV} is not set and {GH_FALLBACK_ENV} is set, so neither the token "
            f"nor the gh CLI fallback is available. Build approvals are NOT being checked "
            "against instrument health this run."
        ]
    try:
        text = _fetch_via_gh() if use_gh else _fetch(token)
    # Wide for the same reason omni's own cross-repo read is: a connection dropped
    # mid-body raises from inside urlopen's `with` and is not a URLError, the gh
    # fallback adds its own nonzero-exit RuntimeError, and either would otherwise
    # escape and fail the publish over a transient blip.
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,  # subclass of URLError; named for readability
        TimeoutError,
        http.client.IncompleteRead,
        http.client.RemoteDisconnected,
        RuntimeError,
        OSError,
        subprocess.TimeoutExpired,
    ) as exc:
        via = "gh api" if use_gh else "the GitHub API"
        return {}, [
            f"could not read the instrument health state from {REPO} via {via} ({exc}). "
            "Build approvals are NOT being checked against instrument health this run."
        ]
    try:
        return parse_latches(text), []
    except (ValueError, TypeError, AttributeError, json.JSONDecodeError) as exc:
        return {}, [
            f"{STATE_PATH} in {REPO} did not parse as health state ({exc}). Build "
            "approvals are NOT being checked against instrument health this run."
        ]


def reason(legs: list[Latch]) -> str:
    """The sentence the catalog prints under a metric needing re-verification."""
    named = ", ".join(f"'{leg.leg_key}'" for leg in legs)
    plural = "events have" if len(legs) > 1 else "event has"
    since = min(leg.since for leg in legs)
    return (
        f"declared {plural} not fired since {since}: {named} (instrument health monitor, "
        "latched). Clears when the event recovers, or when anchored_on adopts its successor."
    )


def apply(records: list[MetricRecord], latches: dict[str, list[Latch]]) -> list[MetricRecord]:
    """Mark each record whose declared anchor omni has latched as dormant.

    Only a metric with a BUILD approval is marked. A metric nobody has signed off
    on is already pending, and telling a reader that a pending approval needs
    re-verification says nothing.
    """
    out: list[MetricRecord] = []
    for rec in records:
        legs = latches.get(rec.name)
        if not legs or not rec.build_approved:
            out.append(rec)
            continue
        out.append(replace(rec, needs_reverification=reason(legs)))
    return out
