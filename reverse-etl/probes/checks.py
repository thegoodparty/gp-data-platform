"""The sandbox checks: what HubSpot actually does, for the behaviors retl assumes.

Each check answers one question the design leans on and returns a Finding carrying the
raw evidence, not a bare pass/fail -- the point is to learn the behavior, and a verdict
with no response body behind it cannot settle an argument later.

Checks that concern retl's own parsing (4, 5) feed the real HubSpot response into
`retl.hubspot_destination`, never a reimplementation, so a confirmed check is evidence
about the shipped code rather than about this file.

Check 9 (sales-owned omission end to end) is deliberately absent: it needs the sandbox
mirror and the desired-state model, so it is not an API-only probe.

Run: `uv run python -m probes.checks --all` from `reverse-etl/`.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from retl.hubspot_destination import (
    HUBSPOT_ID_PROPERTY,
    UPSERT_PATH,
    build_batch_body,
    parse_batch_response,
)
from retl.hubspot_destination import HttpResponse as RetlHttpResponse

from .client import SandboxClient, settle

# An address HubSpot's own validation rejects, used to force one row of a batch to fail
# so the 207 envelope has something to report.
INVALID_EMAIL = "not-an-email"


@dataclass
class Finding:
    check: int
    title: str
    verdict: str
    implication: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    manual_followup: str = ""


def _upsert(client: SandboxClient, rows: list[tuple[str, dict[str, Any]]]) -> Any:
    """Send one batch through retl's own body builder, so the checks exercise the real
    request shape rather than a hand-rolled one.

    Contacts the upsert CREATES are registered for cleanup here rather than in each check:
    an upsert is how most checks make a contact, and registering at the call site is what
    stops a run leaving orphans in the portal.
    """
    serialized = [(key, json.dumps(props)) for key, props in rows]
    response = client.request("POST", UPSERT_PATH, json=build_batch_body(serialized))
    client.created_contact_ids.extend(
        str(r["id"]) for r in response.body.get("results", []) if r.get("new") and r.get("id")
    )
    return response


def check_01_merged_contact_ids(client: SandboxClient) -> Finding:
    """After sales merges two contacts, does an upsert on the loser's person key still land?"""
    key_a, key_b = client.tag("merge-a"), client.tag("merge-b")
    id_a = client.create_contact({HUBSPOT_ID_PROPERTY: key_a}, label="merge-a")
    id_b = client.create_contact({HUBSPOT_ID_PROPERTY: key_b}, label="merge-b")

    merge = client.request(
        "POST",
        "/crm/v3/objects/contacts/merge",
        json={"primaryObjectId": id_a, "objectIdToMerge": id_b},
    )
    settle()

    after_merge = client.get_contact(id_a, [HUBSPOT_ID_PROPERTY])
    response = _upsert(client, [(key_b, {"jobtitle": "after merge"})])
    settle()

    results = response.body.get("results", [])
    landed_on = str(results[0].get("id")) if results else None
    return Finding(
        check=1,
        title="Merged/retired contact-id behavior on batch upsert",
        verdict=(
            f"upsert on the merged-away key returned {response.status_code}; "
            f"landed on contact {landed_on} (survivor was {id_a}, merged-away was {id_b})"
        ),
        implication=(
            "If the loser's key creates a NEW contact, a merge silently splits a person "
            "back into two records and the design's claim that our key survives merges is wrong."
        ),
        evidence={
            "merge_status": merge.status_code,
            "survivor_person_id_after_merge": after_merge.get(HUBSPOT_ID_PROPERTY),
            "upsert_status": response.status_code,
            "upsert_body": response.body,
            "created_new_contact": landed_on not in (id_a, id_b),
        },
    )


def check_02_omitted_properties_untouched(client: SandboxClient) -> Finding:
    """The daily diff only sends changed payloads, so an omitted property must not be cleared."""
    key = client.tag("omit")
    contact_id = client.create_contact(
        {HUBSPOT_ID_PROPERTY: key, "jobtitle": "original", "email": f"{key}@example.com"},
        label="omit",
    )
    settle()

    _upsert(client, [(key, {"lastname": "touched"})])
    settle()

    after = client.get_contact(contact_id, ["jobtitle", "email", "lastname"])
    preserved = after.get("jobtitle") == "original"
    return Finding(
        check=2,
        title="Omitted properties genuinely untouched",
        verdict=f"jobtitle after an upsert that omitted it: {after.get('jobtitle')!r}",
        implication=(
            "The whole daily diff depends on this. If omission clears, every send would "
            "wipe every property not in that day's payload."
        ),
        evidence={"preserved": preserved, "properties_after": after},
    )


def check_03_identical_value_writes(client: SandboxClient) -> Finding:
    """Re-sending an unchanged payload should be free. Does HubSpot treat it as a write?"""
    key = client.tag("idem")
    contact_id = client.create_contact({HUBSPOT_ID_PROPERTY: key, "jobtitle": "same"}, label="idem")
    settle()

    _upsert(client, [(key, {"jobtitle": "same"})])
    settle()
    first = client.get_contact(contact_id, ["lastmodifieddate"])
    history_first = client.property_history(contact_id, "jobtitle")

    _upsert(client, [(key, {"jobtitle": "same"})])
    settle()
    second = client.get_contact(contact_id, ["lastmodifieddate"])
    history_second = client.property_history(contact_id, "jobtitle")

    bumped = first.get("lastmodifieddate") != second.get("lastmodifieddate")
    return Finding(
        check=3,
        title="Identical-value writes: lastmodifieddate, property history",
        verdict=(
            f"lastmodifieddate {'changed' if bumped else 'unchanged'} on an identical rewrite; "
            f"jobtitle history entries {len(history_first)} -> {len(history_second)}"
        ),
        implication=(
            "A bumped lastmodifieddate on an unchanged value pollutes any downstream "
            "recency logic and makes 'HubSpot's copy is newer' useless as an ownership rule."
        ),
        evidence={
            "lastmodifieddate_before": first.get("lastmodifieddate"),
            "lastmodifieddate_after": second.get("lastmodifieddate"),
            "history_len_before": len(history_first),
            "history_len_after": len(history_second),
        },
        manual_followup=(
            "Workflow re-triggering is not covered here. Sandboxes do not inherit production "
            "workflows; build a contact workflow in the sandbox by hand and re-run, or record "
            "that the sandbox had none and the question stays open."
        ),
    )


def check_04_partial_failure_shape(client: SandboxClient) -> Finding:
    """What a 207 looks like per row, and whether retl's parser attributes it correctly."""
    good_a, bad, good_b = client.tag("207a"), client.tag("207bad"), client.tag("207b")
    rows = [
        (good_a, {"email": f"{good_a}@example.com"}),
        (bad, {"email": INVALID_EMAIL}),
        (good_b, {"email": f"{good_b}@example.com"}),
    ]
    response = _upsert(client, rows)

    sent_rows = {key: json.dumps(props) for key, props in rows}
    parsed = parse_batch_response(
        RetlHttpResponse(status_code=response.status_code, body=response.body),
        flow_id="probe",
        sent_rows=sent_rows,
    )
    unknown = [e.tracking_key for e in parsed.errors if e.error_code == "UNKNOWN_DELIVERY"]
    return Finding(
        check=4,
        title="What a 207 partial failure looks like per row",
        verdict=(
            f"status {response.status_code}; retl attributed {len(parsed.confirmed)} confirmed "
            f"and {len(parsed.errors)} errors, of which {len(unknown)} were UNKNOWN_DELIVERY"
        ),
        implication=(
            "Any UNKNOWN_DELIVERY here means the error envelope does not carry "
            "objectWriteTraceId the way the parser expects, and per-row attribution is broken."
        ),
        evidence={
            "raw_body": response.body,
            "confirmed_keys": sorted(parsed.confirmed),
            "errors": [asdict(e) for e in parsed.errors],
            "error_context_keys": sorted(
                {k for e in response.body.get("errors", []) for k in (e.get("context") or {})}
            ),
        },
    )


def check_05_mixed_id_properties(client: SandboxClient) -> Finding:
    """One batch mixing idProperty values: accepted, or rejected outright?"""
    key = client.tag("mixed")
    email = f"{client.tag('mixed-email')}@example.com"
    client.create_contact({HUBSPOT_ID_PROPERTY: key, "email": email}, label="mixed")
    settle()

    body = {
        "inputs": [
            {
                "idProperty": HUBSPOT_ID_PROPERTY,
                "id": key,
                "properties": {"jobtitle": "by person id"},
                "objectWriteTraceId": key,
            },
            {
                "idProperty": "email",
                "id": email,
                "properties": {"jobtitle": "by email"},
                "objectWriteTraceId": email,
            },
        ]
    }
    response = client.request("POST", UPSERT_PATH, json=body)
    return Finding(
        check=5,
        title="One batch mixing idProperty values",
        verdict=f"status {response.status_code} for a batch using two different idProperty values",
        implication=(
            "retl sends one idProperty per batch today. If mixing is rejected, that constraint "
            "needs stating in the code; if accepted, a future flow can batch heterogeneous keys."
        ),
        evidence={"status": response.status_code, "body": response.body},
    )


def check_06_new_flag_reliability(client: SandboxClient) -> Finding:
    """Does `new: true` mean created, reliably enough to count creates from it?"""
    key = client.tag("new")
    first = _upsert(client, [(key, {"jobtitle": "first"})])
    settle()
    second = _upsert(client, [(key, {"jobtitle": "second"})])
    settle()

    def flags(response: Any) -> list[Any]:
        return [r.get("new") for r in response.body.get("results", [])]

    return Finding(
        check=6,
        title="Whether `new: true` reliably means created",
        verdict=f"first upsert new={flags(first)}, second upsert new={flags(second)}",
        implication=(
            "The enable-day convergence reports creates vs updates. If `new` is absent or "
            "always true, that number has to come from the mirror instead."
        ),
        evidence={
            "first_results": first.body.get("results"),
            "second_results": second.body.get("results"),
            "field_present": all("new" in r for r in first.body.get("results", [])),
        },
    )


def _rate_limit_remaining(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if "ratelimit" in k.lower()}


def check_07_batch_rate_limit_accounting(client: SandboxClient) -> Finding:
    """Does a 100-record batch cost 1 request or 100 against the rate limit?"""
    one = _upsert(client, [(client.tag("rl-1"), {"jobtitle": "rl"})])
    before = _rate_limit_remaining(one.headers)

    many = [(client.tag(f"rl-{i}"), {"jobtitle": "rl"}) for i in range(100)]
    hundred = _upsert(client, many)
    after = _rate_limit_remaining(hundred.headers)

    def remaining(headers: dict[str, str]) -> int | None:
        for key, value in headers.items():
            if key.lower() == "x-hubspot-ratelimit-remaining":
                return int(value)
        return None

    before_n, after_n = remaining(one.headers), remaining(hundred.headers)
    delta = before_n - after_n if before_n is not None and after_n is not None else None
    return Finding(
        check=7,
        title="Whether a 100-record batch counts as 1 or 100 against the rate limit",
        verdict=f"remaining went {before_n} -> {after_n} across a 1-row then a 100-row batch (delta {delta})",
        implication=(
            "Docs say a batch is one request. A delta near 100 instead of near 1 would mean the "
            "enable-day convergence needs throttling it does not have."
        ),
        evidence={"headers_after_single": before, "headers_after_hundred": after},
    )


def check_08_email_collision(client: SandboxClient) -> Finding:
    """A person-keyed upsert carrying an email another contact already holds: reject or reassign?"""
    email = f"{client.tag('collide')}@example.com"
    holder_key = client.tag("collide-holder")
    holder_id = client.create_contact(
        {HUBSPOT_ID_PROPERTY: holder_key, "email": email}, label="collide-holder"
    )
    settle()

    other_key = client.tag("collide-other")
    response = _upsert(client, [(other_key, {"email": email})])
    settle()

    holder_after = client.get_contact(holder_id, ["email"])
    reassigned = holder_after.get("email") != email

    return Finding(
        check=8,
        title="Person-keyed upsert carrying an email another contact already holds",
        verdict=(
            f"status {response.status_code}; the original holder's email is now "
            f"{holder_after.get('email')!r} ({'reassigned' if reassigned else 'intact'})"
        ),
        implication=(
            "Reassignment or corruption is what would justify building the shared-email hold. "
            "A clean rejection means the hold is unnecessary and should not be built."
        ),
        evidence={
            "status": response.status_code,
            "body": response.body,
            "holder_email_after": holder_after.get("email"),
            "reassigned": reassigned,
        },
    )


DATED_PATH_CANDIDATES = [
    "/crm/objects/2026-09/contacts/batch/upsert",
    "/crm/objects/contacts/batch/upsert/2026-09",
    "/crm/2026-09/objects/contacts/batch/upsert",
]


def check_10_dated_api_version(client: SandboxClient) -> Finding:
    """v3 loses support in Sept 2027. Which dated path replaces our one call, and is it identical?"""
    key = client.tag("dated")
    serialized = [(key, json.dumps({"jobtitle": "dated"}))]
    body = build_batch_body(serialized)

    attempts = {}
    working = None
    for path in DATED_PATH_CANDIDATES:
        response = client.request("POST", path, json=body)
        attempts[path] = {"status": response.status_code, "body": response.body}
        if response.status_code in (200, 207) and working is None:
            working = path
            client.created_contact_ids.extend(
                str(r["id"]) for r in response.body.get("results", []) if r.get("id")
            )

    baseline = client.request("POST", UPSERT_PATH, json=body)
    dated_keys = sorted(attempts[working]["body"]) if working else []
    return Finding(
        check=10,
        title="Date-based API versioning for the batch upsert",
        verdict=(
            f"working dated path: {working or 'none of the candidates'}; "
            f"v3 baseline returned {baseline.status_code}"
        ),
        implication=(
            "If a dated path works with an identical response shape, migrating off v3 is a "
            "one-line change. A different shape means the 207 parsing needs reworking first."
        ),
        evidence={
            "attempts": attempts,
            "v3_response_keys": sorted(baseline.body),
            "dated_response_keys": dated_keys,
            "shapes_match": dated_keys == sorted(baseline.body) if working else None,
        },
        manual_followup=(
            "" if working else "No candidate path worked; check the API reference version dropdown."
        ),
    )


CHECKS: dict[int, Callable[[SandboxClient], Finding]] = {
    1: check_01_merged_contact_ids,
    2: check_02_omitted_properties_untouched,
    3: check_03_identical_value_writes,
    4: check_04_partial_failure_shape,
    5: check_05_mixed_id_properties,
    6: check_06_new_flag_reliability,
    7: check_07_batch_rate_limit_accounting,
    8: check_08_email_collision,
    10: check_10_dated_api_version,
}


def render(finding: Finding) -> str:
    lines = [f"## Check {finding.check}: {finding.title}", "", f"**Verdict:** {finding.verdict}"]
    if finding.implication:
        lines += ["", f"**Why it matters:** {finding.implication}"]
    if finding.manual_followup:
        lines += ["", f"**Manual follow-up:** {finding.manual_followup}"]
    lines += ["", "```json", json.dumps(finding.evidence, indent=2, default=str), "```", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the HubSpot sandbox checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", type=int, choices=sorted(CHECKS), help="Run one check")
    group.add_argument("--all", action="store_true", help="Run every check")
    parser.add_argument("--json", action="store_true", help="Emit findings as JSON, not markdown")
    parser.add_argument("--no-cleanup", action="store_true", help="Leave created contacts in place")
    args = parser.parse_args(argv)

    client = SandboxClient.from_env()
    print(f"portal {client.expected_portal_id} confirmed, run id {client.run_id}", file=sys.stderr)

    selected = sorted(CHECKS) if args.all else [args.check]
    findings: list[Finding] = []
    try:
        for number in selected:
            try:
                findings.append(CHECKS[number](client))
            except Exception as exc:
                findings.append(
                    Finding(
                        check=number,
                        title=CHECKS[number].__doc__ or "",
                        verdict=f"CHECK ERRORED: {type(exc).__name__}: {exc}",
                    )
                )
    finally:
        if not args.no_cleanup:
            print(f"cleaned up {client.cleanup()} contact(s)", file=sys.stderr)

    if args.json:
        print(json.dumps([asdict(f) for f in findings], indent=2, default=str))
    else:
        for finding in findings:
            print(render(finding))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
