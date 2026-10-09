"""Merge named contact pairs and check the result against what the merge rule predicts.

For the test merges that confirm hubspot_contact_merge_candidates picks the right
primary before the bulk run. Pairs come from that model as `primary:secondary`. Without
`--execute` nothing is written: each pair is read, checked to still exist, and printed
with the values the merge should keep.

A merge cannot be undone. This runs against whatever portal
`RETL_PROBE_EXPECTED_PORTAL_ID` names (the sandbox by default), and only on pairs given
on the command line.

    uv run python -m probes.merge_pairs --pair 111:222 --pair 333:444
    RETL_PROBE_EXPECTED_PORTAL_ID=<prod> uv run python -m probes.merge_pairs --pair 111:222 --execute
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from retl.hubspot_destination import MERGE_HISTORY_PROPERTIES, merged_contact_ids

from .client import SandboxClient, settle

MERGE_PATH = "/crm/v3/objects/contacts/merge"

# The properties the ranking cares about. A merge keeps the primary's value where it
# has one and the secondary's otherwise; the listed exceptions follow their own rules.
PRIMARY_WINS = (
    "firstname",
    "lastname",
    "email",
    "phone",
    "hubspot_owner_id",
    "win_stage",
    "hs_lead_status",
    "pro_candidate",
    "goodparty_org_user_id",
    "candidate_office",
    "state",
)
EXCEPTIONS = ("lifecyclestage", "createdate", "hs_additional_emails")
LIFECYCLE_ORDER = (
    "subscriber",
    "lead",
    "marketingqualifiedlead",
    "salesqualifiedlead",
    "opportunity",
    "customer",
    "evangelist",
)
READ_PROPERTIES = (*PRIMARY_WINS, *EXCEPTIONS, *MERGE_HISTORY_PROPERTIES)


def _read(client: SandboxClient, contact_id: str) -> tuple[str | None, dict[str, Any]]:
    """The id HubSpot answers with, which differs from the one asked for once the
    contact has been merged away, and its properties."""
    query = ",".join(READ_PROPERTIES)
    response = client.request("GET", f"/crm/v3/objects/contacts/{contact_id}?properties={query}")
    if response.status_code >= 300:
        return None, {}
    return str(response.body.get("id")), response.body.get("properties", {})


def _blank(value: Any) -> bool:
    return value is None or value == ""


def predict(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
    expected = {
        prop: secondary.get(prop) if _blank(primary.get(prop)) else primary.get(prop) for prop in PRIMARY_WINS
    }
    stages = [
        s for s in (primary.get("lifecyclestage"), secondary.get("lifecyclestage")) if s in LIFECYCLE_ORDER
    ]
    expected["lifecyclestage"] = (
        max(stages, key=LIFECYCLE_ORDER.index) if stages else primary.get("lifecyclestage")
    )
    dates = [d for d in (primary.get("createdate"), secondary.get("createdate")) if d]
    expected["createdate"] = min(dates) if dates else None
    return expected


def run_pair(client: SandboxClient, primary_id: str, secondary_id: str, *, execute: bool) -> dict[str, Any]:
    result: dict[str, Any] = {"primary_id": primary_id, "secondary_id": secondary_id}
    p_seen, primary = _read(client, primary_id)
    s_seen, secondary = _read(client, secondary_id)
    # A contact already merged away answers with its survivor's id; merging it again
    # would merge the survivor, which nobody reviewed.
    for role, asked, seen in (("primary", primary_id, p_seen), ("secondary", secondary_id, s_seen)):
        if seen != asked:
            result["skipped"] = f"{role} {asked} " + (
                "not found" if seen is None else f"now resolves to {seen}"
            )
            return result
    if p_seen == s_seen:
        result["skipped"] = "both ids resolve to one contact"
        return result

    expected = predict(primary, secondary)
    result["before"] = {"primary": primary, "secondary": secondary}
    result["expected"] = expected
    if not execute:
        return result

    merge = client.request(
        "POST", MERGE_PATH, json={"primaryObjectId": primary_id, "objectIdToMerge": secondary_id}
    )
    result["merge_status"] = merge.status_code
    if merge.status_code >= 300:
        result["merge_error"] = merge.body.get("message")
        return result
    settle(2.0)

    survivor_id = str(merge.body.get("id") or "")
    _, survivor = _read(client, survivor_id)
    retired_resolves_to, _ = _read(client, secondary_id)
    result.update(
        survivor_id=survivor_id,
        survivor_is_new_record=survivor_id not in (primary_id, secondary_id),
        retired_id_resolves_to=retired_resolves_to,
        after=survivor,
        merge_history={prop: merged_contact_ids(survivor.get(prop)) for prop in MERGE_HISTORY_PROPERTIES},
        mismatched={
            prop: {"expected": want, "observed": survivor.get(prop)}
            for prop, want in expected.items()
            if prop != "createdate" and (survivor.get(prop) or None) != (want or None)
        },
    )
    return result


def _parse_pair(text: str) -> tuple[str, str]:
    primary, sep, secondary = text.partition(":")
    if not sep or not primary.isdigit() or not secondary.isdigit() or primary == secondary:
        raise argparse.ArgumentTypeError(f"expected PRIMARY_ID:SECONDARY_ID, got {text!r}")
    return primary, secondary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge reviewed HubSpot contact pairs")
    parser.add_argument(
        "--pair", type=_parse_pair, action="append", required=True, help="PRIMARY_ID:SECONDARY_ID"
    )
    parser.add_argument("--execute", action="store_true", help="Merge. Without it nothing is written.")
    args = parser.parse_args(argv)

    client = SandboxClient.from_env()
    mode = "EXECUTE" if args.execute else "dry run"
    print(f"portal {client.expected_portal_id} confirmed, {mode}, {len(args.pair)} pair(s)", file=sys.stderr)
    results = [run_pair(client, p, s, execute=args.execute) for p, s in args.pair]
    print(json.dumps(results, indent=2, default=str))
    return 1 if any(r.get("merge_error") or r.get("mismatched") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
