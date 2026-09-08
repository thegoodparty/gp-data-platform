"""Scope a DSAR subject across every source that holds person-level data.

Read-only. Reports hit or miss per source so intake is one command instead of a
hand-assembled sweep. Run from `analytics/` so the Databricks connector resolves:

    cd analytics && uv run python ../.claude/skills/dsar-deletion/scope_subject.py \
        --name "First Last" --email someone@example.com --phone 555-555-5555

The L2 voter file is deliberately not swept. We do not delete from it, so scanning it
would only mint a subject-to-LALVOTERID link we have no purpose for.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "analytics" / "lib"))

DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/18583d8b081c6486"  # Serverless Starter


def lit(value: str) -> str:
    """Render a SQL string literal, escaping quotes and backslashes."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def build_probes(name: str, email: str, phone: str) -> list[tuple[str, str, str]]:
    """Return (group, source, sql) for every source worth checking.

    Each probe selects identifying columns rather than a bare count, so a hit tells the
    operator which record to act on without a second query.
    """
    first, _, last = (name or "").partition(" ")
    last = last or first
    e, fn, ln = lit(email.lower()), lit(first.lower()), lit(last.lower())
    ph = digits(phone)
    phone_like = lit(f"%{ph}%") if ph else "'%__nomatch__%'"
    name_like = lit(f"%{last.lower()}%") if last else "'%__nomatch__%'"
    email_like = lit(f"%{email.lower()}%")

    return [
        (
            "A product",
            "airbyte_source.gp_api_db_user",
            f"""select id, email, phone, first_name, last_name, clerk_id, person_id, created_at
                from goodparty_data_catalog.airbyte_source.gp_api_db_user
                where lower(coalesce(email,'')) = {e}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or (lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln})""",
        ),
        (
            "A product",
            "mart_civics.people",
            f"""select gp_person_id, gp_api_user_id, hs_contact_id, br_person_id, ddhq_candidate_id,
                       ts_candidate_code, first_name, last_name, email, phone, state
                from goodparty_data_catalog.mart_civics.people
                where lower(coalesce(email,'')) = {e}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or (lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln})""",
        ),
        (
            "A product",
            "airbyte_source.hubspot_api_contacts",
            f"""select id, properties_email, properties_phone, properties_firstname,
                       properties_lastname, properties_state, archived
                from goodparty_data_catalog.airbyte_source.hubspot_api_contacts
                where lower(coalesce(properties_email,'')) = {e}
                   or regexp_replace(coalesce(properties_phone,''),'[^0-9]','') like {phone_like}
                   or (lower(coalesce(properties_firstname,'')) = {fn}
                       and lower(coalesce(properties_lastname,'')) = {ln})""",
        ),
        (
            "D copies",
            "archives.airbyte_source__hubspot_api_contacts_20260122",
            f"""select id, properties_email, properties_firstname, properties_lastname
                from goodparty_data_catalog.archives.airbyte_source__hubspot_api_contacts_20260122
                where lower(coalesce(properties_email,'')) = {e}""",
        ),
        (
            "D copies",
            "dbt.snapshot__hubspot_api_contacts",
            f"""select id, properties_email, dbt_valid_from, dbt_valid_to
                from goodparty_data_catalog.dbt.snapshot__hubspot_api_contacts
                where lower(coalesce(properties_email,'')) = {e}""",
        ),
        (
            "D copies",
            "airbyte_internal raw hubspot contacts",
            f"""select count(*) as raw_rows
                from goodparty_data_catalog.airbyte_internal.airbyte_source_raw__stream_hubspot_api_contacts
                where lower(cast(_airbyte_data as string)) like {email_like}
                having count(*) > 0""",
        ),
        (
            "record of request",
            "airbyte_source.hubspot_api_tickets",
            f"""select id, createdAt, properties_subject
                from goodparty_data_catalog.airbyte_source.hubspot_api_tickets
                where lower(coalesce(properties_content,'')) like {email_like}
                   or lower(coalesce(properties_content,'')) like {name_like}""",
        ),
        (
            "A product",
            "airbyte_source.stripe_api_customers",
            f"""select id, email, name, phone, is_deleted
                from goodparty_data_catalog.airbyte_source.stripe_api_customers
                where lower(coalesce(email,'')) = {e} or lower(coalesce(name,'')) like {name_like}""",
        ),
        (
            "A product",
            "segment gp_api.identifies",
            f"select count(*) as n from segment_storage.gp_api.identifies where lower(coalesce(email,'')) = {e} having count(*) > 0",
        ),
        (
            "A product",
            "segment gp_api.users",
            f"select count(*) as n from segment_storage.gp_api.users where lower(coalesce(email,'')) = {e} having count(*) > 0",
        ),
        (
            "A product",
            "segment gp_api.tracks",
            f"select count(*) as n from segment_storage.gp_api.tracks where lower(coalesce(context_traits_email,'')) = {e} having count(*) > 0",
        ),
        (
            "A product",
            "segment web_app.identifies",
            f"select count(*) as n from segment_storage.web_app.identifies where lower(coalesce(email,'')) = {e} having count(*) > 0",
        ),
        (
            "A product",
            "segment web_app.users",
            f"select count(*) as n from segment_storage.web_app.users where lower(coalesce(email,'')) = {e} having count(*) > 0",
        ),
        (
            "A product",
            "airbyte_source.amplitude_api_events",
            f"""select count(*) as n
                from goodparty_data_catalog.airbyte_source.amplitude_api_events
                where lower(coalesce(user_id,'')) = {e}
                   or lower(cast(user_properties as string)) like {email_like}
                having count(*) > 0""",
        ),
        (
            "record of request",
            "airbyte_source.clickup_task",
            f"select id, name from goodparty_data_catalog.airbyte_source.clickup_task where lower(coalesce(name,'')) like {name_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ballotready_s3_candidacies_v3",
            f"""select id, first_name, last_name, email, state
                from goodparty_data_catalog.airbyte_source.ballotready_s3_candidacies_v3
                where lower(coalesce(email,'')) = {e}
                   or (lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln})""",
        ),
        (
            "B vendor civic",
            "airbyte_source.ballotready_s3_office_holders_v3",
            f"""select id, first_name, last_name
                from goodparty_data_catalog.airbyte_source.ballotready_s3_office_holders_v3
                where lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln}""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_candidates",
            f"""select first_name, last_name, email, phone_clean, street_address, office_name
                from goodparty_data_catalog.airbyte_source.techspeed_gdrive_candidates
                where lower(coalesce(email,'')) = {e}
                   or regexp_replace(coalesce(phone_clean,''),'[^0-9]','') like {phone_like}
                   or (lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln})""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_officeholders",
            f"""select first_name, last_name, email, phone, street_address, office_name
                from goodparty_data_catalog.airbyte_source.techspeed_gdrive_officeholders
                where lower(coalesce(email,'')) = {e}
                   or (lower(coalesce(first_name,'')) = {fn} and lower(coalesce(last_name,'')) = {ln})""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_marketing_data_enrichment",
            f"select name from goodparty_data_catalog.airbyte_source.techspeed_gdrive_marketing_data_enrichment where lower(coalesce(name,'')) like {name_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ddhq_gdrive_election_results_master",
            f"select candidate_id, candidate, state, office from goodparty_data_catalog.airbyte_source.ddhq_gdrive_election_results_master where lower(coalesce(candidate,'')) like {name_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ddhq_gdrive_election_results",
            f"select candidate_id, candidate, state, office from goodparty_data_catalog.airbyte_source.ddhq_gdrive_election_results where lower(coalesce(candidate,'')) like {name_like}",
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help='Full name as given, e.g. "First Last"')
    parser.add_argument("--email", required=True)
    parser.add_argument("--phone", default="", help="Any format; digits are extracted")
    parser.add_argument("--out", default="", help="Optional path for a JSON summary")
    args = parser.parse_args()

    os.environ.setdefault("DATABRICKS_HTTP_PATH", DEFAULT_HTTP_PATH)
    import databricks_conn as dbc

    hits, misses, errors = [], [], []
    summary: list[dict[str, object]] = []

    for group, source, sql in build_probes(args.name, args.email, args.phone):
        try:
            df = dbc.run_query(sql)
        except Exception as exc:  # noqa: BLE001
            errors.append((source, str(exc)[:160]))
            summary.append({"group": group, "source": source, "status": "error"})
            continue
        if len(df):
            hits.append((group, source, df))
            summary.append({"group": group, "source": source, "status": "hit", "rows": len(df)})
        else:
            misses.append((group, source))
            summary.append({"group": group, "source": source, "status": "miss"})

    print(f"\nDSAR scope for {args.name} <{args.email}>\n" + "=" * 72)

    if hits:
        print(f"\nHITS ({len(hits)} sources)\n")
        for group, source, df in hits:
            print(f"[{group}] {source}  ({len(df)} rows)")
            print(df.to_string(index=False, max_colwidth=44))
            print()
    else:
        print("\nNo hits in any source.\n")

    print(f"CLEAR ({len(misses)} sources)")
    for _, source in misses:
        print(f"  {source}")

    if errors:
        print(f"\nERRORS ({len(errors)}) - probe these by hand before concluding they are clear")
        for source, msg in errors:
            print(f"  {source}: {msg}")

    print("\nThe L2 voter file is out of scope by policy and was not swept.")
    print("\nNext: record the identifiers you will act on.\n")
    print("insert into goodparty_data_catalog.source_dsar.suppressed_identifiers")
    print("    (request_id, subject_name, identifier_type, identifier_value,")
    print("     received_at, respond_by, notes, created_at, created_by)")
    print("values")
    print(f"    ('<TICKET>', {lit(args.name)}, 'email', {lit(args.email.lower())},")
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>');")
    print("\nPresence in that table means suppress. Do not add an identifier you will not act on.")

    if args.out:
        Path(args.out).write_text(json.dumps(summary, indent=2))
        print(f"\nSummary written to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
