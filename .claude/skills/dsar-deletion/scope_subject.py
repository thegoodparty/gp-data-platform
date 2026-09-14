"""Scope a DSAR subject across every source that holds person-level data.

Read-only. Reports hit or miss per source so intake is one command instead of a
hand-assembled sweep. Run from `analytics/` so the Databricks connector resolves:

    cd analytics && uv run python ../.claude/skills/dsar-deletion/scope_subject.py \
        --name "First Last" --email someone@example.com --phone 555-555-5555 \
        --address "123 Example St"

Surnames are matched by substring, not equality, so "First M Last" and a hyphenated
surname still land. That means false positives: a search for "Mboh" also returns
"Schlumbohm". Read the ANCHOR block first, which reports exact-surname counts, then
use the fuzzy hits to catch anything the anchor missed.

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

CATALOG = "goodparty_data_catalog"

# Insert-only raw JSON of every version ever extracted. Large enough that scanning it
# blows the connector's retry budget on an X-Small warehouse, so it is opt-in.
DEEP_ONLY = {"airbyte_internal raw hubspot contacts"}

# Sources with a person-name column, checked for an exact surname match. This is the
# clean signal; the fuzzy probes exist to catch spelling and field-placement variants.
ANCHOR_TABLES = [
    ("gp_api_db_user", f"{CATALOG}.airbyte_source.gp_api_db_user", "last_name"),
    ("mart_civics.people", f"{CATALOG}.mart_civics.people", "last_name"),
    ("hubspot_contacts", f"{CATALOG}.airbyte_source.hubspot_api_contacts", "properties_lastname"),
    ("br_candidacies", f"{CATALOG}.airbyte_source.ballotready_s3_candidacies_v3", "last_name"),
    ("br_office_holders", f"{CATALOG}.airbyte_source.ballotready_s3_office_holders_v3", "last_name"),
    ("ts_candidates", f"{CATALOG}.airbyte_source.techspeed_gdrive_candidates", "last_name"),
    ("ts_officeholders", f"{CATALOG}.airbyte_source.techspeed_gdrive_officeholders", "last_name"),
    ("sent_to_hubspot", f"{CATALOG}.historical.ballotready_records_sent_to_hubspot", "last_name"),
    ("sent_to_techspeed", f"{CATALOG}.historical.ballotready_records_sent_to_techspeed", "last_name"),
    ("er_clustered_candidacies", f"{CATALOG}.er_source.clustered_candidacy_stages", "last_name"),
    ("er_clustered_officials", f"{CATALOG}.er_source.clustered_elected_officials", "last_name"),
]

# Structurally cannot hold a person, so there is nothing to probe. Reported so the
# operator knows it was considered rather than forgotten.
NO_PERSON_COLUMNS = [
    ("airbyte_source.ballotready_s3_recruitment_v1", "race and position level only"),
    ("airbyte_source.ddhq_elections_gsheet_*", "race level only, keyed on race_id"),
    ("airbyte_source.ballotready_api_*", "CivicEngine GraphQL carries no person names"),
]


def lit(value: str) -> str:
    """Render a SQL string literal, escaping quotes and backslashes."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "md", "phd", "esq"}


def split_name(name: str) -> tuple[str, str]:
    """Return (first, last). The surname is the last token after dropping suffixes.

    Everything-after-the-first-space is wrong: "First Middle Last" yields the key
    "middle last", which matches no last_name column and reports a false clear.
    """
    tokens = [t for t in (name or "").split() if t]
    while len(tokens) > 1 and tokens[-1].lower().strip(",") in NAME_SUFFIXES:
        tokens.pop()
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], tokens[0]
    return tokens[0], tokens[-1]


def build_anchors(name: str) -> list[tuple[str, str, str]]:
    """Exact-surname counts per source. The clean signal, free of substring noise."""
    last = split_name(name)[1].lower()
    parts = [
        f"select {lit(label)} as source, count(*) as n from {table} where lower(trim({column})) = {lit(last)}"
        for label, table, column in ANCHOR_TABLES
    ]
    return [("anchor", "exact surname match per source", "\nunion all ".join(parts))]


def build_probes(
    name: str, email: str, phone: str, address: str, deep: bool = False
) -> list[tuple[str, str, str]]:
    """Return (group, source, sql) for every source worth checking.

    Each probe selects identifying columns rather than a bare count, so a hit tells the
    operator which record to act on without a second query.
    """
    first, last = split_name(name)
    e = lit(email.lower())
    ph = digits(phone)

    last_like = lit(f"%{last.lower()}%") if last else "'%__nomatch__%'"
    email_like = lit(f"%{email.lower()}%")
    local_like = lit(f"%{email.lower().partition('@')[0]}%")
    phone_like = lit(f"%{ph}%") if ph else "'%__nomatch__%'"
    addr_like = lit(f"%{address.lower()}%") if address else "'%__nomatch__%'"

    # One regex alternation rather than three LIKEs, so a JSON blob is scanned once.
    # Three separate LIKEs over these columns exhausted the connector's retry budget.
    needles = [n for n in (email.lower().partition("@")[0], ph, address.lower()) if n]
    blob_pattern = lit("|".join(re.escape(n) for n in needles)) if needles else lit("(?!)")

    def blob(col: str) -> str:
        return f"lower(cast({col} as string)) rlike {blob_pattern}"

    return [
        (
            "A product",
            "airbyte_source.gp_api_db_user",
            f"""select id, email, phone, first_name, last_name, clerk_id, person_id, created_at
                from {CATALOG}.airbyte_source.gp_api_db_user
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(email,'')) like {local_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or lower(coalesce(name,'')) like {last_like}""",
        ),
        (
            "A product",
            "mart_civics.people",
            f"""select gp_person_id, gp_api_user_id, hs_contact_id, br_person_id, ddhq_candidate_id,
                       ts_candidate_code, first_name, last_name, email, phone, state
                from {CATALOG}.mart_civics.people
                where lower(coalesce(email,'')) = {e}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or lower(coalesce(last_name,'')) like {last_like}""",
        ),
        (
            "A product",
            "airbyte_source.hubspot_api_contacts",
            f"""select id, properties_email, properties_phone, properties_firstname,
                       properties_lastname, properties_state, properties_address, archived
                from {CATALOG}.airbyte_source.hubspot_api_contacts
                where lower(coalesce(properties_email,'')) = {e}
                   or lower(coalesce(properties_lastname,'')) like {last_like}
                   or regexp_replace(coalesce(properties_phone,''),'[^0-9]','') like {phone_like}
                   or {blob("properties")}""",
        ),
        (
            "A product",
            "airbyte_source.hubspot_api_companies",
            f"""select id, properties_candidate_name, properties_phone, properties_address, properties_state
                from {CATALOG}.airbyte_source.hubspot_api_companies
                where {blob("properties")}""",
        ),
        (
            "D copies",
            "archives.airbyte_source__hubspot_api_contacts_20260122",
            f"""select id, properties_email, properties_firstname, properties_lastname
                from {CATALOG}.archives.airbyte_source__hubspot_api_contacts_20260122
                where lower(coalesce(properties_email,'')) = {e}
                   or lower(coalesce(properties_lastname,'')) like {last_like}
                   or {blob("properties")}""",
        ),
        (
            "D copies",
            "dbt.snapshot__hubspot_api_contacts",
            f"""select id, properties_email, dbt_valid_from, dbt_valid_to
                from {CATALOG}.dbt.snapshot__hubspot_api_contacts
                where lower(coalesce(properties_email,'')) = {e}
                   or lower(coalesce(properties_lastname,'')) like {last_like}
                   or {blob("properties")}""",
        ),
        (
            "D copies",
            "airbyte_internal raw hubspot contacts",
            f"""select count(*) as raw_rows
                from {CATALOG}.airbyte_internal.airbyte_source_raw__stream_hubspot_api_contacts
                where lower(cast(_airbyte_data as string)) like {email_like}
                   or {blob("_airbyte_data")}
                having count(*) > 0""",
        ),
        (
            "record of request",
            "airbyte_source.hubspot_api_tickets",
            f"""select id, createdAt, properties_subject
                from {CATALOG}.airbyte_source.hubspot_api_tickets
                where lower(coalesce(properties_content,'')) like {email_like}
                   or lower(coalesce(properties_content,'')) like {last_like}""",
        ),
        (
            "A product",
            "airbyte_source.stripe_api_customers",
            f"""select id, email, name, phone, is_deleted
                from {CATALOG}.airbyte_source.stripe_api_customers
                where lower(coalesce(email,'')) = {e} or lower(coalesce(name,'')) like {last_like}""",
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
            # user_id is the gp-api user id, never an email, so match it through the
            # user table rather than comparing it to the address directly.
            f"""select count(*) as n
                from {CATALOG}.airbyte_source.amplitude_api_events
                where user_id in (
                        select cast(id as string)
                        from {CATALOG}.airbyte_source.gp_api_db_user
                        where lower(coalesce(email,'')) = {e}
                    )
                   or lower(cast(user_properties as string)) like {email_like}
                having count(*) > 0""",
        ),
        (
            "record of request",
            "airbyte_source.clickup_task",
            f"select id, name from {CATALOG}.airbyte_source.clickup_task where lower(coalesce(name,'')) like {last_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ballotready_s3_candidacies_v3",
            f"""select id, first_name, last_name, email, phone, state
                from {CATALOG}.airbyte_source.ballotready_s3_candidacies_v3
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}""",
        ),
        (
            "B vendor civic",
            # Raw source, like every other probe: the staging view applies the
            # suppression filter, so reading it would hide records we still hold.
            # email and phone live inside the `contacts` array, which the staging
            # model parses; match the serialized blob rather than duplicating that
            # parser here, normalizing to digits so formatting cannot hide a number.
            "airbyte_source.ballotready_s3_office_holders_v3",
            f"""select id, first_name, middle_name, last_name, nickname,
                       office_holder_mailing_address_line_1, contacts
                from {CATALOG}.airbyte_source.ballotready_s3_office_holders_v3
                where lower(coalesce(last_name,'')) like {last_like}
                   or lower(coalesce(nickname,'')) like {last_like}
                   or lower(coalesce(office_holder_mailing_address_line_1,'')) like {addr_like}
                   or lower(cast(contacts as string)) like {email_like}
                   or regexp_replace(cast(contacts as string), '[^0-9]', '') like {phone_like}""",
        ),
        (
            "B vendor civic",
            "airflow_source.ballotready_person_raw",
            f"""select database_id,
                       get_json_object(payload, '$.firstName') as first_name,
                       get_json_object(payload, '$.middleName') as middle_name,
                       get_json_object(payload, '$.lastName') as last_name
                from {CATALOG}.airflow_source.ballotready_person_raw
                where lower(get_json_object(payload, '$.lastName')) like {last_like}
                   or {blob("payload")}""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_candidates",
            f"""select first_name, last_name, email, phone_clean, street_address, office_name
                from {CATALOG}.airbyte_source.techspeed_gdrive_candidates
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or regexp_replace(coalesce(phone_clean,''),'[^0-9]','') like {phone_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or lower(coalesce(street_address,'')) like {addr_like}""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_officeholders",
            f"""select first_name, last_name, email, phone, street_address, office_name
                from {CATALOG}.airbyte_source.techspeed_gdrive_officeholders
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}
                   or lower(coalesce(street_address,'')) like {addr_like}""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_marketing_data_enrichment",
            f"select name from {CATALOG}.airbyte_source.techspeed_gdrive_marketing_data_enrichment where lower(coalesce(name,'')) like {last_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ddhq_gdrive_election_results_master",
            f"select candidate_id, candidate, state, office from {CATALOG}.airbyte_source.ddhq_gdrive_election_results_master where lower(coalesce(candidate,'')) like {last_like}",
        ),
        (
            "B vendor civic",
            "airbyte_source.ddhq_gdrive_election_results",
            f"select candidate_id, candidate, state, office from {CATALOG}.airbyte_source.ddhq_gdrive_election_results where lower(coalesce(candidate,'')) like {last_like}",
        ),
        (
            "onward disclosure",
            "historical.ballotready_records_sent_to_hubspot",
            f"""select first_name, middle_name, last_name, email, phone, state, upload_timestamp
                from {CATALOG}.historical.ballotready_records_sent_to_hubspot
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}""",
        ),
        (
            "onward disclosure",
            "historical.ballotready_records_sent_to_techspeed",
            f"""select first_name, middle_name, last_name, nickname, email, phone, state, upload_datetime
                from {CATALOG}.historical.ballotready_records_sent_to_techspeed
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or regexp_replace(coalesce(phone,''),'[^0-9]','') like {phone_like}""",
        ),
        (
            "B vendor civic",
            "er_source.clustered_candidacy_stages",
            f"""select first_name, last_name, email, phone, source_name, official_office_name
                from {CATALOG}.er_source.clustered_candidacy_stages
                where lower(coalesce(last_name,'')) like {last_like}
                   or lower(coalesce(email,'')) = {e}""",
        ),
        (
            "B vendor civic",
            "er_source.clustered_elected_officials",
            f"""select first_name, last_name, email, phone, source_name, official_office_name
                from {CATALOG}.er_source.clustered_elected_officials
                where lower(coalesce(last_name,'')) like {last_like}
                   or lower(coalesce(email,'')) = {e}""",
        ),
        (
            "A product",
            "mart_civics.person_identifiers via people",
            f"""select pi.gp_person_id, pi.source_name, pi.source_id, pi.record_key
                from {CATALOG}.mart_civics.person_identifiers as pi
                join {CATALOG}.mart_civics.people as p on p.gp_person_id = pi.gp_person_id
                where lower(coalesce(p.last_name,'')) like {last_like}
                   or lower(coalesce(p.email,'')) = {e}""",
        ),
    ]


def run(probes, dbc):
    import time

    hits, misses, errors = [], [], []
    slow: list[tuple[str, float]] = []
    summary: list[dict[str, object]] = []
    for group, source, sql in probes:
        started = time.monotonic()
        try:
            df = dbc.run_query(sql)
        except Exception as exc:  # noqa: BLE001
            errors.append((source, str(exc)[:160]))
            summary.append({"group": group, "source": source, "status": "error"})
            continue
        elapsed = time.monotonic() - started
        if elapsed > 60:
            slow.append((source, elapsed))
        if len(df):
            hits.append((group, source, df))
            summary.append({"group": group, "source": source, "status": "hit", "rows": len(df)})
        else:
            misses.append((group, source))
            summary.append({"group": group, "source": source, "status": "miss"})
    return hits, misses, errors, summary, slow


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help='Full name as given, e.g. "First Last"')
    parser.add_argument("--email", required=True)
    parser.add_argument("--phone", default="", help="Any format; digits are extracted")
    parser.add_argument("--address", default="", help='Street portion only, e.g. "123 Example St"')
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Also scan the airbyte_internal raw JSON. Slow; use when a hit is expected but not found.",
    )
    parser.add_argument("--out", default="", help="Optional path for a JSON summary")
    args = parser.parse_args()

    os.environ.setdefault("DATABRICKS_HTTP_PATH", DEFAULT_HTTP_PATH)
    import databricks_conn as dbc

    print(f"\nDSAR scope for {args.name} <{args.email}>\n" + "=" * 72)

    anchor_hits, _, anchor_errors, _, _ = run(build_anchors(args.name), dbc)
    print("\nANCHOR: exact surname match per source")
    if anchor_hits:
        df = anchor_hits[0][2]
        nonzero = df[df["n"] > 0] if "n" in df.columns else df
        print(df.to_string(index=False))
        print(
            "\n  => "
            + ("EXACT MATCHES PRESENT, act on these" if len(nonzero) else "no exact surname anywhere")
        )
    for source, msg in anchor_errors:
        print(f"  anchor query failed: {msg}")

    probes = [
        p
        for p in build_probes(args.name, args.email, args.phone, args.address, args.deep)
        if args.deep or p[1] not in DEEP_ONLY
    ]
    hits, misses, errors, summary, slow = run(probes, dbc)

    if hits:
        print(f"\nFUZZY HITS ({len(hits)} sources) - substring matching, expect false positives\n")
        for group, source, df in hits:
            print(f"[{group}] {source}  ({len(df)} rows)")
            print(df.to_string(index=False, max_colwidth=40))
            print()
    else:
        print("\nNo fuzzy hits in any source.\n")

    print(f"CLEAR ({len(misses)} sources)")
    for _, source in misses:
        print(f"  {source}")

    if errors:
        print(f"\nERRORS ({len(errors)}) - probe these by hand before concluding they are clear")
        for source, msg in errors:
            print(f"  {source}: {msg}")

    if slow:
        print("\nSLOW PROBES (over 60s)")
        for source, secs in sorted(slow, key=lambda x: -x[1]):
            print(f"  {source}: {secs:.0f}s")

    if not args.deep:
        print("\nSKIPPED (pass --deep to include)")
        for source in sorted(DEEP_ONLY):
            print(f"  {source}")

    print("\nNOT PROBED (no person-level columns)")
    for source, why in NO_PERSON_COLUMNS:
        print(f"  {source}: {why}")

    print("\nThe L2 voter file is out of scope by policy and was not swept.")
    print("\nNext: record the identifiers you will act on.\n")
    print(f"insert into {CATALOG}.source_dsar.suppressed_identifiers")
    print("    (request_id, subject_name, identifier_type, identifier_value,")
    print("     received_at, respond_by, notes, created_at, created_by)")
    print("values")
    print(f"    ('<TICKET>', {lit(args.name)}, 'email', {lit(args.email.lower())},")
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>'),")
    if digits(args.phone):
        print(f"    ('<TICKET>', {lit(args.name)}, 'phone', {lit(digits(args.phone))},")
        print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>'),")
    print(f"    ('<TICKET>', {lit(args.name)}, 'gp_api_user_id', '<id from the gp_api_db_user hit>',")
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>');")
    print("\nAmplitude keys on gp_api_user_id, not email. Omitting it leaves Amplitude unsuppressed.")
    print("Drop any row that does not apply; a subject with no gp-api account has no user id.")
    print("\nPresence in that table means suppress. Do not add an identifier you will not act on.")

    if args.out:
        Path(args.out).write_text(json.dumps(summary, indent=2))
        print(f"\nSummary written to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
