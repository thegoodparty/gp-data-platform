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
import os
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "analytics" / "lib"))

DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/18583d8b081c6486"  # Serverless Starter

CATALOG = "goodparty_data_catalog"

# A pattern no value matches, for identifiers the request did not supply.
NOMATCH = "'%__nomatch__%'"

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

SEGMENT_TABLES = [
    ("gp_api", "identifies", "email"),
    ("gp_api", "users", "email"),
    ("gp_api", "tracks", "context_traits_email"),
    ("web_app", "identifies", "email"),
    ("web_app", "users", "email"),
]

# No person-level columns, so nothing to probe. Reported so they read as considered.
NO_PERSON_COLUMNS = [
    ("airbyte_source.ballotready_s3_recruitment_v1", "race and position level only"),
    ("airbyte_source.ddhq_elections_gsheet_*", "race level only, keyed on race_id"),
    ("airbyte_source.ballotready_api_*", "CivicEngine GraphQL carries no person names"),
]


def lit(value: str) -> str:
    """Render a SQL string literal, escaping quotes and backslashes."""
    # Databricks evaluates a doubled quote inside a literal to nothing.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def digits(value: str) -> str:
    """Digits only, minus a leading US country code, matching dsar_normalize in dbt."""
    return re.sub(r"^1([0-9]{10})$", r"\1", re.sub(r"[^0-9]", "", value or ""))


NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "md", "phd", "esq"}


def surname(name: str) -> str:
    """Last token after dropping suffixes, so "First Middle Last" keys on "last"."""
    tokens = [t for t in (name or "").split() if t]
    while len(tokens) > 1 and tokens[-1].lower().strip(",") in NAME_SUFFIXES:
        tokens.pop()
    return tokens[-1] if tokens else ""


def sql_phone_digits(column: str) -> str:
    """The dbt dsar_normalize phone rule: digits only, minus a leading US country code."""
    return f"regexp_replace(regexp_replace({column}, '[^0-9]', ''), '^1([0-9]{{10}})$', '$1')"


def build_anchors(name: str) -> list[tuple[str, str, str]]:
    """Exact-surname counts per source. The clean signal, free of substring noise."""
    last = surname(name).lower()
    parts = [
        f"select {lit(label)} as source, count(*) as n from {table} where lower(trim({column})) = {lit(last)}"
        for label, table, column in ANCHOR_TABLES
    ]
    return [("anchor", "exact surname match per source", "\nunion all ".join(parts))]


def build_probes(
    name: str, email: str, phone: str, address: str, gp_api_user_id: str = ""
) -> list[tuple[str, str, str]]:
    """Return (group, source, sql) for every source worth checking.

    Each probe selects identifying columns rather than a bare count, so a hit tells the
    operator which record to act on without a second query.
    """
    last = surname(name)
    e = lit(email.lower())
    ph = digits(phone)

    last_like = lit(f"%{last.lower()}%") if last else NOMATCH
    email_like = lit(f"%{email.lower()}%")
    local_like = lit(f"%{email.lower().partition('@')[0]}%")
    phone_like = lit(f"%{ph}%") if ph else NOMATCH
    addr_like = lit(f"%{address.lower()}%") if address else NOMATCH

    # One regex alternation so a JSON blob is scanned once.
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
                   or {sql_phone_digits("phone")} like {phone_like}
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
                   or {sql_phone_digits("phone")} like {phone_like}
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
                   or {sql_phone_digits("properties_phone")} like {phone_like}
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
        *[
            (
                "A product",
                f"segment {schema}.{table}",
                f"select count(*) as n from segment_storage.{schema}.{table} where lower(coalesce({column},'')) = {e} having count(*) > 0",
            )
            for schema, table, column in SEGMENT_TABLES
        ],
        (
            "A product",
            "airbyte_source.amplitude_api_events",
            # user_id is the gp-api user id, never an email. The explicit id keeps this
            # probe working after the gp-api row is deleted.
            f"""select count(*) as n
                from {CATALOG}.airbyte_source.amplitude_api_events
                where user_id = {lit(gp_api_user_id)}
                   or user_id in (
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
            f"""select id, candidate_id as br_person_id, candidacy_id as br_candidacy_id,
                       first_name, last_name, email, phone, state
                from {CATALOG}.airbyte_source.ballotready_s3_candidacies_v3
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or {sql_phone_digits("phone")} like {phone_like}""",
        ),
        (
            "B vendor civic",
            # email and phone live inside the `contacts` array; match the serialized blob.
            "airbyte_source.ballotready_s3_office_holders_v3",
            f"""select id, candidate_id as br_person_id, candidacy_id as br_candidacy_id,
                       first_name, middle_name, last_name, nickname,
                       office_holder_mailing_address_line_1, contacts
                from {CATALOG}.airbyte_source.ballotready_s3_office_holders_v3
                where lower(coalesce(last_name,'')) like {last_like}
                   or lower(coalesce(nickname,'')) like {last_like}
                   or lower(coalesce(office_holder_mailing_address_line_1,'')) like {addr_like}
                   or lower(cast(contacts as string)) like {email_like}
                   or {sql_phone_digits("cast(contacts as string)")} like {phone_like}""",
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
                   or {sql_phone_digits("phone_clean")} like {phone_like}
                   or {sql_phone_digits("phone")} like {phone_like}
                   or lower(coalesce(street_address,'')) like {addr_like}""",
        ),
        (
            "B vendor civic",
            "airbyte_source.techspeed_gdrive_officeholders",
            f"""select first_name, last_name, email, phone, street_address, office_name
                from {CATALOG}.airbyte_source.techspeed_gdrive_officeholders
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or {sql_phone_digits("phone")} like {phone_like}
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
                   or {sql_phone_digits("phone")} like {phone_like}""",
        ),
        (
            "onward disclosure",
            "historical.ballotready_records_sent_to_techspeed",
            f"""select first_name, middle_name, last_name, nickname, email, phone, state, upload_datetime
                from {CATALOG}.historical.ballotready_records_sent_to_techspeed
                where lower(coalesce(email,'')) = {e}
                   or lower(coalesce(last_name,'')) like {last_like}
                   or {sql_phone_digits("phone")} like {phone_like}""",
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


def build_guard(email: str, phone: str) -> str:
    """Distinct people each proposed email and phone would suppress, per first-party source."""
    e = lit(email.lower())
    ph = digits(phone)
    ph_lit = lit(ph) if ph else NOMATCH
    sources = [
        ("gp_api_db_user", "id", "email", "phone"),
        (
            "hubspot_api_contacts",
            "id",
            "get_json_object(properties, '$.email')",
            "get_json_object(properties, '$.phone')",
        ),
    ]
    parts = [
        f"""select {lit(name)} as source,
                   count(distinct case when lower(trim({email_col})) = {e} then {id_col} end) as people_on_email,
                   count(distinct case when {sql_phone_digits(phone_col)} = {ph_lit} then {id_col} end) as people_on_phone
            from {CATALOG}.airbyte_source.{name}"""
        for name, id_col, email_col, phone_col in sources
    ]
    parts.append(
        f"""select 'ballotready_s3_office_holders_v3 (not filtered on phone; shown for scale)',
                   0,
                   count(distinct candidate_id)
            from {CATALOG}.airbyte_source.ballotready_s3_office_holders_v3
            where {sql_phone_digits("cast(contacts as string)")} like concat('%', {ph_lit}, '%')"""
    )
    return "\nunion all\n".join(parts)


def run(probes, dbc):
    hits, misses, errors = [], [], []
    slow: list[tuple[str, float]] = []
    for group, source, sql in probes:
        started = time.monotonic()
        try:
            df = dbc.run_query(sql)
        except Exception as exc:  # noqa: BLE001
            errors.append((source, str(exc)[:160]))
            continue
        elapsed = time.monotonic() - started
        if elapsed > 60:
            slow.append((source, elapsed))
        if len(df):
            hits.append((group, source, df))
        else:
            misses.append((group, source))
    return hits, misses, errors, slow


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help='Full name as given, e.g. "First Last"')
    parser.add_argument("--email", required=True)
    parser.add_argument("--phone", default="", help="Any format; digits are extracted")
    parser.add_argument("--address", default="", help='Street portion only, e.g. "123 Example St"')
    parser.add_argument(
        "--gp-api-user-id",
        default="",
        help="Numeric gp-api user id, once known. Keeps the Amplitude probe working after the user row is deleted.",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Also scan the airbyte_internal raw JSON. Slow; use when a hit is expected but not found.",
    )
    args = parser.parse_args()

    os.environ.setdefault("DATABRICKS_HTTP_PATH", DEFAULT_HTTP_PATH)
    import databricks_conn as dbc

    print(f"\nDSAR scope for {args.name} <{args.email}>\n" + "=" * 72)

    anchor_hits, _, anchor_errors, _ = run(build_anchors(args.name), dbc)
    print("\nANCHOR: exact surname match per source")
    if anchor_hits:
        df = anchor_hits[0][2]
        nonzero = df[df["n"] > 0]
        print(df.to_string(index=False))
        print(
            "\n  => "
            + ("EXACT MATCHES PRESENT, act on these" if len(nonzero) else "no exact surname anywhere")
        )
    for source, msg in anchor_errors:
        print(f"  anchor query failed: {msg}")

    probes = [
        p
        for p in build_probes(args.name, args.email, args.phone, args.address, args.gp_api_user_id)
        if args.deep or p[1] not in DEEP_ONLY
    ]
    hits, misses, errors, slow = run(probes, dbc)

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

    print("\nREGISTER GUARD: distinct people each proposed contact value would suppress")
    try:
        guard = dbc.run_query(build_guard(args.email, args.phone))
        print(guard.to_string(index=False))
        filtered = guard[~guard["source"].str.contains("not filtered")]
        shared = filtered[(filtered["people_on_email"] > 1) | (filtered["people_on_phone"] > 1)]
        if len(shared):
            print("\n  => SHARED VALUE. Do not register it; suppress this person through their ids.")
        else:
            print("\n  => each value identifies at most one person in the filtered sources")
    except Exception as exc:  # noqa: BLE001
        print(f"  guard query failed: {str(exc)[:160]}")

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
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>'),")
    print(f"    ('<TICKET>', {lit(args.name)}, 'hs_contact_id', '<id from the hubspot_api_contacts hit>',")
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>'),")
    print(f"    ('<TICKET>', {lit(args.name)}, 'br_person_id', '<br_person_id from a BallotReady hit>',")
    print("     date '<received>', date '<received + 45d>', '<note>', current_timestamp(), '<you>');")
    print("\nDrop the rows that do not apply. Presence in the table means suppress.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
