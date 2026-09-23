---
name: dsar-deletion
description: Process a data subject deletion request (CCPA right to be forgotten, GDPR erasure, DSAR) end to end. Given a person's contact details, sweeps every source that holds person-level data, records the identifiers in the suppression table, drives the source-system deletions in the order that stops re-ingestion, clears the warehouse copies, rebuilds downstream, and drafts the response. Use when handed a name, email or phone with a request to delete someone's data, when asked to scope what we hold about a person, or when working a DSAR / CCPA / right-to-be-forgotten ticket.
---

# DSAR deletion

Work a data subject deletion request from intake to response. The response deadline is
45 calendar days from receipt, so establish the receipt date before anything else.

## Scope decisions already settled

These came out of legal review on the first request. Do not re-open them per request.

- Logical deletion satisfies the statute. Do not purge or vacuum, and do not rewrite
  vendor files in S3.
- The L2 voter file is out of scope. We cannot delete from a third party's file and it
  re-ingests on every state refresh. Do not sweep it, do not suppress from it, and do not
  record a subject-to-`LALVOTERID` link anywhere.
- Vendor civic records (BallotReady, DDHQ, TechSpeed) are public records outside our
  control. We filter them at staging rather than chasing deletes through the vendor.
- Stripe rows stay for financial recordkeeping. Anonymize the name, email and billing
  address; keep the transaction.
- No third-party notification is required.
- The request record itself is retained for 24 months as evidence. Never delete it.

## The suppression table

`goodparty_data_catalog.source_dsar.suppressed_identifiers`, one row per identifier per
request. Hand-curated by data engineers with `insert into` and `delete`; dbt reads it and
never writes it.

Presence means suppress. There is no flag. If a record is found but deliberately left
alone, document that on the ticket and leave it out of the table.

Staging models apply it with the `dsar_not_suppressed(column, identifier_type)` macro,
which owns the normalization. The accepted identifier types are the ones some filter
matches on; a type nothing filters is rejected by a dbt test, because a row of that type
would silently do nothing. The table's CHECK constraints reject blank, unnormalized and
unknown values.

Never put an identifier in the repo: no seed, fixture, test, comment or documentation
example. This repo is public.

## Step 0: intake

Confirm the requester was verified. CCPA requires it, and support owns it. Record how it
was verified on the ticket.

Collect the receipt date, the name, and every contact detail given. Compute respond-by as
receipt plus 45 calendar days.

Read the whole HubSpot conversation, not only the first message. The warehouse copy of
`airbyte_source.hubspot_api_tickets` holds only the opening message, and a later message
may widen the request (a profile removal that becomes a full deletion) or add identifiers.
The ClickUp custom field naming the HubSpot ticket has been wrong before, so match on
content and correct the field if it disagrees.

## Step 1: scope

```bash
cd analytics && uv run python ../.claude/skills/dsar-deletion/scope_subject.py \
    --name "First Last" --email someone@example.com --phone 555-555-5555 \
    --address "123 Example St"
```

Pass `--address` whenever the request includes one. A vendor or lead record often carries
the street address when it carries nothing else you can match on.

Read the ANCHOR block first. It reports exact-surname counts per person-bearing source;
any non-zero is a record to act on. Then read the fuzzy hits. Surnames are matched by
substring, so hyphenated and mis-split names still land, at the cost of false positives
(a search for "Mboh" also returns "Schlumbohm"). Read the row before treating a fuzzy hit
as a match.

Rerun the sweep with every identifier the first pass surfaces. A vendor record often
carries a different email or phone than the request, and those need registering too.

The sweep reads raw sources, never the filtered staging models, so it reports what we
hold regardless of who is already suppressed. Probes that error are not clear; check them
by hand. `--deep` adds the `airbyte_internal` raw JSON, which is slow enough to be off by
default.

The two `historical.ballotready_records_sent_to_*` probes are worth reading even on a
clean sweep: they record what we sent onward to HubSpot and TechSpeed.

## Step 2: record the identifiers

Insert one row per identifier the sweep surfaced. The sweep prints an insert template
with the columns filled in.

The rule: match on the identifier that belongs to exactly one person in that source.
First-party sources, where an email or phone is the person's own, check their record id
and the personal contact fields. Vendor civic records check the vendor's id only. In the
BallotReady officeholder file a phone is shared by two or more people a third of the
time, and one switchboard number is shared by 287 officeholders.

Which type each source is filtered on:

- `gp_api_user_id`: gp-api users and campaigns, Amplitude events. Segment is not filtered
  in the warehouse; its control is suppression at Segment (Step 3).
- `hs_contact_id`: HubSpot contacts, the contacts archive, feedback submissions.
- `email`, `phone`: gp-api users and the HubSpot contacts, companies, calls, feedback
  submissions and archive models. First-party only.
- `br_person_id`, `br_candidacy_id`: BallotReady candidacies and office holders, the
  Airflow BallotReady person feed, and both TechSpeed feeds, which resolve through
  BallotReady (officeholders by office holder id, candidates by race plus name).
- `ddhq_candidate_id`: DDHQ election results, valid and invalid.

Record the BallotReady ids whenever the sweep finds a BallotReady record; they are the
only handle the filters have on vendor civic data. The person id covers every candidacy
and term, a candidacy id covers that one.

Never register a shared value. The sweep ends with a register guard that counts how many
distinct people each proposed email and phone matches in gp-api and HubSpot. Above one is
a campaign inbox or an office line; suppress that person through their ids instead. A
warn-level dbt test repeats the check nightly.

Phones are stored as digits with any leading US country code dropped. The filters
normalize both sides the same way, so `+1 (202) 555-0100` in a source still matches
`2025550100` in the register.

## Step 3: delete at the sources, in this order

Order matters. Clearing a warehouse copy before its source means the next sync restores it.

1. HubSpot. Confirm the person is excluded from `mart_sales_reverse_etl.candidacy_hubspot`
   and `candidacy_techspeed` first, or the reverse-ETL recreates them. Then use the GDPR
   delete endpoint, which blocks recreation with the same email.
2. gp-api. Run the admin delete flow (Postgres row, Clerk user, Stripe subscription).
   Then check the tables with no foreign key to `user` by hand: `campaign_plan_version`,
   `chat_message`, `website_contact`, `poll_individual_message`, `voter_file_filter`.
3. Clerk. Confirm the delete landed rather than assuming the cascade worked.
4. Stripe. Anonymize the customer, charge and invoice records. Keep the rows.
5. Segment. Deletion plus suppression API. Suppression is the part that matters, because
   the warehouse sync repopulates `segment_storage` otherwise.
6. Amplitude. User privacy deletion API. This does not touch our copy.
7. Files we own. Redact their rows from the DDHQ `goodparty_ddhq_master.csv` in Drive,
   the TechSpeed Drive CSVs, and `s3://goodparty-external-data-share/incoming/techspeed/`.
   Leave the BallotReady S3 drops alone; the staging filter covers them.

## Step 4: what the warehouse keeps

The staging filter is the control. Once the identifiers are registered, every model from
staging down excludes the person on its next build, and a source that re-ingests them
cannot bring them back. We do not delete rows from the warehouse copies of source
systems: they re-ingest, logical deletion meets the statute, and purpose limitation keeps
them from being read.

Copies upstream of the filter that still hold the rows:

- `airbyte_source.*` landing tables and the insert-only `airbyte_internal` raw JSON. The
  gp-api source replicates by Xmin, not CDC, so a Postgres delete never reaches them.
- `stg_airflow_source__ballotready_person_raw`, an incremental merge. Its filter stops new
  rows and the downstream model re-filters, but a row already merged stays until a
  `--full-refresh` of that model.
- `snapshot__hubspot_api_*` and `snapshot__m_election_api__person`. A snapshot closes a
  row rather than deleting it.
- `archives.airbyte_source__hubspot_api_*_20260122`, `historical.ballotready_records_sent_to_*`,
  `model_predictions.candidacy_ddhq_matches_*`.

Everything from staging down, including PR and dev schemas, is a derived copy that the
next rebuild clears. Do not chase those by hand.

## Step 5: rebuild downstream, in this order

Entity resolution sits in the middle of the chain: matcha reads the `int__er_prematch_*`
models and writes `er_source.*`, which the person and candidacy marts read back. Rebuild
the marts before matcha has rerun and they carry the person from the previous clustering.

1. `dbt build` the `int__er_prematch_*` models.
2. Rerun matcha, so `er_source.*` regenerates without the person.
3. `dbt build --full-refresh` on the affected selectors downstream, which clears
   `mart_civics` and `mart_analytics`.
4. Run the `sync_election_api` DAG. It rebuilds all 13 tables and swaps set-wise, so
   excluding the person from `m_election_api__person` removes them from the serving
   database.

## Step 6: verify

Rerun the sweep and expect the same hits to be gone. Pass `--gp-api-user-id` when the
subject had a gp-api account, because the Amplitude probe otherwise resolves the id from
the user row that Step 3 deleted. Then check the serving systems directly rather than the
marts: election-api Postgres, people-db, and gp-api.

## Step 7: respond and close

Respond before the respond-by date. Say what was deleted by category, and say plainly
what was retained and why: Stripe records under financial recordkeeping, publicly
available candidacy and officeholder records, third-party voter file data we do not
control, and security and audit logs.

Keep the request record. Close the ticket and update the linked HubSpot ticket.

## Gotchas

- A person can hold several identifiers of the same type. HubSpot merges mint a new
  contact id, so a subject may have more than one. Insert a row for each.
- `dbt_cloud` needs SELECT on `source_dsar` for the staging filter to run. It has
  catalog-wide SELECT today, so tightening that grant would break the filter quietly.
- Purpose limitation is the real compliance risk. The anti-join in staging is a permitted
  use. A `left join` that adds a "requested deletion" flag to a user table is not. Never
  join this table into a mart or expose it in a BI tool.
- Deleting a vendor civic record removes the person from opponent research and contrast
  generation for their race. Confirm with product before filtering a candidate or
  officeholder.
- Two people with the same first and last name in the same BallotReady race cannot be
  told apart by the TechSpeed candidate filter. Accepted; note it on the ticket if it
  happens.
