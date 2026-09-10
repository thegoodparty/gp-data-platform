---
name: dsar-deletion
description: Process a data subject deletion request (CCPA right to be forgotten, GDPR erasure, DSAR) end to end. Given a person's contact details, sweeps every source that holds person-level data, records the identifiers in the suppression table, drives the source-system deletions in the order that stops re-ingestion, clears the warehouse copies, rebuilds downstream, and drafts the response. Use when handed a name, email or phone with a request to delete someone's data, when asked to scope what we hold about a person, or when working a DSAR / CCPA / right-to-be-forgotten ticket.
---

# DSAR deletion

Work a data subject deletion request from intake to response. The response deadline is
**45 calendar days from receipt**, so establish the receipt date before anything else.

## Scope decisions already settled

These came out of legal review on the first request. Do not re-open them per request.

- **Logical deletion satisfies the statute.** A Delta `DELETE` is enough. Do not run
  `REORG ... APPLY (PURGE)` or `VACUUM`, and do not try to rewrite vendor files in S3.
- **The L2 voter file is out of scope.** We cannot delete from a third party's file and it
  re-ingests on every state refresh. Do not sweep it, do not suppress from it, and do not
  record a subject-to-`LALVOTERID` link anywhere. An identifier we never filter on is
  personal information retained for no permitted purpose.
- **Vendor civic records are treated as public records.** BallotReady, DDHQ and TechSpeed
  candidate and officeholder data is outside our control. We filter it at staging rather
  than chasing deletes through the vendor.
- **Stripe rows stay.** Financial recordkeeping. Anonymize the name, email and billing
  address; keep the transaction.
- **No third-party notification is required.** We do not have to forward the request to
  TechSpeed, Clerk, Peerly or anyone else we sent data to.
- **The request record itself is retained for 24 months.** The support ticket quoting the
  person's details is required evidence, not a leftover. Never delete it to be thorough.

## The suppression table

`goodparty_data_catalog.source_dsar.suppressed_identifiers`. Hand-curated by data
engineers with `insert into` and `delete`. dbt reads it as a source and never writes it.

Grain is one row per identifier per request:
`request_id, subject_name, identifier_type, identifier_value, received_at, respond_by,
notes, created_at, created_by`.

**Presence means suppress.** There is no flag. Everything in the table gets filtered,
unconditionally. If a record is found but deliberately left alone, document that on the
ticket and leave it out of the table.

dbt reads the table through the `source_dsar` source and the
`stg_source_dsar__suppressed_identifiers` staging model, which exposes only the
identifier columns so the requester's name is not copied into a wider schema. Staging
models apply it with the `dsar_not_suppressed(column, identifier_type)` macro, which
owns the normalization so no caller can match the register a different way.

Three check constraints reject bad inserts: `identifier_type_known` (11 allowed types,
with `lalvoterid` deliberately excluded because L2 is out of scope),
`email_normalized` (lowercased and trimmed), `phone_digits_only` (10 to 15 digits, no
punctuation). Unity Catalog does not enforce uniqueness, so duplicates are caught by a dbt
test rather than the table.

**Never put an identifier in the repo.** No dbt seed, no fixture, no test, no comment, no
example in documentation. This repo is public. Identifiers live in Databricks only.

## Step 0: intake

Confirm the requester was verified. CCPA requires it, and support owns it. Record how it
was verified on the ticket.

Collect the receipt date, the name, and every contact detail given. Compute respond-by as
receipt plus 45 calendar days.

Find the source ticket in `airbyte_source.hubspot_api_tickets`. The ClickUp custom field
naming the HubSpot ticket has been wrong before, so match on content rather than trusting
it, and correct the field if it disagrees.

## Step 1: scope

```bash
cd analytics && uv run python ../.claude/skills/dsar-deletion/scope_subject.py \
    --name "First Last" --email someone@example.com --phone 555-555-5555 \
    --address "123 Example St"
```

Pass `--address` whenever the request includes one. A vendor or lead record often carries
the street address when it carries nothing else you can match on.

The output has two parts, and they answer different questions.

**Read the ANCHOR block first.** It reports exact-surname counts across the eleven
person-bearing sources. Zero everywhere is a clean negative. Any non-zero is a record to
act on.

**Then read the fuzzy hits.** Surnames are matched by substring, so "First M Last", a
hyphenated surname, and a middle name sitting in the first-name field all still land.
The cost is false positives: searching for "Mboh" also returns "Schlumbohm" and
"Bohnenkamp", because the substring appears inside those names. Eyeball them; do not
treat a fuzzy hit as a match without reading the row.

Both matter. The anchor alone would miss a misspelling or a name split across the wrong
fields. The fuzzy pass alone buries a real answer in noise.

Coverage is roughly 25 probes: product data, HubSpot contacts and companies including
the archive and snapshot copies, Segment, Amplitude, Stripe, ClickUp, the vendor civic
sources, the BallotReady GraphQL person payloads, the entity-resolution cluster tables,
and the two `historical.ballotready_records_sent_to_*` disclosure logs. Those last two
are worth reading even on a clean sweep: they record what we sent onward to HubSpot and
TechSpeed, which is what tells you whether anyone else received the person's data.

`--deep` adds the `airbyte_internal` raw JSON, which holds every version ever extracted.
It is slow enough to exhaust the connector's retry budget on the X-Small warehouse, so it
is off by default. Use it when you expect a hit and the normal sweep does not find one.

Probes that error are not clear. Check them by hand before concluding anything. Probes
taking over 60 seconds are listed separately so a slow source is visible rather than
silently near timeout.

Three source families are listed as not probed because they have no person-level columns
at all: `ballotready_s3_recruitment_v1` is race and position level, the DDHQ gsheet
tables are keyed on `race_id`, and the CivicEngine GraphQL tables carry no person names.
They are reported so a reader can see they were considered rather than forgotten.

If the subject appears in `mart_civics.people`, the sweep also returns their
`mart_civics.person_identifiers` rows, one per contributing source record, which tells
you which vendors hold them. That view only reflects what entity resolution clustered, so
a record the matcher missed will not appear. Trust the sweep over the view for negatives.

## Step 2: record the identifiers

Insert one row per identifier the sweep surfaced. Email and phone are worth recording
even when nothing matches them today, because they are the standing guard if a vendor
delivers the person later.

**Record every identifier type the filters key on, not just the email.** Each staging
filter matches one type, so an identifier you leave out is a filter that silently passes
everyone through. In particular Amplitude keys on `gp_api_user_id`, because its `user_id`
column is the gp-api user id and never an email address. If the subject has a gp-api
account and you record only their email, Amplitude events are not suppressed.

```sql
insert into goodparty_data_catalog.source_dsar.suppressed_identifiers
    (request_id, subject_name, identifier_type, identifier_value,
     received_at, respond_by, notes, created_at, created_by)
values ('DATA-XXXX', '<name>', 'email', '<lowercased email>',
        date '<received>', date '<received + 45d>', '<why>',
        current_timestamp(), '<you>'),
       ('DATA-XXXX', '<name>', 'phone', '<digits only>',
        date '<received>', date '<received + 45d>', '<why>',
        current_timestamp(), '<you>'),
       ('DATA-XXXX', '<name>', 'gp_api_user_id', '<numeric id from the sweep>',
        date '<received>', date '<received + 45d>', '<why>',
        current_timestamp(), '<you>');
```

Which type each source is filtered on:

| identifier_type | filters |
|---|---|
| `email` | gp-api users, HubSpot contacts and companies, HubSpot archive models, BallotReady candidacies and office holders, TechSpeed candidates and officeholders |
| `phone` | the same set |
| `gp_api_user_id` | Amplitude events |
| `ddhq_candidate_id` | DDHQ election results |

Drop the rows that do not apply. A subject with no gp-api account has no
`gp_api_user_id` to record, and the constraints will reject a blank one.

## Step 3: delete at the sources, in this order

Order matters. Clearing a warehouse copy before its source means the next sync restores it.

1. **HubSpot.** Confirm the person is excluded from `mart_sales_reverse_etl.candidacy_hubspot`
   and `candidacy_techspeed` first, or the reverse-ETL recreates them. Then use the GDPR
   delete endpoint, which permanently deletes and blocks recreation with the same email.
2. **gp-api.** Run the admin delete flow. It removes the Postgres row in a transaction,
   deletes the Clerk user, and cancels the Stripe subscription. Then check the tables with
   no foreign key to `user` by hand: `campaign_plan_version`, `chat_message`,
   `website_contact`, `poll_individual_message`, `voter_file_filter`.
3. **Clerk.** Confirm the delete landed rather than assuming the cascade worked.
4. **Stripe.** Anonymize the customer, charge and invoice records. Keep the rows.
5. **Segment.** Deletion plus suppression API. Suppression is the part that matters,
   because the warehouse sync repopulates `segment_storage` otherwise.
6. **Amplitude.** User privacy deletion API. This does not touch our copy.
7. **Files we own.** Redact their rows from the DDHQ `goodparty_ddhq_master.csv` in Drive,
   the TechSpeed Drive CSVs, and `s3://goodparty-external-data-share/incoming/techspeed/`.
   Leave the BallotReady S3 drops alone; the staging filter covers both
   `candidacies_v3` and `office_holders_v3`.

## Step 4: clear the warehouse copies

The gp-api Airbyte source replicates by Xmin, not CDC, so a Postgres delete never reaches
Databricks. There is no `_ab_cdc_deleted_at` column. The row simply stops updating and
persists forever, and `dbt build --full-refresh` faithfully reproduces it. Every one of
these needs an explicit `DELETE`:

- `airbyte_source.gp_api_db_*`, all 36 person-bearing streams
- `airbyte_internal.airbyte_source_raw__stream_*`, insert-only raw JSON of every version
  ever extracted. A stream resync does not clear prior generations.
- `segment_storage` tables in the `gp_api` and `web_app` schemas
- `airbyte_source.amplitude_api_events`
- `airbyte_source.hubspot_api_contacts`, the three `snapshot__hubspot_api_*` tables, and
  the three `archives.airbyte_source__hubspot_api_*_20260122` tables
- `historical.ballotready_records_sent_to_hubspot` and `..._sent_to_techspeed`. Read these
  first; they record what we disclosed onward.
- `model_predictions.candidacy_ddhq_matches_*`

Stop there. No purge, no vacuum.

## Step 5: rebuild downstream

- `dbt build --full-refresh` on the affected selectors, which clears `mart_civics` and
  `mart_analytics`.
- Run the `sync_election_api` DAG. It rebuilds all 13 tables and swaps set-wise, so
  excluding the person from `m_election_api__person` removes them from the serving
  database with no direct delete.
- Rerun matcha so `er_source.*` regenerates from clean inputs.

## Step 6: verify

Rerun the sweep and expect the same hits to be gone. Then check the serving systems
directly rather than the marts, because a clean mart does not prove a clean serving copy:
election-api Postgres, people-db, and gp-api.

## Step 7: respond and close

Respond before the respond-by date. Say what was deleted by category, and say plainly what
was retained and why: Stripe records under financial recordkeeping, publicly available
candidacy and officeholder records, third-party voter file data we do not control, and
security and audit logs.

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
