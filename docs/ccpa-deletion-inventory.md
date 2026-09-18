# Dataplatform DSAR deletion inventory

Below is an inventory of every place an individual's data lands in Data
Platform, for consideration when handling Data Subject Acess Request (DSAR)
deletions, including CCPA. This is inclusive of all "people", not strictly
GoodParty.org users.

## Process notes

The gp-api Airbyte source currently uses `Xmin` replication (not CDC), which
means hard deletes aren't transparently propagated. We need to run manual
deletes against the user table in Databricks.

The process for deleting gp_api records should be:

1. Delete in Postgres (stops the row being re-sent, and satisfies the product side).
2. Remove the row from the Airbyte landing table and its `airbyte_internal` raw table,
   either by clearing connection state and resyncing the stream, or by a direct
   `DELETE` in Databricks.
3. Then `dbt build --full-refresh` on the affected selectors to purge downstream.


## Group A: product data

Clear obligation since this is data the person gave us:

| Where | What | Find by | Delete | Comes back? |
|---|---|---|---|---|
| gp-api Postgres (`gpdb`, prod cluster) | `user`, plus `campaign`, `campaign_plan`, `campaign_plan_version`, `ai_chat`, `chat_conversation`, `chat_message`, `annotation*`, `artifact_feedback`, `outreach`, `poll`, `poll_individual_message`, `website`, `website_contact`, `website_view`, `tcr_compliance`, `elected_office`, `organization`, `ecanvasser*`, `meeting_briefing`, `voter_file_filter` | `user.id`, `user.email`, `user.clerk_id` | Product-side delete. Needs a cascade review: not every table has an FK to `user`. | No, once removed at source |
| Databricks `airbyte_source.gp_api_db_*` | 36 incrementally replicated streams, synced hourly | `id`, `email` | See mechanic 1 above | Only if the Postgres row still exists |
| Databricks `airbyte_internal.airbyte_source_raw__stream_gp_api_db_*` | Insert-only raw JSON of every version ever extracted. 159 raw tables across all sources. | JSON payload search | Explicit `DELETE`. A stream resync does not necessarily clear prior generations. | No |
| Databricks marts | `mart_civics.users`, `campaigns`, `people`, `candidate`, `candidacy`, `elected_officials`; `mart_analytics.users_win_base`, `users_serve_base`, `users_win_candidacy`, `users_serve_activity`, `leads_win_candidacy`, `prospects`, `pmf_survey_responses`, `win_user_satisfaction_responses` | `gp_api_user_id`, email | Falls out of a full refresh once upstream is clean | No |
| election-api Postgres (prod) | `Person`, `Candidacy`, `OfficeHolder`, `Stance` | `Person.slug`, `Person.gp_api_user_id` | Nothing to do directly. The `sync_election_api` DAG rebuilds all 13 tables and swaps them set-wise, so removing the person from `m_election_api__person` removes them from election-api on the next run. | No, as long as the mart excludes them |
| Segment (`segment_storage` catalog, `gp_api` and `web_app` schemas) | `identifies`, `tracks`, `users`, plus ~12 named event tables per source. Behavioral history keyed to the user. | `user_id`, `anonymous_id`, email trait | Segment has a deletion and suppression API. This catalog is written by Segment's own warehouse sync, not Airbyte, so it will be repopulated unless suppression is set at Segment. | Yes, without suppression |
| Amplitude (`airbyte_source.amplitude_api_events`) | Event stream keyed on `user_id` and device id | `user_id` | Amplitude has a user privacy deletion API. Deleting in Amplitude does not clean our copy; the copy needs its own delete. | Yes, in our copy, unless deleted there too |
| Stripe (`dbt_stripe.*`, `airbyte_source.stripe_api_*`) | `customers`, `charges`, `invoices`, `persons`. Name, email, billing address. | Stripe customer id, email | Possibly(?) a retention exemption. See open questions. | n/a |
| ClickUp (`airbyte_source.clickup_*`) | Support and ops tasks. May quote the person's name or email in task text or comments. | Free-text search on `clickup_task`, `list_comments` | Manual. Low volume. | Yes, if the task still exists |

## Group B: third-party civic records

This is the ambiguous group. It is public-records data about candidates and
officeholders, obtained from vendors.

Note that the sources pulled from the CivicEngine GraphQL API connection does
not carry person names. It ingests `election`, `position`, `position_to_place`,
`race`, `place`, `issue`, and `mtfcc`. `race.candidacies` is an array of ids
only. The email and phone fields on `place` and `position` are the election
office's contact details and filing office details, not an individual's. So
re-querying the CivicEngine API cannot reintroduce this person.

All BallotReady person-level data reaches us through the S3 CSV drops instead.

| Where | What | Find by | Delete | Comes back? |
|---|---|---|---|---|
| `s3://goodparty-ballotready/` | `candidacies_v3_*.csv`, `office_holders_v3_*.csv`, `recruitment_v1_*.csv`. Vendor drops, one file per delivery, all vintages retained. | Name search in CSV | Vendor-supplied files. Deleting our copies is possible but the next drop reintroduces the record. | Yes, weekly. The connection runs five times every Monday. |
| `airbyte_source.ballotready_s3_candidacies_v3`, `..._office_holders_v3`, `..._recruitment_v1` | Candidate and officeholder records with names, and contact details on some rows | `id`, name | `DELETE` in Databricks | Yes, on the next Monday sync |
| `airbyte_source.techspeed_gdrive_candidates`, `..._officeholders`, `..._marketing_data_enrichment` | Contact enrichment. This is the most sensitive part of group B because enrichment adds personal contact details that are not in the public record. | Name, email | `DELETE`, plus remove the source CSV | Yes. The GDrive connection runs twice daily and is `incremental_append`, so every historical file row is retained. |
| Google Drive folder `1LkfBb77fpxrJ-PL8QvWbISyWCW9PHmUm` | TechSpeed candidate and officeholder CSVs, `*_clean_utf8_nobom.csv` | Filename plus row search | Delete or redact rows in the Drive files | No, once TechSpeed stops sending them |
| Google Drive folder `1vz1o0JYF-ZikQlj650Cxiz8x9Ka4qIiX` | DDHQ election results, `goodparty_ddhq_YYYYMMDD.csv` per delivery plus `goodparty_ddhq_master.csv` | Candidate name | Delete or redact rows. This is the source you asked about; the files are ours to edit in our Drive. | No, if DDHQ stops sending the record. The master file is the one that matters most since it is re-read on every sync. |
| `airbyte_source.ddhq_gdrive_election_results`, `..._master` | Election results with candidate names and vote counts | `candidate_id`, name | `DELETE` | Yes, from the Drive files, daily |
| `airbyte_source.ddhq_elections_gsheet_*` | Race-level only, keyed on `race_id`. No individuals. | n/a | Nothing to do | n/a |
| `s3://goodparty-external-data-share/incoming/techspeed/candidates_*.csv` | TechSpeed drops. No active Airbyte connection reads this today. | Name | Delete files | No |
| `er_source.*` (6 tables) | Splink entity resolution output. Names, and the pairwise tables hold name comparison features. | Name, `br_candidate_id`, cluster id | Regenerated by the matcha container, so it needs the inputs clean first, then a rerun | Yes, on the next matcha run |
| `model_predictions.candidacy_ddhq_matches_*` (6 dated tables) | Gemini name-matching output between DDHQ and candidacies | Name | `DELETE`. These are frozen one-time loads, so no rerun risk. | No |
| `historical.ballotready_records_sent_to_hubspot`, `..._sent_to_techspeed` | Log of which records we disclosed to HubSpot and TechSpeed | Name, id | `DELETE`. Worth reading first: it tells you whether this person's data was sent onward, which affects who else has to be notified. | No |
| `exports_zapier.*` | Zapier-facing candidate views | Name | Rebuild after upstream is clean | No |

**Product implication:** Deleting a BallotReady or DDHQ record removes
the person from `m_election_api__person`, `m_election_api__candidacy`, and
`m_election_api__office_holder`, which is what campaign plan generation reads. The
visible effects are: the person disappears from opponent research and contrast
generation for their race, their race may show fewer or no opponents, and if they hold
office, elected-official surfaces lose them.

For very few deletes, this is *probably* an acceptable product impact, so the
consideration is likely more around necessity to delete from Group B entirely.

## Group C: L2 voter file

| Where | What | Find by | Delete | Comes back? |
|---|---|---|---|---|
| `s3://goodparty-databricks-workspace/l2_data/from_sftp_server/VMFiles/prod/<STATE>/` | Raw `VM2--<ST>--<date>-DEMOGRAPHIC.tab` and `-VOTEHISTORY.tab`. Every vintage is retained. Alabama alone has deliveries from 2025-05-20 onward at roughly 7 GB each. | `LALVOTERID` | Rewriting multi-GB tab files to remove one row is impractical. Realistically this is a suppression problem, not a deletion problem. | Yes, on every refresh |
| `dbt_source.l2_s3_<state>_demographic`, `..._uniform`, `..._vote_history` (about 150 state tables) | Name, address, phone, and modeled demographics | `LALVOTERID` | `DELETE`, but these are rebuilt from S3 | Yes |
| `dbt.snapshot__int__l2_nationwide_uniform` | SCD2 history of the national uniform table, keyed on `LALVOTERID`, `check_cols` includes name and address fields. Every past version of the record. | `LALVOTERID` | `DELETE`. A `dbt snapshot --full-refresh` would drop all history for everyone, which is not what you want. | No, once the upstream row is suppressed |
| gp-api voter Postgres, `Voter<STATE>` tables | Per-state voter tables written by `write__l2_databricks_to_gp_api` (still enabled). Names, addresses, phones. | `LALVOTERID` | `DELETE`. The writer upserts, so it will not remove a row on its own. | Yes, if still in Databricks |
| people-api Postgres (dated RDS clusters) | Full voter serving tables, loaded by `people-api-loader` | `LALVOTERID` | `DELETE` on the live cluster | Yes, the next monthly load rebuilds from Databricks |
| `s3://gp-people-loader-us-west-2/voter_export_<date>/` | Voter extracts staged for the Postgres COPY. `teardown` deletes the prefix, so check whether it ran for each past date. | `LALVOTERID` | Delete leftover prefixes | No |
| RDS snapshots `gp-people-db-<date>-<env>-final`, plus 14-day automated backups | Full voter data | n/a | Delete manual snapshots. Automated backups age out in 14 days. | n/a |
| `mart_mban2026.deid_voters` | Already de-identified: names and contact removed, addresses hashed, `LALVOTERID` retained for linkage | `LALVOTERID` | Retaining `LALVOTERID` means it is pseudonymized rather than anonymized, so it probably still counts as personal information. Worth a legal read. | Rebuilt monthly |
| `m_people_api__voter`, `m_people_api__districtvoter`, `int__l2_nationwide_uniform*`, `int__voter_turnout_inference`, `model_predictions.voter_turnout_scores_20260730` | Voter-level rows and per-voter turnout scores | `LALVOTERID` | Full refresh once upstream is clean | No |

**There is a half-built suppression pattern to copy.** We currently ingest the
expired/to-delete voters passed along by L2, but don't perform any hard deletes
on them yet. Note that these are separate from GoodParty-initiated delete
requests, but we can probably use the same mechanism **if** we determine we should
delete these records.

If so, we'll need to rerun deletes on an ongoing basis to prevent reingesting
affected records.

## Group D: copies, history, and backups

- dbt PR review schemas (either orphaned or in-flight). We should probably just
clean these all up and implement a fail safe scheduled "tidy" job going forward.
- Personal dbt dev schemas: `dbt_dball`, `dbt_hugh`, `dbt_hugh_source`, et.c
- `private_alex_day`, `private_amanda`, `private_nigel`, `private_nikao`,
  `private_samuel`, `private_tristan`.
- `dbt_staging`, `dbt_staging_source`, `dbt_preview`, `sandbox`, `models_mban`.

**dbt snapshots.** Five snapshot tables in the `dbt` schema, with duplicates in
`dbt_staging` and various PR schemas:

- `snapshot__hubspot_api_contacts`, `..._companies`, `..._deals`. These retain every
  past version of a HubSpot contact. Deleting the contact in HubSpot does not touch
  them, and because the strategy is `timestamp` on `updatedAt`, a deleted contact just
  stops updating.
- `snapshot__int__civics_person_canonical_ids`. Person id mint history, `hard_deletes:
  invalidate`, so removal is recorded rather than erased.
- `snapshot__int__l2_nationwide_uniform`. Covered in group C, but this deletion
will need to be handled separately (or we just drop the whole table)

**`archives` schema.** `airbyte_source__hubspot_api_contacts_20260122`,
`..._companies_20260122`, `..._engagements_20260122`. Point-in-time HubSpot copies from
January.

**Delta deletion vectors.** `airbyte_source` tables have
`delta.enableDeletionVectors: true` and no `delta.deletedFileRetentionDuration`
override, so the default 7 days applies. A `DELETE` writes a deletion vector and leaves
the original Parquet in
`s3://goodparty-warehouse-databricks/goodparty_data_catalog/__unitystorage/`. The bytes
are still there and still readable via time travel.

**If we decide we need to physically purge these**, we need to run a `REORG TABLE
<table> APPLY (PURGE)` to rewrite the affected files, then a `VACUUM` whose retention
window has actually elapsed. Note that `VACUUM` only removes files older than the
retention period, so a plain `VACUUM` right after the purge removes nothing new. Either
wait out the 7-day default, or lower `delta.deletedFileRetentionDuration` on the table
first. Forcing `RETAIN 0 HOURS` requires disabling the retention safety check, which
risks breaking concurrent readers, so it should not be the default approach.

Again, whether CCPA requires physical erasure or accepts logical deletion is a
compliance question.

**Other copies to check:** Databricks Delta time travel on every touched table, dbt
Cloud run artifacts and logs, Airflow task logs on Astronomer (the loader logs row
counts, not rows, but worth confirming), and `sigma_writeback` plus
`temp_file_storage` schemas. **This may be exempt from CCPA as "write-only logs"
that may be kept for compliance and securtiy purposes.**

## Group E: external processors

An open question is whether the request has to be forwarded to anyone we sent
the data to. This may include TechSpeed, Clerk, Stripe, Peerly, etc.

**HubSpot.** In either case, we should delete in HubSpot first. HubSpot has a
GDPR delete endpoint that permanently deletes and blocks re-creation with the same email.
This is probably helpful since our sales reverse-ETL feeds
(`mart_sales_reverse_etl.candidacy_hubspot`, `candidacy_techspeed`) push
candidate records back into HubSpot.

The order for HubSpot deletes should be:

1. Confirm the person is excluded from `candidacy_hubspot` and `candidacy_techspeed` in Databricks
2. GDPR-delete in HubSpot.
3. Clear our copies: `airbyte_source.hubspot_api_contacts`, the three snapshots, the
   three `archives` tables, and `airbyte_internal` raws.

**Clerk** holds the auth identity

**Peerly and Ecanvasser** hold voter contact data, not just user data, so they matter
for group C as well.

## Ongoing re-delete

Sources that could reintroduce a deleted record on their next run:

1. TechSpeed GDrive
2. DDHQ GDrive: daily, re-reads the master file.
3. HubSpot: daily, unless GDPR-deleted.
4. Segment and Amplitude: daily, unless suppressed at source.
5. BallotReady S3
6. L2: on each state refresh, roughly monthly.
7. matcha ER and people-api loader: on their own schedules, downstream of the above.

Because the of above, we should add a standing suppression list of identifiers
(hashed email, gp_user_id, hashed voter ID, etc.) and delete from those sources
on an ongoing basis. We can probably use the same pattern as an L2 expired
voters job, but this is new work.

## Open questions

1. Does a third-party public-records exemption cover the BallotReady, DDHQ, and
   TechSpeed candidate and officeholder data? Candidacy for public office is a public
   record, but TechSpeed enrichment adds contact details that are not, so the answer may
   differ by source.
2. Does it matter that the requester is a product user as well as a subject in the
   public civic data? If those are the same person, is the civic record still exempt?
3. Do the Stripe financial records fall under a retention exemption?
4. Does compliance require physical erasure, or is logical deletion enough? This decides
   whether we need to delete underlying Databricks data on S3 and how we handle
   the multi-GB L2 `.tab` vintages in S3 that are impractical to rewrite.
5. Is a suppression list that we purge weekly acceptable as the ongoing control for reingested data?
6. What is the deadline?
7. Do we need to notify TechSpeed or other downstream 3rd parties as recipients
