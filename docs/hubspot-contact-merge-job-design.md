# HubSpot contact merge job: design

## Why

The HubSpot Data Flow TDD commits Data Platform to an automated nightly job that merges
duplicate HubSpot Contacts (Workstream B, item 8). The merge candidates already exist as a
dbt mart (`hubspot_contact_merge_candidates`), which ranks a person's live contacts and
names one primary. Nothing consumes it yet. Today a human reads reviewed pairs and runs
`reverse-etl/probes/merge_pairs.py` by hand.

This document covers the job that consumes those pairs, and the dbt changes around it. It
is deliberately narrow.

## Scope

In scope: HubSpot Contacts only. The job takes the object type as configuration and records
it in the log, so a later increment can add Companies without a schema change. Until
then, the job rejects any type other than `contacts`.

Out of scope, each a later increment: Companies, the Office Holder custom object, a
merge-candidate review UI, and changes to the person graph itself.

## Goals

- Merge the pairs in a dbt relation through HubSpot's merge endpoint, unattended, on a
  schedule.
- Be safe to rerun at any point. A rerun after a crash completes the remaining pairs, and a
  pair that is already merged is recorded as such, never merged twice.
- Handle every expected race without failing the run: a secondary that no longer exists, a
  secondary that was already merged into the primary or into some other contact, and a
  primary that was itself merged away.
- Leave an append-only record of every attempt in Databricks. dbt reads it so every model
  that excludes merged contacts excludes these ones at once, without waiting for Airbyte.

## Non-goals

- The job does not decide who should merge. Eligibility, survivor ranking and every
  guardrail live in dbt. The job merges each row of its input relation, up to a cap, and
  applies no filter of its own.
- It is not a general HubSpot client and not a reverse-ETL flow. `reverse-etl` sends
  desired state. This job is destructive, so it has its own package, image, credential and
  DAG.
- A merge cannot be undone. Anything that makes the job merge a pair outside its input
  relation is a failure, even if the merge itself succeeds.

## Decisions that differ from the TDD

The TDD was written before the Primary ID Preservation beta. These points supersede it, and
the TDD is updated to match in step 1.

1. **The primary keeps its record ID and its place in workflows.** Verified in the sandbox:
   with the beta on, the survivor keeps `primaryObjectId` and stays enrolled where it was,
   delays and pending actions included. Only the absorbed record is unenrolled. The
   workflow guardrail in the TDD's Phase 2 (defer pairs found in a workflow's
   `activeIlsListId`) is therefore dropped. A secondary that was mid-sequence still leaves
   that sequence. We accept that.
2. **Order of merges does not affect correctness.** Because the primary ID is stable,
   every secondary of a person merges straight into `primary_hs_contact_id`. No chain,
   and a failed pair affects only itself. The queue still sets an order, oldest elections
   first, to put the lowest-risk merges first.
3. **Primary email comes from the ranking, not from a write.** A merge keeps the primary's
   `email` and adds the secondary's to `hs_additional_emails`. If the most recently active
   app-linked contact is ranked primary, the TDD's "primary email is the most recently
   active account's" rule holds without the job writing anything. Step 3 changes the
   ranking to do that. The job never writes `email` or `hs_additional_emails`. Known gap:
   216 app-linked primaries have a HubSpot email that differs from their gp-api user's
   email, which the ranking does not change. The Segment-to-HubSpot primary email test
   (see "Related work outside this job") is what would close it.
4. **Retired external keys are avoided, not tracked.** A merge silently drops one value of
   a unique property when both records hold different values. On contacts today that is
   `br_candidacy_id`, in 15 pairs. The queue excludes those pairs (steps 5 and 20) instead of
   building the `retired_keys` table now.
5. **Primary Company re-assertion is deferred.** The TDD's post-merge step to re-set the
   primary Company is not part of this job. We record the decision and measure the
   population before the cap rises past the first increments (step 25).
6. **Human rejections live in a seed.** The TDD's `merge_decisions` table becomes a dbt
   seed of contacts or persons that must never be auto-merged, with a reason. Rejections
   are rare and reviewed in a PR, so a seed is enough.

## Constraints

**One pair per call.** `POST /crm/v3/objects/contacts/merge` with `primaryObjectId` and
`objectIdToMerge`. There is no batch merge. The burst limit is per app, 100 or 190 requests
per 10 seconds by tier. Reads can be batched (`batch/read`, 100 ids per call), so the job
reads 50 pairs at a time and merges them one by one.

**The beta is a beta.** The job records the ID each merge returns, and treats "the ID I got
back is not the primary" as its own status that alerts. If HubSpot withdraws the beta,
merges still work, and the log shows where each survivor went.

**Merging changes more than duplicates.** The merge applies HubSpot's own rules: the
primary's value wins for any property it has set, lifecycle stage keeps the furthest stage,
`createdate` keeps the earliest. Owner is the large one: in 22,146 of 40,485 pairs the two
contacts have different owners, and the secondary's owner loses. Sales signs off on that
before the first production merge (step 1).

**Airbyte lags HubSpot.** A merged-away contact keeps its last extract in staging and looks
live. The only sign it is gone is the survivor's `hs_merged_object_ids`, which arrives on
the next sync, and the candidates mart is rebuilt only twice a day. Until then, any model
that writes to HubSpot by contact ID can write the secondary's values onto the survivor,
because HubSpot redirects writes to merged IDs. `contact_attributes_hubspot` already guards
against this through `int__hubspot_contact_merges`. Step 19 makes that model read the
merge log too, which closes the gap for every consumer at once.

## The population

Measured against the mart on 2026-11-04.

| | count |
|---|---|
| secondary rows (merge operations) | 40,485 |
| distinct persons | 31,146 |
| persons with 3 or more live contacts (largest: 14) | 6,282 |
| `matched_on = 'splink'` | 39,872 (98.5%) |
| `matched_on = 'native_ids'` | 613 |
| `match_evidence = 'same_phone'` | 14,508 |
| `match_evidence = 'name_differs'` | 3,885 |
| shared native ID or same phone, and last names agree | 13,731 |
| owner differs between the pair | 22,146 |
| `br_candidacy_id` differs between the pair | 15 |
| both contacts app-linked | 955 |
| both app-linked and the secondary was more recently active | 49 |
| only the secondary is app-linked | 16 |
| persons holding more than one gp_api account | 1,313 |

The TDD says only exact matches merge unattended, and 98.5% of pairs come from the
probabilistic matcher. The 13,731 row is a first cut of "exact": a shared native ID or a
shared normalised phone, with agreeing last names. Step 4 defines it in the mart. At a few
merges per second, all of it is a few hours of API time, so the cap, not throughput, sets
the pace.

## The parts

### 1. dbt: candidates, queue and log

Three relations, each with one job.

- `hubspot_contact_merge_candidates` (existing table): the review surface. One row per
  live contact in a multi-contact person, with the ranking, the evidence and the review
  flags. Gains `exact_match` and `has_unique_key_conflict`.
- `hubspot_contact_merge_queue` (new view): the job's input and the whole interface
  between dbt and the job. One row per merge that is eligible to run now. It reads the
  candidates mart and applies every rule: secondary rows only, `exact_match`, no unique key
  conflict, a person-size cap, not in the exclusion seed, and not already terminal in the
  merge log. A view, so a merge logged a minute ago drops out on the next read.
- `airflow_source.hubspot_contact_merge_log` (new table, job-owned): the job's append-only
  record, declared as a dbt source. `int__hubspot_contact_merges` unions its successful
  rows with what it derives from `hs_merged_object_ids` today.

Queue columns:

| column | meaning |
|---|---|
| `primary_hs_contact_id` | merge target (string) |
| `hs_contact_id` | the secondary to absorb (string) |
| `gp_person_id` | carried into the log for audit; the job does not use it |
| `match_evidence` | carried into the log for audit |
| `latest_election_date` | the person's latest candidacy election date; sets the merge order |

The job merges the oldest elections first, since those contacts matter least. A person
with no election date goes last.

The relation name is configuration. A sandbox run, a narrowed production run and the
scheduled run differ only in which relation the job reads.

### 2. The merger

A small Python package, `hubspot-merge/`, beside `reverse-etl/`. It knows nothing about
dbt or Airflow. Its core takes an iterable of pairs, a HubSpot client and a log writer, so
tests pass a list and fakes, and the CLI passes a Databricks reader and real clients.

Per run:

1. Confirm the portal: `GET /account-info/v3/details` must return the portal the
   configuration names. A service key carries no portal in its text, so asking the API is
   the only check.
2. Read up to `MERGE_MAX_PAIRS` rows from the relation, oldest `latest_election_date`
   first (`ORDER BY` and `LIMIT` in SQL).
3. For each chunk of 50 pairs, one `batch/read` of the 100 IDs with the properties the
   snapshot keeps. Classify each pair from the read. Merge only the pairs that read as
   two live, distinct contacts.
4. Merge serially, at no more than `MERGE_MAX_RPS`, honouring `Retry-After` on 429, and
   retrying 5xx with backoff. Concurrency is not needed at the volumes the cap allows, and
   serial merges cannot collide on the same primary.
5. Append one log row per pair, chunk by chunk.

There is no resumption logic in the job. A rerun reads the queue, which already excludes
logged pairs. A pair that was merged but whose log row was lost in a crash reads as
`already_merged` on the next run. The pre-merge read is what makes the job idempotent.

`--dry-run` does steps 1 to 3 and prints the classification. It merges nothing and writes
no log.

Statuses, one log row per pair attempted:

| status | meaning | queue excludes it? |
|---|---|---|
| `merged` | merge succeeded and returned the primary's ID | yes |
| `already_merged` | the secondary already resolves to the primary | yes |
| `merged_elsewhere` | the secondary resolves to a third contact | yes, and review |
| `secondary_missing` | the secondary does not resolve at all | yes |
| `primary_merged_away` | the primary resolves to another contact | no; the mart re-ranks after the next sync |
| `primary_missing` | the primary does not resolve at all | no; the mart drops it after the next sync |
| `id_changed` | merge succeeded but returned an ID other than the primary | yes, and alert |
| `failed` | HubSpot refused the merge, or retries ran out | no; retried next run |

### 3. The log

Append-only Delta table, one INSERT per chunk, following `reverse-etl/src/retl/sent_log.py`
(rows packed into one JSON parameter, since each INSERT is its own commit of about 2.5
seconds). A crash loses at most one chunk, and the pre-merge read recovers it.

Columns: `run_id`, `object_type`, `primary_id`, `secondary_id`, `status`,
`survivor_id` (the ID the merge returned, or what the secondary resolved to),
`gp_person_id`, `match_evidence`, `error`, `primary_snapshot` and `secondary_snapshot`
(JSON of the pre-merge read, for manual reconstruction of a wrong merge), `attempted_at`.

Location: `goodparty_data_catalog.airflow_source.hubspot_contact_merge_log` in
production and `goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log` for
sandbox and dev runs. The log is ingest, since dbt reads it as a source. It follows the
repo's ingest convention:

- It sits in a `<writer>_source` schema, never in a schema dbt builds into (`dbt`,
  `mart_*`, personal or PR schemas).
- Prod and dev are separated by schema, not by table name, as `extract_ballotready` and
  matcha do. There is one catalog, so the schema is all that keeps a dev run off prod data.
- The writing service principal owns the table: `airflow` in `airflow_source`,
  `airflow_dev` in `airflow_source_dev`.

The job creates the table only in an explicit `init-log` command, never during a run, as
`retl --init-log` does. A table created by a person is then transferred to the service
principal (steps 16 and 17). The full name is configuration. The DAG sets it from a
Variable, so astro-dev points at `airflow_source_dev`, and astro-prod points at
`airflow_source`.

### 4. The runner

An Airflow DAG shaped like `reverse_etl_hubspot`: one KubernetesPodOperator running the
job's image, the image tag pinned by an Airflow Variable with no mutable default, the
token, relation, log table, cap and expected portal passed through the environment,
deployed paused. Unpausing is the explicit enable step. The log table Variable has no
default, so a dev deployment cannot write to the production log by omission.

## Verification

**Unit tests.** Every status is reachable without HubSpot. Classification is a pure
function of the read and merge responses. The client's pacing and retry, the log's
chunking and the reader's SQL are tested with fakes.

**Sandbox.** A marked test (`pytest -m sandbox`, run by hand, never in CI) creates
throwaway contacts in the sandbox, builds pairs for every reachable status, runs the
executor against them, and asserts the outcome by reading HubSpot back: the primary still
exists with its ID, the secondary resolves to the primary, the survivor's
`hs_merged_object_ids` names the secondary, and a contact enrolled in a throwaway workflow
stays enrolled. The same run is then repeated through the CLI against a hand-built pair
table, with the log in `airflow_source_dev`, to exercise the real reader and log writer.

**Production, narrowed.** The CLI's `verify --run-id` command reads a run's log rows and
re-checks the first three assertions against HubSpot for each `merged` pair. It is used
after the hand-run production batches and is never part of the scheduled run. Workflow
enrollment in production is checked by hand on the few pairs where it applies.

## Steps

Each step is one PR or one recorded action, reviewable on its own, with its own tests or
evidence. "Done when" is the exit check. Each step, or small group of steps, has its own
ticket in the [HubSpot Data Flow via Data Platform](https://app.clickup.com/t/86ak1txhk)
epic. The ticket names the steps it covers, and its ClickUp dependencies match the table
below.

### Order

| Steps | Ticket | Waits on |
|---|---|---|
| 1 | DATA-2779 | nothing |
| 2 | DATA-2780 | nothing |
| 3 | DATA-2781 | nothing |
| 4-5 | DATA-2782 | nothing |
| 6 | DATA-2783 | nothing |
| 7-8 | DATA-2784 | nothing |
| 9 | DATA-2785 | 7-8 |
| 10 | DATA-2786 | 2, 7-8 |
| 11 | DATA-2787 | 7-8 |
| 12 | DATA-2788 | 7-8 |
| 13 | DATA-2789 | 9, 10, 11 |
| 14 | DATA-2790 | 12, 13 |
| 15 | DATA-2791 | 2, 13 |
| 16 | DATA-2792 | 14, 15 |
| 17 | DATA-2793 | 14 |
| 18 | DATA-2794 | 17 |
| 19 | DATA-2795 | 18 |
| 20 | DATA-2796 | 4-5, 6, 19 |
| 21 | DATA-2797 | 18 |
| 22-23 | DATA-2521 | 1, 3, 16, 20, 21 |
| 24 | DATA-2798 | 14 |
| 25 | DATA-2799 | 20 |
| 26 | DATA-2800 | 24 |
| 27-28 | DATA-2562 | 22-23, 25, 26, and the two gates under "Related work outside this job" |
| 29 | DATA-2757 | nothing; must finish before the reverse-ETL HubSpot sync is first enabled |

Steps 1 to 6 and 7 to 14 can run in parallel. The critical path is 7 and 8, then 9 to 11,
13, 14, then 17 to 20, then 22 and 23. Steps 24 to 26 can be built while 22 and 23 run;
only enabling (27) waits on the first production merges. Some tickets cover two steps that
ship together.

Ticket names carry the zero-padded step number ("Merge step 07-08: ..."), so sorting the
epic's subtasks by name gives this order. Tickets that can start now are in "to do"; the
rest stay in "backlog" until what they wait on is done.

### Phase 0: decisions and access

1. **Update the TDD and get sign-off.** Record the "Decisions that differ from the TDD"
   section in the ClickUp TDD: ID and workflow preservation confirmed, workflow guardrail
   dropped, primary email by ranking, retired keys avoided, primary Company deferred,
   rejections in a seed. Get Sales sign-off that the primary's owner wins (22,146 pairs
   differ). Get the primary rule re-confirmed with the step 3 ranking change, which moves
   the primary in 65 merges. Done when the TDD reflects this and both sign-offs are on
   the ticket.
2. **Create the service keys and confirm merge behaviour the job relies on.** One
   dedicated service key per portal (sandbox 51780263, production 21589597), scoped
   `crm.objects.contacts.read` and `crm.objects.contacts.write`. Add a probe to
   `reverse-etl/probes/checks.py` that records, on sandbox contacts: what `batch/read`
   returns for a merged-away ID (the survivor's record, or nothing), what the merge
   endpoint returns for an already-merged pair and for a missing ID, and the response
   shape of a successful merge. Done when the findings are in the probes README and the
   status table above is confirmed or corrected.

### Phase 1: dbt, candidates

3. **Change the survivor ranking.** In `hubspot_contact_merge_candidates.sql`, move
   `is_app_linked` and `app_user_last_active_at` ahead of `is_pro_candidate`, and reorder
   the `primary_decided_by` CASE to match. Update the model description in
   `_sales_reverse_etl__models.yml`. Add a test: when a person has an app-linked contact,
   the primary is the most recently active one. Done when the mart builds with
   `--favor-state`, the test passes, and the 65 affected pairs (49 plus 16) have the
   app-linked contact as primary.
4. **Add `exact_match`.** True when the secondary shares the primary's deterministic
   identity (`matched_on = 'native_ids'`) or normalised phone, and the last names agree.
   Document it, and test that it is never true for `name_differs` or `name_incomplete`.
   Done when the count is recorded on the ticket (about 13,700 expected).
5. **Add `has_unique_key_conflict`.** True when both contacts hold different
   `br_candidacy_id` values. Done when documented, tested on the 15 known pairs, and the
   list of unique contact properties it covers is written in the column description.
6. **Add the exclusion seed.** `seeds/hubspot_contact_merge_exclusions.csv` with
   `hs_contact_id`, `gp_person_id`, `reason`, `added_by`, either ID optional. Start it
   empty. Done when the seed builds and its schema tests pass.

### Phase 2: the merger package

7. **Scaffold `hubspot-merge/`.** `pyproject.toml` (uv, Python 3.14, `requests`,
   `tenacity`, `databricks-sql-connector[pyarrow]`, `databricks-sdk`; dev group `pytest`,
   `mypy`, `types-requests`), `src/hsmerge/`, `tests/`, `.env.example`, `README.md`,
   console script `hsm`, a `pytest-hubspot-merge` pre-push hook and a
   `.github/workflows/hubspot-merge.yml` test workflow. Done when `uv run pytest` passes on
   an empty suite in CI.
8. **Define types and configuration.** `Pair`, `Status` (the eight values above),
   `Outcome`, and `Config.from_env` reading `HUBSPOT_TOKEN`, `HUBSPOT_BASE_URL`,
   `HUBSPOT_EXPECTED_PORTAL_ID`, `MERGE_OBJECT_TYPE`, `MERGE_PAIR_RELATION`,
   `MERGE_LOG_TABLE`, `MERGE_MAX_PAIRS`, `MERGE_MAX_RPS`, plus the Databricks variables.
   Missing values raise a named error, a non-positive cap is rejected, and an object type
   other than `contacts` is rejected. Done when each branch has a test.
9. **Add the HubSpot client.** `verify_portal`, `batch_read(ids, properties)` and
   `merge(primary, secondary)`, over an injectable transport, with a minimum interval
   between calls, `Retry-After` on 429 and backoff on 5xx, and no retry on other 4xx. The
   token stays out of the repr. Done when tests cover a portal mismatch, a 429 that then
   succeeds, a 400 that does not retry, and the pacing.
10. **Write classification.** `classify_read(pair, read)` and `classify_merge(pair,
    response)` as pure functions, built on the shapes recorded in step 2. Done when every
    status has a test using a recorded response shape.
11. **Write the log writer and its DDL.** `create_log_table_sql`, `init_log_table` and a
    chunked `MergeLogWriter`, reusing the JSON-parameter INSERT pattern from
    `retl/sent_log.py` (copied, since subprojects do not share code). Done when tests show
    rows arrive in chunks, the remainder flushes on exit including after an exception, and
    the DDL is `create table if not exists` only. The writer has no UPDATE, DELETE or
    MERGE path: the current state of each pair is derived in dbt, never stored.
12. **Write the pair reader.** `read_pairs(connection, relation, limit)` emitting
    `select primary_hs_contact_id, hs_contact_id, gp_person_id, match_evidence from
    {relation} order by latest_election_date nulls last, hs_contact_id limit {limit}`.
    Done when the SQL and the row mapping are tested.
13. **Write the executor.** `execute(pairs, client, log)`: chunk, batch-read, classify,
    merge, log. Done when tests with fakes show a mixed chunk produces the right status
    per pair, a merge exception logs `failed` and continues, and a crash mid-run leaves
    every finished chunk logged.
14. **Wire the CLI.** `hsm run [--dry-run]`, `hsm init-log`, `hsm verify --run-id ID`.
    Done when each command's wiring is tested with the executor faked, and `--dry-run`
    is shown to call neither `merge` nor the log writer.

### Phase 3: sandbox

15. **Add the sandbox end-to-end test.** `tests/test_sandbox.py`, marked `sandbox` and
    skipped unless the sandbox key is set. It creates tagged contacts, builds pairs for
    `merged`, `already_merged`, `merged_elsewhere`, `secondary_missing` and
    `primary_merged_away`, runs `execute` with an in-memory log, asserts statuses and the
    read-back checks, and archives what it created. A workflow check enrolls one primary
    in a throwaway delay workflow built for the purpose and asserts it stays enrolled.
    Done when it passes twice in a row against the sandbox.
16. **Run the CLI against the sandbox through Databricks.** Run `hsm init-log` against
    `airflow_source_dev.hubspot_contact_merge_log`, transfer its ownership to the
    `airflow_dev` service principal, and grant yourself `MODIFY` on that table. Hand-build
    a pair table of sandbox IDs in your dev schema (it is input, not a log). Run `hsm run
    --dry-run`, then `hsm run`, then `hsm verify`. Done when the log rows match what
    HubSpot shows, and the results are written to
    `docs/hubspot-contact-merge-job-verification.md`.

### Phase 4: dbt, queue and log

17. **Create the production log table.** Run `hsm init-log` against
    `goodparty_data_catalog.airflow_source.hubspot_contact_merge_log`, then transfer its
    ownership to the `airflow` service principal (`alter table ... owner to`, which a
    data-engineers member can do through the catalog-level `MANAGE`). Grant `MODIFY` on
    that one table to the people who will run the manual batches in steps 22 and 23, and
    revoke it after step 23. The dbt principals already read the schema through
    catalog-level grants. Done when the table is owned by the service principal and each
    identity can do what it needs.
18. **Declare the log as a dbt source.** Add the table to the existing `airflow_source`
    source in `models/staging/__sources.yaml`, with its own freshness setting (`null`
    until step 27, since the log only grows when the job runs), and a staging model
    `stg_airflow_source__hubspot_contact_merge_log` that types the columns and parses the
    snapshots. Done when `dbt build` passes on the empty table.
19. **Union the log into `int__hubspot_contact_merges`.** Rows with status `merged`,
    `already_merged` or `id_changed` add `secondary_id` merged into `survivor_id`;
    `merged_elsewhere` adds it merged into the contact it resolved to. Keep the existing
    `hs_merged_object_ids` derivation and deduplicate. Then switch the candidates mart's
    own `merged_away` CTE to read this model. Done when existing tests pass and a test
    seeded with a log row shows the contact excluded from the candidates mart and from
    `contact_attributes_hubspot`.
20. **Add `hubspot_contact_merge_queue`.** A view over the candidates mart: secondary rows
    only, `exact_match`, no unique key conflict, `live_contact_count` under an agreed cap,
    not in the exclusion seed, and the secondary not in the log with a status the queue
    excludes. The queue is empty while the log holds an `id_changed` row that nobody has
    reviewed, so a scheduled run cannot merge after the ID preservation beta stops
    working. Add `latest_election_date` from the person's candidacies in `mart_civics`.
    Document every rule in the model YAML. Test that it has no duplicate secondaries and
    that no secondary equals its primary. Done when its row count, by election year, is
    recorded on the ticket.
21. **Add log health tests.** dbt tests on the log source: error on any `id_changed` row,
    warn when a pair has failed in three or more runs, warn on any `merged_elsewhere`.
    Done when the tests run in the scheduled build.

### Phase 5: production, by hand

Before the first production merge: the step 1 sign-offs are on record, the step 3 ranking
is in production, and the sandbox runs in step 16 were clean.

22. **Run five hand-checked pairs in production.** Pick five low-risk pairs from the queue
    (same phone, same name, no owner conflict, no app account) and check each in the
    HubSpot UI. Point `MERGE_PAIR_RELATION` at a scratch view selecting only those five,
    set the production key and portal, and run `hsm run --dry-run`, then `hsm run`, then
    `hsm verify`. Check the survivors in the UI, including any workflow enrollment. Done
    when the results are on the ticket and the five secondaries have left the queue.
23. **Run a batch of 100 from the queue.** Same procedure, with the scratch view taking
    100 rows from the queue across a mix of `match_evidence` values. Spot-check ten in the
    UI. Done when there is no `failed` or `id_changed` row, or each one is explained.

### Phase 6: automation

24. **Build and publish the image.** `hubspot-merge/Dockerfile` and
    `.github/workflows/hubspot-merge-container.yml`, copied in shape from `reverse-etl`:
    two-stage build, `uv sync --frozen --no-dev --no-editable`, entrypoint `hsm`, published
    to `ghcr.io/thegoodparty/gp-data-platform/hubspot-merge` with a sha tag beside
    `latest`. Done when a merge to main publishes an image that runs `hsm --help`.
25. **Measure the primary Company population.** Count the queued pairs where both
    contacts have a primary Company association and they differ. Decide on the ticket
    whether re-assertion is needed before the cap passes 1,000. Done when the decision is
    recorded.
26. **Add the DAG.** `airflow/astro/dags/hubspot_contact_merge.py`, with Variables for the
    image tag, pull secret, token, relation, log table (no default), cap and expected
    portal, scheduled after the 12:02 UTC dbt build and before the 17:00 UTC reverse-ETL
    send, `is_paused_upon_creation=True`, plus a DAG test of the env surface and the pinned-tag
    rule. Document it in `airflow/astro/docs/hubspot_contact_merge.md`. Done when the DAG
    tests pass and the DAG is deployed paused.
27. **Enable with a small cap.** Set the Variables with the cap at 100 and unpause. Done
    when three scheduled runs complete with clean logs.
28. **Raise the cap in agreed steps.** For example 500, then 2,000, then the full queue,
    each after a clean week of runs. Watch the log tests and confirm the candidates count
    falls. Before the cap passes 100, the gp-api lookup audit and the merged-away
    verification decision must both be done (see "Related work outside this job"). Done
    when the exact-match queue is empty apart from rows explained in the log.

### Housekeeping

Independent of the merge job. Do it before `reverse_etl_hubspot` is first unpaused, while
the `reverse_etl` schema is still empty (it was on 2026-11-04).

29. **Move the reverse-ETL send logs to an ingest schema.** retl's send logs are
    append-only logs read back by a job, so they follow the same ingest rule. The
    `reverse_etl` schema puts dev and prod in one schema, separated only by table name.
    It is also owned by the `github-action` principal rather than the writer.
    - Point the default in `reverse-etl/.env.example` at
      `goodparty_data_catalog.airflow_source.sent_log_hubspot`. Update `reverse-etl/README.md`,
      `airflow/astro/docs/reverse_etl_hubspot.md`, and the table names in the retl and DAG
      tests.
    - Set the `reverse_etl_hubspot_log_table` Variable to
      `airflow_source.sent_log_hubspot` on astro-prod and to
      `airflow_source_dev.sent_log_hubspot` on astro-dev. The `_orphans` companion table
      follows automatically, because its name derives from the log table.
    - Run `retl --init-log` against each, and transfer ownership to the matching service
      principal.
    - If any table exists in `reverse_etl` by then, copy it with
      `create table ... deep clone`, verify the row counts, and drop the original. Then
      drop the `reverse_etl` schema.

    Done when both deployments write to their own ingest schema, and `reverse_etl` no
    longer exists.

## Related work outside this job

These have their own tickets in the same epic. They are not steps of this plan, but
some of them gate it, and some of them wait on it.

Gates for step 28:

- **Audit gp-api's HubSpot Contact lookups for merge tolerance** (DATA-2572). Engineering confirms
  every lookup accepts a different Contact back, and that nothing writes
  `hs_additional_emails`.
- **Decide whether a merged-away contact still verifies a campaign** (DATA-2647). A gp-api user whose
  saved contact ID is merged away has their campaign verification read a frozen copy of
  the old contact. The job grows that group: 971 secondaries are app-linked, 581 of them
  exact matches. The fix, if Product wants it, reads through
  `int__hubspot_contact_merges`, which carries the merge log from step 19.

Not a gate:

- **Segment-to-HubSpot primary email test** (DATA-2571). Would close the gap in decision 3.

Waiting on this plan:

- **Make `gp_person_id` unique on Contacts** (DATA-2522). Proposed: stamp it only on each person's
  ranked primary contact, leaving the others null. That makes the property unique without
  waiting for every merge, and a later merge keeps the primary's value. If agreed, it can
  start once step 3 is in production. The reverse-ETL HubSpot sync's enable (DATA-2412) waits on it.
- **The Office Holder work** (DATA-2683, DATA-2645, DATA-2684). Its first production writes wait on the first merges (steps 22 and 23).

### After this job

Widening the queue past `exact_match` (for example probabilistic pairs with a human review
round) is a change to the queue view and the exclusion seed, not to the job.
