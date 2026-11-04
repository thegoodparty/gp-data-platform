# HubSpot contact merge job: design

## Why

The HubSpot Data Flow TDD commits Data Platform to an automated nightly job that merges
duplicate HubSpot Contacts. The merge candidates already exist as a dbt mart
(`hubspot_contact_merge_candidates`), which ranks a person's live contacts and names one
primary. Nothing consumes it yet: today a human reads reviewed pairs and runs
`reverse-etl/probes/merge_pairs.py` by hand.

This document covers the job that consumes those pairs. It is deliberately narrow.

## Scope

In scope: HubSpot Contacts only.

Out of scope, each a later increment: Companies, the Office Holder custom object, the
merge-candidate report for human review, and any change to how candidates are computed
beyond the ranking change in step 6 below.

## Goals

- Merge a reviewed pair list through HubSpot's contacts merge endpoint, unattended, on a
  schedule.
- Be safe to retry: a rerun after a partial failure completes the remaining pairs and
  re-attempts nothing that already succeeded.
- Fail gracefully on every expected race: a secondary that no longer exists, a pair whose
  survivor already absorbed the secondary, a primary that was itself merged away.
- Leave an append-only record of what was merged, in Databricks, so merges can be audited
  and later excluded from the candidate model.

## Non-goals and anti-goals

- It does not decide who should merge. All identity resolution and survivor ranking stays
  in dbt. The job merges exactly what the pair relation says to merge.
- It is not a general HubSpot client and not a reverse-ETL path. `reverse-etl` ships
  desired state outward; this job is destructive and separate on purpose.
- A merge cannot be undone. Anything that makes the job merge a pair outside the reviewed
  relation is a failure even if the merge itself succeeds.

## Constraints

**No batch merge endpoint.** HubSpot merges one pair per call:
`POST /crm/v3/objects/contacts/merge` with `primaryObjectId` and `objectIdToMerge`. The
burst limit is per app key, 100 or 190 requests per 10 seconds depending on tier, with a
daily cap of 250,000 or more. There is no bulk form of the call, so "batching" is about
pacing and checkpointing, never about fewer calls.

**The primary ID is preserved.** Primary ID Preservation for Merged Records is a public
beta, enabled in sandbox and production. The survivor keeps the primary's record ID instead
of receiving a new one. This makes secondaries independent: every secondary of a person can
be merged into `primary_hs_contact_id` in any order, so a failed pair does not cascade and a
retry is a single pair. The beta is also what preserves workflow enrollment on the survivor,
verified in sandbox.

The beta is a beta. The job therefore reads the returned ID from each merge response and
records it, and treats "the ID I asked for is not the ID I got back" as a distinct state
rather than an impossibility. If the beta is withdrawn, merges still work and the log shows
where survivor IDs moved.

**Merging is destructive.** It also changes behaviour silently: a merge unenrolls the
absorbed record, folds its email in as a secondary address, and applies HubSpot's own merge
rules to properties. Nothing in the job may write `hs_additional_emails`; doing so releases
an absorbed address and recreates the duplicate.

## What the population actually looks like

Measured against the mart on 2026-11-03, which sets the shape of the safety model:

| | count |
|---|---|
| secondary rows (merge operations) | 40,485 |
| distinct primaries (people) | 31,146 |
| rows where `matched_on = 'splink'` | 39,872 (98.5%) |
| rows where `matched_on = 'native_ids'` | 613 |
| rows where `match_evidence = 'same_phone'` | 14,508 |
| rows where `match_evidence = 'name_differs'` | 3,885 |
| rows where `match_evidence = 'name_incomplete'` | 12 |

Two consequences. At roughly 10 requests per second, the full population is about 1.1 hours
of API time, so the whole back catalogue fits in a few scheduled nights rather than a
month-long drip. And 98.5% of candidate pairs come from the probabilistic matcher, not from
a shared native id, with 11,682 pairs resting on a name comparison alone. The TDD says only
exact matches merge unattended. Closing that gap belongs to dbt, not to this job: add
`exact_match` to the mart in step 3 below, and let the job's default filter read it.

## The parts

### 1. The pair relation

Input contract, and the whole interface between dbt and the job. One row per merge
operation:

| column | meaning |
|---|---|
| `primary_hs_contact_id` | merge target (string) |
| `hs_contact_id` | the secondary to absorb (string) |
| `gp_person_id` | audit key, not used by the job |
| `contact_rank` | ordering within a person, 2 upward |

Default source is `hubspot_contact_merge_candidates` filtered to `merge_role = 'secondary'`.
The relation name is configuration, not code, so one image serves a narrowed view, a
sandbox mirror, and a hand-built table of chosen IDs.

### 2. The merger

A small Python package with one responsibility: given an iterable of pairs, merge them and
report outcomes. It knows nothing about dbt, Airflow, or where pairs come from.

- Input is a reader that yields legs of pairs, so the same code paths serve a CSV of
  hand-picked IDs and a Databricks relation.
- Per pair: one batch-read of both contacts (confirms both exist, neither is already merged
  away, and fetches the properties the log wants), then the merge call.
- Per attempt: a status, evidence and timestamps, appended to the log.
- Pacing: a token-bucket limiter sized below the portal's burst limit, plus bounded
  concurrency. Retries with backoff on 429 and 5xx.
- A hard cap on pairs per run, defaulting well below the full population, so production
  exposure is a configuration value rather than a property of the table's size.
- A portal guard: the first call is `GET /account-info/v3/details`, and the job refuses to
  run when the portal does not match what its configuration declares. A service key carries
  no portal in its text, so asking the API is the only way to know which portal a credential
  opens.

Statuses, one row per attempted pair:

| status | meaning | retry? |
|---|---|---|
| `merged` | merge returned success, survivor id recorded | no |
| `already_merged` | secondary no longer resolves as itself; absorbed previously | no |
| `secondary_missing` | secondary does not exist as a contact at all | no |
| `primary_merged_away` | primary resolves to a different contact | no, flag for review |
| `mismatch` | merge succeeded but the returned id is not the primary | no, log and alert |
| `skipped` | filtered before any write (same id twice, cap reached, filter rule) | no |
| `failed` | non-retryable error, with the HubSpot message | next run, unless malformed |

### 3. The log

Append-only table, written as batched INSERTs, one Delta commit per chunk. The chunk is
appended when full and at the end of the run, so a container that dies mid-run loses at most
one chunk rather than the run's whole record.

Columns: `run_id`, `batch_id`, `primary_hs_contact_id`, `hs_contact_id`,
`survivor_hs_contact_id`, `status`, `error`, `attempted_at`, `merged_at`.

Location: `goodparty_data_catalog.reverse_etl`, beside the existing send-log tables, which
that schema's description already records as job-created and outside the marts and
shared-marts rollups. The name is configuration, so a sandbox deployment points elsewhere.

Two consumers follow immediately: the next run excludes recently completed pairs, and the
candidate model skips pairs already merged while the mart catches up.

`reverse-etl/src/retl/sent_log.py` already implements this shape, including why rows are
packed per chunk (each INSERT is its own Delta commit, roughly 2.5s). Follow that file
rather than invent a second pattern.

### 4. The runner

An Airflow DAG shaped like `reverse_etl_hubspot`: one KubernetesPodOperator running the
job's image, image tag pinned by an Airflow Variable with no mutable default, token and
relation name passed through the environment, deployed paused, unpaused as the explicit
enable step. The job package does not import Airflow.

## Verification

Three stages, differing in what is verified rather than in what runs.

**In the merger's own tests.** Every status is reachable without HubSpot: the pre-merge read
and the merge call are faked, and statuses are asserted from response shapes. Pacing, attempt
accounting, resumption from a partially written log, and the cap are unit-tested.

**Against sandbox, on chosen pairs.** Run the image with a fixed list of IDs created for the
purpose. Confirm per pair: the primary still exists, the secondary ID resolves to the primary,
the survivor shows the merge in its merge history, the survivor kept its workflow enrollment,
and the log rows match what the API did.

**Against production, on a handful of low-risk IDs.** Same assertions on a small slice, with
the pair relation narrowed rather than the job changed. Only after that does the cap rise.

**Not on every run.** The five assertions are a `--verify` mode and a manual checklist. A
steady-state run does one batch-read and one merge call per pair and nothing else; five reads
per pair would not scale past a few thousand merges.

## Numbered steps

Each step is one commit, independently reviewable, with its own tests. Steps 1 to 4 and 6 to
14 are code; 5, 15 and 16 need hubspot access and a human.

### Prerequisite: the ranking change (dbt)

1. **Change the ranking that picks the survivor.** In
   `dbt/project/models/marts/sales_reverse_etl/hubspot_contact_merge_candidates.sql`, move
   `is_app_linked` and `app_user_last_active_at` ahead of `is_pro_candidate` in the
   `row_number()` `order by`, so the most recently active gp-api account wins even when the
   contact is not Pro. Update the `primary_decided_by` CASE in the same order. Add a dbt test
   asserting that when a person has at least one app-linked contact, the primary is one of
   them.
2. **Document the new order.** Update the model's `description` in
   `_sales_reverse_etl__models.yml` to state the rule in words, and note that
   `primary_decided_by` can now report `is_app_linked` first.
3. **Add the exact-match filter column.** Add `exact_match` (boolean) to the mart: true when
   the pair shares a deterministic native id, or a normalised phone, and the last names agree.
   Document it and test that `exact_match` is false for every `match_evidence` of
   `name_differs` or `name_incomplete`. This is what the TDD's "exact matches only" means,
   and what the job's default filter reads.
4. **Confirm the mart still builds and its tests pass** with `--favor-state` against dev, and
   record the new counts.

### The merger package

5. **Scaffold the subproject.** Create `hubspot-merge/` beside `reverse-etl/`: `pyproject.toml`
   (uv, Python 3.14, `requests`, `tenacity`, `databricks-sql-connector[pyarrow]`,
   `databricks-sdk`; dev group `pytest`, `mypy`, `types-requests`, `ruff`), `src/mergejob/`
   package, `tests/`, `.env.example`, `README.md`. Console script `hsm`. Nothing runs yet.
6. **Define the pair and status types.** `Pair(primary_id: str, secondary_id: str,
   gp_person_id: str | None, contact_rank: int | None)` and a `Status` enum with exactly the
   seven values in the table above. Test: constructing a `Pair` from a dict of mart columns
   ignores unknown keys.
7. **Add config loading from the environment.** `Config.from_env(env)`: `HUBSPOT_TOKEN`,
   `HUBSPOT_BASE_URL`, `HUBSPOT_EXPECTED_PORTAL_ID`, `MERGE_PAIR_RELATION`,
   `MERGE_PAIR_FILTER` (default `exact_match`), `MERGE_LOG_TABLE`, `MERGE_MAX_PAIRS`,
   `MERGE_MAX_RPS`. Missing required values raise a named error, and a non-positive
   `MERGE_MAX_PAIRS` is rejected at load time, mirroring `retl/config.py`. Test each branch.
8. **Add the portal guard.** `verify_portal(client, expected_id)` calls
   `GET /account-info/v3/details` and raises `WrongPortalError` on mismatch, naming both ids.
   Every write path calls it first. Test with a faked client for match, mismatch, and a
   non-200.
9. **Add the HubSpot client with pacing.** `HubSpotClient.read_contacts(ids)` over
   `POST /crm/v3/objects/contacts/batch/read` and `merge(primary, secondary)` over
   `POST /crm/v3/objects/contacts/merge`, both under a token bucket enforcing
   `MERGE_MAX_RPS`, with `tenacity` retry on 429 and 5xx and no retry on 4xx. Test: the
   bucket refuses to exceed the rate; a 429 retries and then succeeds; a 400 does not retry.
10. **Classify one pair's outcome.** `classify(pair, read_result, merge_result) -> Outcome`
    covering all seven statuses. Tests are the status table: one test per status, with the
    response shapes that produce it, including the case where the read shows the secondary
    resolving to the primary (`already_merged`) and where the merge returns an id other than
    the primary (`mismatch`).
11. **Execute pairs with a cap and resumption.** `execute(pairs, client, log, max_pairs)`:
    takes the first `max_pairs`, checks the log for pairs already recorded as terminal, runs
    the rest through classify, appends outcomes. Test: a second run over the same list
    re-attempts nothing already logged; the cap truncates and logs `skipped` for the
    remainder; a mid-run exception still leaves the completed chunk appended.
12. **Write the log.** `LogWriter` over `databricks-sql-connector`, batched INSERTs of the
    nine columns, plus `terminal_pairs_since(run_cutoff) -> set[Pair]` for resumption. Mirror
    `retl/sent_log.py`'s chunking. Tests with a faked cursor: rows arrive in chunks, the
    flush happens on exit, and the resumption query's SQL names the right columns.
13. **Add the two readers.** `CsvReader` (local pairs of ids, for sandbox runs) and
    `DatabricksReader(relation, filter)` emitting `SELECT primary_hs_contact_id, hs_contact_id,
    gp_person_id, contact_rank FROM {relation} WHERE {filter}` in legs. Test the SQL string
    and the leg chunking.
14. **Wire the CLI.** `hsm run --source=csv|databricks [--limit N] [--verify] [--dry-run]`.
    `--dry-run` reads and classifies nothing that writes; `--verify` runs the five assertions
    on each merged pair and appends them as evidence. Test each flag's wiring with the
    executor faked.

### Sandbox, then production

15. **Sandbox verification.** Create a dozen throwaway contacts in the sandbox with a known
    duplicate shape, run `hsm run --source=csv` against them with `--verify`, and record per
    pair: primary still exists, secondary resolves to primary, merge history on the survivor,
    workflow enrollment preserved, log rows correct. Write the findings into
    `docs/hubspot-contact-merge-job-verification.md`.
16. **Build and publish the image.** `Dockerfile` and
    `.github/workflows/hubspot-merge-container.yml`, copied in shape from `reverse-etl`:
    two-stage build, `uv sync --frozen --no-dev --no-editable`, entrypoint `hsm`, GHCR image
    at `ghcr.io/thegoodparty/gp-data-platform/hubspot-merge`, sha tag beside `latest`.
17. **Add the DAG.** `airflow/astro/dags/hubspot_contact_merge.py` with a
    configuration table (relation, log table, cap, image tag, pull secret, token, expected
    portal id) and `is_paused_upon_creation=True`, plus a dag test asserting the env surface
    and the no-mutable-tag rule. `airflow/astro/docs/hubspot_contact_merge.md` documents it.
18. **Narrowed production run.** Point the relation at a view selecting a small hand-checked
    set of pairs with `exact_match` true, run with `--verify`, confirm the five assertions on
    real records, and record the result on the ticket.
19. **Raise the cap.** Step `MERGE_MAX_PAIRS` up in agreed increments, watching the log's
    `failed` and `mismatch` counts, and confirm the mart's candidate count falls.
20. **Wire the feedback loop.** Add `hubspot_contact_merge_candidates`' exclusion of pairs the
    log records as merged, so the mart stops proposing pairs the job already handled while the
    source extract catches up.
