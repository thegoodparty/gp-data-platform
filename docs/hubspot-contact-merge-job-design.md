# HubSpot contact merge job: design and plan

This is the source of truth for the HubSpot contact merge job: its design, its plan and
where the work stands. Anyone who picks up a step, person or agent, should be able to do
it from this document, the code, and the step's ticket in the
[DATA-2328](https://app.clickup.com/t/86ak1txhk) epic. If they disagree, the code wins,
then the ticket. When you finish a step, update "Status" and the "Order" table here in
the same PR.

## Status, 2026-11-18

The goal of this work is one HubSpot contact for each person. Today many people have two
or more contacts. The plan is to merge these duplicates with an automatic job that runs
every night. We split the work into 29 steps, and each step has its own ticket in the
DATA-2328 epic.

Much of the work is done. In dbt, the candidates table now picks the right contact to
keep: the one linked to the most recently active app account. It also marks the pairs
that are safe to merge without a person checking them first. A list of contacts that must
never be merged exists, but it is empty for now. We also wrote the merge program,
`hubspot-merge`. It reads pairs of contacts, checks each pair in HubSpot, merges the pairs
that are safe, and writes one log row for each pair it tries. It is tested with fake data
and against the HubSpot sandbox. The program is built into an image, and an Airflow job
that runs it is on astro-prod. The job is paused, so it does not merge anything yet. We
also moved the reverse-ETL logs to their own ingest schema.

One plan change came from the code we built. The Airflow job can now create its own log
table, and we can run the first test batches through it. So nobody needs to change table
owners by hand or get special write access. Steps 16, 17, 22 and 23 below say this.

The next steps, in order:

1. **Get the sign-offs (step 1).** Record the changed decisions in the TDD. Get Sales to
   agree that the kept contact's owner wins, and get the rule for which contact we keep
   confirmed again. This can start now, and no production merge can happen until it is
   done.
2. **Test in the sandbox through Airflow (step 16).** Set up the job on astro-dev, create
   the dev log table, and run a small batch of sandbox contacts. Write the results in a
   short doc. This can start now.
3. **Create the production log table (step 17).** Set the job's settings on astro-prod
   and run the command that creates the table. This can start now.
4. **Connect the log to dbt (steps 18 to 21).** Add the log as a dbt source. Use it so
   merged contacts drop out of other models at once. Build the queue view that lists the
   pairs the job may merge, and add tests that warn us when a merge goes wrong.
5. **Run the first production merges by hand (steps 22 and 23).** First merge five pairs
   that a person has checked one by one, then a batch of 100.
6. **Measure one known risk (step 25).** Count the pairs where the two contacts belong to
   different companies.
7. **Turn on the nightly job (steps 27 and 28).** Start with 100 merges a night, then
   raise the limit step by step. Two other teams must finish their work before the limit
   goes above 100: Engineering must check that the app still works with merged contacts,
   and Product must decide how merged contacts affect campaign checks.

The "Order" table under "Steps" lists every step with its status and PRs. "Running the
job" has the settings and commands. "Not yet checked" and "Working notes" at the end
list what nobody has confirmed and how we work on this.

## Rules for anyone working on this

Read these before you start any step. They apply to people and to agents.

1. **A merge cannot be undone.** Never trigger `run` against production HubSpot (portal
   `21589597`) unless a person has told you to, for that batch, in this session. You may
   prepare pair tables, queries and dry runs.
2. **Do one step at a time**, in the order in the "Order" table. Before you start, check
   that every step it waits on is done. If one is not, stop and say so.
3. **Follow each step's numbered list in order.** When a step says "expect" or "must",
   check it. If the result is different, stop, do not try to fix it by changing other
   steps, and report what you saw.
4. **Ask a person** for any key, any decision a step names (for example a cap), and any
   change to an Astro deployment's branch mapping. Do not guess a value.
5. **Never write a secret** (a HubSpot key, a Databricks secret) into a file, a commit, a
   PR, a ticket or a log.
6. **Follow the repo's rules.** `CLAUDE.md` at the repo root covers job logs and
   pre-commit, and `dbt/project/CLAUDE.md` covers dbt style, branches and PRs. In short:
   - branch from a fresh `origin/main` as `data-<number>/<short-slug>`;
   - title PRs `[DATA-<number>] <short title>`;
   - keep ticket numbers out of code and comments;
   - run `dbt` from `dbt/project/` with the system dbt Cloud CLI, never through `uv`.
7. **When a step is done**, do the following in the same PR or straight after it:
   - update its row in the "Order" table and the "Status" section;
   - set its ticket to "done";
   - move the tickets it blocks to "to do" (see "Working notes").

## Why

The HubSpot Data Flow TDD commits Data Platform to an automated nightly job that merges
duplicate HubSpot Contacts (Workstream B, item 8). The merge candidates already exist as a
dbt mart (`hubspot_contact_merge_candidates`), which ranks a person's live contacts and
names one primary. Before this job, a human read reviewed pairs and ran
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
`retl --init-log` does. The DAG's `init_log` command runs it as the deployment's service
principal, so that principal owns the table from the start (steps 16 and 17). The full
name is configuration. The DAG sets it from a Variable, so astro-dev points at
`airflow_source_dev`, and astro-prod points at `airflow_source`.

### 4. The runner

An Airflow DAG shaped like `reverse_etl_hubspot`: one KubernetesPodOperator running the
job's image, the image tag pinned by an Airflow Variable with no mutable default, the
token, relation, log table, cap and expected portal passed through the environment,
deployed paused. Unpausing is the explicit enable step. The log table Variable has no
default, so a dev deployment cannot write to the production log by omission.

### 5. Running the job

The built job matches the parts above. These are the facts you need to run it. The full
procedure is in `airflow/astro/docs/hubspot_contact_merge.md`, and the environment is in
`hubspot-merge/.env.example`.

**Code.** `hubspot-merge/src/hsmerge/`: `config.py`, `client.py`, `classify.py`,
`pairs.py`, `execute.py`, `log.py`, `databricks.py`, `cli.py`. The DAG is
`airflow/astro/dags/hubspot_contact_merge.py`. The image is
`ghcr.io/thegoodparty/gp-data-platform/hubspot-merge`, tagged with each main commit's sha.

**Commands.** `hsm init-log`, `hsm run --dry-run`, `hsm run`, `hsm verify --run-id ID`.
Every command except `init-log` checks the portal first. `run` exits 1 when any pair is
`id_changed`, or when some pairs `failed` and none merged. `verify` exits 1 when a
`merged` pair does not read as merged. The last line of each command's output is the
one to check:

| command | last line |
|---|---|
| `init-log` | `log table ready: <table>` |
| `run --dry-run` | `dry run: <status>=<n>, ...` (a pair that would merge shows as `would_merge`) |
| `run` | `run <run_id>: <status>=<n>, ...` |
| `verify` | `verified <n> merged pairs, <m> problems` |

**Rules the built job adds.** The run stops after the first chunk with an `id_changed`
row, and after a failed batch read; the chunks already logged stay logged. Before the
first merge, it checks that it can write to the log, so a missing table or grant stops the
run early. It refuses a pair relation that lists a secondary twice, or one ID as both a
primary and a secondary. Table names must have the form `catalog.schema.table`.
`MERGE_PAIR_RELATION` and `MERGE_MAX_PAIRS` are read by `run` only.

**DAG.** Scheduled at 15:00 UTC. The `command` trigger param is `dry_run`, `run`,
`verify` or `init_log`, and defaults to `dry_run`. A scheduled run always runs `run`. The
`pair_relation` and `max_pairs` params override their Variables for one run, and
`run_id` is for `verify`. Airflow does not start manual runs on a paused DAG, so a manual
trigger needs the DAG unpaused for a short time.

To trigger by hand, open the DAG in the Airflow UI of the deployment, choose "Trigger
DAG", and enter the params as JSON. Examples:

```json
{"command": "init_log"}
{"command": "dry_run", "pair_relation": "goodparty_data_catalog.dbt_<you>.<table>", "max_pairs": 5}
{"command": "run", "pair_relation": "goodparty_data_catalog.dbt_<you>.<table>", "max_pairs": 5}
{"command": "verify", "run_id": "<run_id from the run's last log line>"}
```

A manual `dry_run` or `run` must set both `pair_relation` and `max_pairs` until step 27
sets the Variables. With either missing, the task fails before it starts a pod.

Unpausing the DAG can start one scheduled run straight away. Until step 27 it fails
before it starts a pod, because the two Variables are unset. That failure is expected;
ignore it.

To set a Variable, use the Astro UI (the deployment, then Admin, then Variables in its
Airflow UI) or the CLI:

```bash
astro deployment airflow-variable create --deployment-id <id> --key <key> --value <value>
astro deployment airflow-variable update --deployment-id <id> --key <key> --value <value>
```

Deployment IDs: astro-prod `cmljxv7hh1v9d01omzqi58ov1`, astro-dev
`cml8n1tav1nxs01nd7ptp4tpm`. astro-prod deploys `main` on every merge. astro-dev deploys
whichever branch is mapped to it in the Astro UI.

**Variables**, per deployment:

| Variable | astro-dev | astro-prod |
|---|---|---|
| `hubspot_merge_image_tag` | a full 40-character commit sha whose "hubspot-merge container" workflow run on main succeeded; on 2026-11-18 that was `d10ef7a222764a2033ebb58ad369d154e58ed891` | same |
| `hubspot_merge_image_pull_secret` | `ghcr-pull` | `ghcr-pull` |
| `hubspot_merge_hubspot_token` | sandbox merge key, from a person | production merge key, from a person |
| `hubspot_merge_expected_portal_id` | `51780263` | `21589597` |
| `hubspot_merge_log_table` | `goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log` | `goodparty_data_catalog.airflow_source.hubspot_contact_merge_log` |
| `hubspot_merge_pair_relation` | unset until needed | the queue view, at step 27 |
| `hubspot_merge_max_pairs` | unset until needed | `100` at step 27 |
| `hubspot_merge_max_rps` | defaults to 5 | defaults to 5 |

While `hubspot_merge_pair_relation` or `hubspot_merge_max_pairs` is unset, a scheduled
run fails before it starts the pod. That keeps a briefly unpaused DAG from merging before
step 27. Neither log table existed on 2026-11-18. Steps 16 and 17 create them. On
2026-11-18 no `hubspot_merge_*` Variable was set on either deployment.

The DAG also reads the shared `databricks_conn_id` Variable, which names the Databricks
connection (`databricks_dev` on astro-dev, `databricks` on astro-prod). On 2026-11-18 it
was listed on astro-dev but not on astro-prod. Step 17 checks this.

**Querying Databricks.** Use the repo's helper, from the repo root:
`python .claude/skills/databricks-query/scripts/dbsql.py "<sql>"`. It needs a valid CLI
login. If it fails with an expired token, ask a person to run
`databricks auth login --profile dbc-3d8ca484-79f3`. From `dbt/project/`,
`dbt show --inline "<sql>"` also works, and it resolves `ref()` and `source()`.

## Verification

**Unit tests.** Every status is reachable without HubSpot. Classification is a pure
function of the read and merge responses. The client's pacing and retry, the log's
chunking and the reader's SQL are tested with fakes.

**Sandbox.** A marked test (`pytest -m sandbox`, run by hand, never in CI) creates
throwaway contacts in the sandbox, builds pairs for every reachable status, runs the
executor against them, and asserts the outcome by reading HubSpot back: the primary still
exists with its ID, the secondary resolves to the primary, the survivor's
`hs_merged_object_ids` names the secondary, and a contact enrolled in a throwaway workflow
stays enrolled. The same run is then repeated through the DAG on astro-dev against a
hand-built pair table, with the log in `airflow_source_dev`, to exercise the real reader,
log writer, image and DAG.

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

| Steps | Ticket | Waits on | Status |
|---|---|---|---|
| 1 | DATA-2801 | nothing | to do |
| 2 | DATA-2780 | nothing | done, #1316 |
| 3 | DATA-2781 | nothing | done, #1315 (follow-up #1335 open) |
| 4-5 | DATA-2782 | nothing | done, #1317, #1328 |
| 6 | DATA-2783 | nothing | done, #1333 |
| 7-8 | DATA-2784 | nothing | done, #1314 |
| 9 | DATA-2785 | 7-8 | done, #1327, #1332 |
| 10 | DATA-2786 | 2, 7-8 | done, #1329, #1332 |
| 11 | DATA-2787 | 7-8 | done, #1336 |
| 12 | DATA-2788 | 7-8 | done, #1330 |
| 13 | DATA-2789 | 9, 10, 11 | done, #1337 |
| 14 | DATA-2790 | 12, 13 | done, #1338 |
| 15 | DATA-2791 | 2, 13 | done, #1339 |
| 16 | DATA-2792 | 14, 15, 26 | to do |
| 17 | DATA-2793 | 14, 26 | to do |
| 18 | DATA-2794 | 17 | backlog |
| 19 | DATA-2795 | 18 | backlog |
| 20 | DATA-2796 | 4-5, 6, 19 | backlog |
| 21 | DATA-2797 | 18 | backlog |
| 22-23 | DATA-2521 | 1, 3, 16, 17, 20, 21 | waiting |
| 24 | DATA-2798 | 14 | done, #1340 |
| 25 | DATA-2799 | 20 | backlog |
| 26 | DATA-2800 | 24 | done, #1341; deployed paused to astro-prod on 2026-11-18 |
| 27-28 | DATA-2562 | 22-23, 25, 26, and the two gates under "Related work outside this job" | backlog |
| 29 | DATA-2757 | nothing; must finish before the reverse-ETL HubSpot sync is first enabled | done, #1331 |

The critical path from here is 17, then 18 to 20, then 22 and 23, then 27 and 28. Steps 1
and 16 can run beside it, and both must be done before 22. Some tickets cover two steps
that ship together. PR #1338 also records a CLI run against the sandbox, with scratch
tables in `dbt_dball`. That run is evidence for step 16, but it does not replace it.

Ticket names carry the zero-padded step number ("Merge step 07-08: ..."), so sorting the
epic's subtasks by name gives this order. Tickets that can start now are in "to do"; the
rest stay in "backlog" until what they wait on is done.

### Phase 0: decisions and access

1. **Update the TDD and get sign-off.** This step is people work. An agent may draft the
   TDD text and the questions, but a person records the sign-offs. Record the "Decisions that differ from the TDD"
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

#### 15. Add the sandbox end-to-end test (done)

`hubspot-merge/tests/test_sandbox.py`, marked `sandbox` and skipped unless
`HUBSPOT_SERVICE_KEY` is set. It creates tagged contacts, builds pairs for `merged`,
`already_merged`, `merged_elsewhere`, `secondary_missing`, `primary_merged_away` and
`primary_missing`, runs `execute` with an in-memory log, checks the statuses and the
read-back, and archives what it created. Run it with
`cd hubspot-merge && HUBSPOT_SERVICE_KEY=<sandbox key> uv run pytest -m sandbox`.

#### 16. Run the job against the sandbox through Airflow

Waits on 14, 15 and 26 (all done). Blocks 22-23. Uses only the sandbox portal, so it
cannot change production HubSpot.

1. **Get the DAG onto astro-dev.** astro-dev deploys whichever branch is mapped to it
   in the Astro UI (Workspace Settings, Git Deploys). Its last deploy was on 2026-09-10,
   before the DAG existed. Ask the human to map astro-dev to `main`, or do it yourself
   if they tell you to. Only one branch can be mapped at a time, so someone else may be
   using it. Confirm the deploy with
   `astro deployment inspect cml8n1tav1nxs01nd7ptp4tpm | grep current_tag`: the date in
   the tag must be after the mapping change.
2. **Get the sandbox merge key** from the human. Do not search for it, and never write it
   into a file, a commit, a PR or a ticket.
3. **Set the astro-dev Variables.** Use the values in "Running the job", with the image
   tag from that section. Leave `hubspot_merge_pair_relation` and
   `hubspot_merge_max_pairs` unset.
4. **Create the dev log table.** Unpause the DAG in the Astro UI, trigger it with
   `{"command": "init_log"}`, and pause it again. The task log ends with
   `log table ready: goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log`.
   Check the owner:

   ```sql
   describe table extended goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log
   ```

   The `Owner` row must name the astro-dev service principal, not a person.
5. **Build sandbox contacts and a pair table.** Create contacts in the sandbox portal
   (51780263) only. Use the same approach as the `Sandbox` class in
   `hubspot-merge/tests/test_sandbox.py`, and give every contact a first name that starts
   with `hsm-dag-` so you can find and archive them later. Make three pairs:
   - pair A: two new live contacts (expected status `merged`);
   - pair B: two new contacts that you merge yourself with the HubSpot API first
     (expected `already_merged`);
   - pair C: a new primary, and a secondary that you archive first (expected
     `secondary_missing`).

   Then build the table:

   ```sql
   create or replace table goodparty_data_catalog.dbt_<you>.hsm_sandbox_pairs as
   select * from values
     ('<A primary>', '<A secondary>', cast(null as string), 'sandbox', cast(null as date)),
     ('<B primary>', '<B secondary>', cast(null as string), 'sandbox', cast(null as date)),
     ('<C primary>', '<C secondary>', cast(null as string), 'sandbox', cast(null as date))
   as t(primary_hs_contact_id, hs_contact_id, gp_person_id, match_evidence, latest_election_date)
   ```

   It is job input, so a personal schema is fine. Never put a log there.
6. **Dry run.** Trigger with
   `{"command": "dry_run", "pair_relation": "goodparty_data_catalog.dbt_<you>.hsm_sandbox_pairs", "max_pairs": 3}`.
   `max_pairs` is required on every manual `dry_run` and `run`, because the Variable is
   unset. The task log must show `would_merge` for A, `already_merged` for B and
   `secondary_missing` for C. Stop if it does not.
7. **Run.** Trigger with the same params and `"command": "run"`. The last log line is
   `run <run_id>: already_merged=1, merged=1, secondary_missing=1`. Copy the `run_id`.
8. **Verify.** Trigger with `{"command": "verify", "run_id": "<run_id>"}`. The log line
   must be `verified 1 merged pairs, 0 problems`.
9. **Check the log.**

   ```sql
   select secondary_id, status, survivor_id, error, attempted_at
   from goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log
   where run_id = '<run_id>'
   ```

   Expect three rows. A and B have `survivor_id` equal to their primary. C has a null
   `survivor_id`. All `error` values are null.
10. **Clean up.** Archive every `hsm-dag-` contact in the sandbox and drop the pair table.
    Leave the log table and its rows.
11. **Record it.** Write `docs/hubspot-contact-merge-job-verification.md` with the date,
    the image sha, the run ID, each command's last log line and the query result. Open a
    PR, and set the ticket to "done".

Done when the log rows match what HubSpot shows and the verification doc is merged.

### Phase 4: dbt, queue and log

#### 17. Create the production log table

Waits on 14 and 26 (both done). Blocks 18 and 22-23. Creates an empty table. It does not
touch HubSpot.

1. **Check the Databricks connection Variable.** The DAG reads the `databricks_conn_id`
   Variable to find its connection, and on 2026-11-18 `astro deployment airflow-variable
   list --deployment-id cmljxv7hh1v9d01omzqi58ov1` did not list it on astro-prod. Other
   astro-prod DAGs may get it another way, for example from a secrets backend. Ask the
   human how astro-prod resolves it. If it is missing, it must be set to `databricks`
   before anything else here works.
2. **Get the production merge key** from the human, with the same rules as step 16.
3. **Set the astro-prod Variables** from "Running the job". Leave
   `hubspot_merge_pair_relation` and `hubspot_merge_max_pairs` unset.
4. **Create the table.** Unpause the DAG, trigger `{"command": "init_log"}`, and pause it
   again straight after the run finishes. With the two Variables unset, a scheduled run
   that starts while the DAG is unpaused fails before it starts a pod, so it cannot merge.
5. **Check the owner and access.**

   ```sql
   describe table extended goodparty_data_catalog.airflow_source.hubspot_contact_merge_log;
   select count(*) from goodparty_data_catalog.airflow_source.hubspot_contact_merge_log;
   ```

   The owner must be the astro-prod service principal, and the count must be 0. dbt
   reads the schema through catalog-level grants. Step 18's `dbt build` proves it.

Done when the table exists, empty, and the service principal owns it.

#### 18. Declare the log as a dbt source

Waits on 17. Blocks 19 and 21. dbt work, from `dbt/project/`.

1. In `models/staging/__sources.yaml`, add a table to the existing `airflow_source`
   source (search for `- name: airflow_source`). That source sets `loaded_at_field:
   loaded_at` and a 14-day freshness for all its tables, and neither fits the log, so
   override both on the table:

   ```yaml
      - name: hubspot_contact_merge_log
        description: >
          Append-only log written by the hubspot-merge job: one row per pair it tried
          to merge. Written by the job only, never by dbt.
        config:
          loaded_at_field: attempted_at
          # The log grows only when the job runs. Step 27 sets a real threshold.
          freshness: null
        columns:
          - name: primary_snapshot
            config:
              meta:
                sensitive: confidential
          - name: secondary_snapshot
            config:
              meta:
                sensitive: confidential
   ```

   The snapshots hold email, phone and names, so tag them as `hubspot_api_contacts` is
   tagged in the same file.
2. Add `models/staging/airflow_source/stg_airflow_source__hubspot_contact_merge_log.sql`.
   Select every column from `{{ source("airflow_source", "hubspot_contact_merge_log") }}`.
   Keep the names. Parse the snapshots with
   `from_json(primary_snapshot, 'map<string,string>') as primary_snapshot` (the same for
   the secondary). Add no filters and no deduplication: the log is the record.
3. Document the model and every column in a new
   `models/staging/airflow_source/_stg_airflow_source__hubspot_contact_merge_log.yml`.
   Add `not_null` on `run_id`, `primary_id`, `secondary_id`, `status` and `attempted_at`,
   and `accepted_values` on `status` with the eight statuses. Put the test arguments
   under `arguments:`.
4. Run `dbt build --select "stg_airflow_source__hubspot_contact_merge_log"`.

Done when the build passes on the empty table. Open the PR.

#### 19. Read the log in `int__hubspot_contact_merges`

Waits on 18. Blocks 20. dbt work.

Today `models/intermediate/hubspot/int__hubspot_contact_merges.sql` derives merges only
from the survivors' `merged_object_ids`, which reach dbt after Airbyte's next sync. Its
output is one row per `merged_contact_id` (string), with `surviving_contact_id`
(string). `contact_attributes_hubspot`, `people`, `hubspot_contacts`,
`int__civics_person_links` and `int__user_resolved_keys` read it. Keep that output and
grain.

1. Rename the current final select to a CTE named `from_hubspot`.
2. Add a CTE `from_log`: from the step 18 staging model, keep rows whose `status` is
   `merged`, `already_merged`, `id_changed` or `merged_elsewhere`, and select
   `secondary_id as merged_contact_id` and `survivor_id as surviving_contact_id`. Keep
   one row per `merged_contact_id`, the latest by `attempted_at`.
3. The final select is a full outer join of the two on `merged_contact_id`. Select
   `coalesce(h.merged_contact_id, l.merged_contact_id) as merged_contact_id` and
   `coalesce(h.surviving_contact_id, l.surviving_contact_id) as surviving_contact_id`. HubSpot's own record wins
   once it arrives, because it also reflects later merges that the log never saw.
4. Add `unique` and `not_null` on `merged_contact_id` in the model YAML.
5. In `models/marts/sales_reverse_etl/hubspot_contact_merge_candidates.sql`, replace the
   `merged_away` CTE body with
   `select merged_contact_id as merged_id from {{ ref("int__hubspot_contact_merges") }}`.
   The join below it stays the same.
6. Add a dbt unit test for `int__hubspot_contact_merges`, following the `unit_tests:`
   example in `models/intermediate/analytics/int__analytics.yaml`. Give it:
   - a contact listing one merged id;
   - a log row `merged` for a different secondary;
   - a log row `failed`;
   - a log row and a HubSpot listing for the same secondary with different survivors.

   Expect rows for the first two, no row for the `failed` one, and HubSpot's survivor
   for the last.
7. Run `dbt build --select "int__hubspot_contact_merges hubspot_contact_merge_candidates contact_attributes_hubspot"`.
   Then check that the candidates row count is unchanged against production, since the
   log is empty in production.

Done when the build and the unit test pass and the row count matches.

#### 20. Add `hubspot_contact_merge_queue`

Waits on 4-5, 6 and 19. Blocks 22-23 and 25. dbt work. This view is the only input the
scheduled job reads, so every rule about what may merge lives here.

Before you write it, ask the human two questions and put the answers in the model YAML:

- **The person-size cap.** On 2026-11-18, the exact-match secondaries without a key
  conflict (13,669) split by `live_contact_count` as: 2: 5,014; 3: 1,553; 4: 6,887;
  5: 153; 6: 24; 7: 8; 8: 17; 11: 2; 14: 11. The proposal is `live_contact_count <= 4`
  (13,454 rows).
- **How a reviewed `id_changed` row is recorded.** The proposal is a seed,
  `seeds/hubspot_contact_merge_reviewed_runs.csv`, with columns `run_id`, `reason` and
  `reviewed_by` (a role, not a person's name). It starts empty, and a row is added in a
  reviewed PR after someone has looked at that run. Declare it in
  `seeds/seeds_schema.yaml` with all three columns typed `STRING`, as
  `hubspot_contact_merge_exclusions` is.

Build `models/marts/sales_reverse_etl/hubspot_contact_merge_queue.sql`. Start the file
with `{{ config(materialized="view", tblproperties={}) }}`, as
`contact_attributes_hubspot.sql` does, because the folder defaults to tables. The view:

1. Reads `hubspot_contact_merge_candidates` where `merge_role = 'secondary'`,
   `exact_match`, `not has_unique_key_conflict`, and `live_contact_count` is within the
   cap.
2. Drops a row when the exclusion seed (`hubspot_contact_merge_exclusions`) names its
   `hs_contact_id`, its `primary_hs_contact_id`, or its `gp_person_id`.
3. Drops a row when the log (the step 18 staging model) has its `hs_contact_id` as
   `secondary_id` with status `merged`, `already_merged`, `merged_elsewhere`,
   `secondary_missing` or `id_changed`. Rows logged only as `failed`,
   `primary_merged_away` or `primary_missing` stay in the queue.
4. Returns no rows at all while the log has an `id_changed` row whose `run_id` is not in
   the reviewed-runs seed. Do this with a one-row CTE that computes `is_blocked` and a
   `where not is_blocked` filter. Do not raise an error: an empty queue is the safe
   state.
5. Adds `latest_election_date`: per `gp_person_id`, the `max` of
   `greatest(primary_election_date, primary_runoff_election_date, general_election_date,
   general_runoff_election_date)` from `{{ ref("candidacy") }}` (all four are `date`, and
   `greatest` skips nulls). Left join it, so a person with no candidacy gets null.
6. Selects exactly these five columns, in this order: `primary_hs_contact_id`,
   `hs_contact_id`, `gp_person_id`, `match_evidence`, `latest_election_date`.

Follow `dbt/project/CLAUDE.md`: use CTEs, not subqueries, and add `is not null` inside
any `not in` subquery. Document the model, every rule and every column in
`_sales_reverse_etl__models.yml`. Add tests:
- `unique` and `not_null` on `hs_contact_id`;
- `not_null` on `primary_hs_contact_id`;
- `dbt_utils.expression_is_true` with `hs_contact_id != primary_hs_contact_id`;
- a singular test in `dbt/project/tests/` that fails when any `hs_contact_id` also
  appears as a `primary_hs_contact_id`.

The job refuses to run if the last two fail. Build with
`dbt build --select "hubspot_contact_merge_queue"`.

Then record the row count by election year on the ticket:

```sql
select year(latest_election_date) as election_year, count(*) as merges
from {{ ref("hubspot_contact_merge_queue") }}
group by 1 order by 1
```

Done when the build passes, the PR is merged, and the counts are on the ticket.

#### 21. Add log health tests

Waits on 18. Blocks 22-23. dbt work. Add three singular tests in `dbt/project/tests/`,
each reading the step 18 staging model. Each returns the rows that are the problem.

| file | severity | returns |
|---|---|---|
| `assert_hubspot_merge_log_no_id_changed.sql` | error (the default) | every `id_changed` row |
| `assert_hubspot_merge_log_no_repeated_failures.sql` | `{{ config(severity="warn") }}` | each `secondary_id` with `failed` in 3 or more distinct `run_id`s and no later row with another status |
| `assert_hubspot_merge_log_no_merged_elsewhere.sql` | `{{ config(severity="warn") }}` | every `merged_elsewhere` row |

Run `dbt build --select "stg_airflow_source__hubspot_contact_merge_log"`, which runs the
tests attached to it. After the merge, find the three tests in the next scheduled
production dbt Cloud run's results.

Done when the tests show in a scheduled run at those severities.

### Phase 5: production, by hand

These are the first production merges. A merge cannot be undone. An agent may prepare
the pair tables and the queries. A person must look at the pairs, trigger every `run`,
and approve each next step. Before you start, check every item here:

- step 1's sign-offs are on its ticket;
- steps 16, 17, 20 and 21 are done;
- `select count(*) from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue`
  returns more than 0.

#### 22. Merge five hand-checked pairs in production

1. **Pick five pairs.** This query lists low-risk candidates: same phone, same first and
   last name, the same owner (or none), and no app account on either side.

   ```sql
   with
       q as (select * from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue),
       c as (select * from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_candidates)
   select q.*, s.first_name, s.last_name, s.phone, s.hubspot_owner_id, p.hubspot_owner_id as primary_owner
   from q
   inner join c as s on s.hs_contact_id = q.hs_contact_id
   inner join c as p on p.hs_contact_id = q.primary_hs_contact_id
   where q.match_evidence = 'same_phone'
       and lower(s.first_name) = lower(p.first_name)
       and lower(s.last_name) = lower(p.last_name)
       and s.hubspot_owner_id <=> p.hubspot_owner_id
       and not s.is_app_linked and not p.is_app_linked
   order by q.latest_election_date nulls last
   limit 20
   ```

   The person picks five, and opens both contacts of each pair in the HubSpot UI.
2. **Build the pair table** with only those five:

   ```sql
   create or replace table goodparty_data_catalog.dbt_<you>.hsm_prod_batch_01 as
   select * from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue
   where hs_contact_id in ('<s1>', '<s2>', '<s3>', '<s4>', '<s5>')
   ```

   Check that it has 5 rows.
3. **Dry run** on astro-prod:
   `{"command": "dry_run", "pair_relation": "goodparty_data_catalog.dbt_<you>.hsm_prod_batch_01", "max_pairs": 5}`.
   Expect `would_merge=5`. Stop if anything else appears.
4. **Run**, triggered by the person, with the same params and `"command": "run"`. Copy
   the `run_id` from the last log line.
5. **Verify** with `{"command": "verify", "run_id": "<run_id>"}`. Expect
   `verified 5 merged pairs, 0 problems`.
6. **Check.** The person opens each survivor in the HubSpot UI: same record ID as the
   primary, the secondary's email under additional emails, and any workflow enrollment
   kept. Then check that the five left the queue:

   ```sql
   select count(*) from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue
   where hs_contact_id in ('<s1>', '<s2>', '<s3>', '<s4>', '<s5>')
   ```

   Expect 0. The view reads the log, so this needs no dbt rebuild.
7. Post the run ID, the three log lines and the checks on the ticket, and drop the pair
   table.

#### 23. Merge a batch of 100 from the queue

Do this only after a person approves step 22's results. It uses the same procedure, with
these changes:

- The pair table takes 100 rows from the queue with a mix of `match_evidence` values,
  for example
  `qualify row_number() over (partition by match_evidence order by latest_election_date nulls last) <= 34`,
  then `limit 100`.
- Use `"max_pairs": 100`.
- The person spot-checks ten survivors in the UI.

Done when the log has no `failed` or `id_changed` row for the run, or each one is
explained on the ticket. Any `id_changed` stops all merge work until a person reviews it.

### Phase 6: automation

#### 24. Build and publish the image (done)

`hubspot-merge/Dockerfile` and `.github/workflows/hubspot-merge-container.yml`. Each
merge to main that touches `hubspot-merge/` publishes
`ghcr.io/thegoodparty/gp-data-platform/hubspot-merge` tagged `latest` and the commit sha.

#### 25. Measure the primary Company conflict population

Waits on 20. Blocks 27-28. Read-only. A contact's primary Company is the
`associatedcompanyid` property. It is not a staging column, so read it from the Airbyte
source for this one-off measurement (from `dbt/project/`):

```sql
with
    company as (
        select cast(id as string) as hs_contact_id,
               nullif(get_json_object(properties, '$.associatedcompanyid'), '') as company_id
        from {{ source("airbyte_source", "hubspot_api_contacts") }}
    )
select count(*) as queued, count_if(s.company_id != p.company_id) as primary_company_differs
from {{ ref("hubspot_contact_merge_queue") }} as q
left join company as s on s.hs_contact_id = q.hs_contact_id
left join company as p on p.hs_contact_id = q.primary_hs_contact_id
```

Run it with `dbt show --inline "<query>"`. Post both counts on the ticket. A person
decides whether the job must re-set the primary Company before the cap passes 1,000.

Done when the counts and the decision are on the ticket.

#### 26. Add the DAG (done)

`airflow/astro/dags/hubspot_contact_merge.py`, documented in
`airflow/astro/docs/hubspot_contact_merge.md`. It deployed paused to astro-prod on
2026-11-18.

#### 27. Turn on the schedule with a cap of 100

Waits on 22-23, 25 and 26. A person must approve this step, because it starts unattended
production merges.

1. On astro-prod, set `hubspot_merge_pair_relation` to
   `goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue` and
   `hubspot_merge_max_pairs` to `100`.
2. In a dbt PR, change the log table's `freshness: null` to
   `warn_after: {count: 2, period: day}` and `error_after: {count: 4, period: day}`, so a
   job that stops running shows up. The log grows only when the queue has rows, so set it
   back to `null` once the queue is empty.
3. Unpause the DAG. Runs start at 15:00 UTC.
4. After each of the first three runs, check the last log line and the log rows for that
   `run_id`.

Done when three scheduled runs finish with no `failed` or `id_changed` rows, or each one
is explained on the ticket.

#### 28. Raise the cap in agreed steps

Waits on 27, DATA-2572 and DATA-2647. Do not set the cap above 100 until both of those
tickets are done. Each change needs a person's approval. Suggested steps: 500, then
2,000, then the whole queue, each after a clean week. Change it with
`astro deployment airflow-variable update --deployment-id cmljxv7hh1v9d01omzqi58ov1 --key hubspot_merge_max_pairs --value <n>`.
Watch the step 21 tests, and check that the candidates count falls.

Done when the queue is empty, apart from rows the log explains.

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
  waiting for every merge, and a later merge keeps the primary's value. Step 3 is in
  production, so it can start once the proposal is agreed. The reverse-ETL HubSpot sync's enable (DATA-2412) waits on it.
- **The Office Holder work** (DATA-2683, DATA-2645, DATA-2684). Its first production writes wait on the first merges (steps 22 and 23).

### After this job

Widening the queue past `exact_match` (for example probabilistic pairs with a human review
round) is a change to the queue view and the exclusion seed, not to the job.

## Not yet checked

- Whether the `reverse_etl_hubspot_log_table` Variable is set on both deployments, and
  whether the `reverse_etl` schema is dropped (the operational part of step 29).
- Whether the installed `databricks-sql-connector` accepts the CLI's
  `credentials_provider` shape when it runs from the image. Only a laptop run has used
  the real connection.
- Whether the astro-prod principal can read pair tables in personal dev schemas.
- Whether the GitHub token can delete `pr-N` image versions in the image cleanup job.
- How astro-prod resolves the `databricks_conn_id` Variable, which its Variable list
  did not show on 2026-11-18 (step 17 checks this).

## Working notes

- Branch from a fresh `origin/main`. Name branches `data-NNNN/short-name` and PR titles
  `[DATA-NNNN] ...`.
- Ask for review by commenting `delegate review` on the PR. Reply to comments, push, and
  comment again until it is approved.
- When a step is done, set its ticket to "done" and add it to the current sprint list
  while its home stays in Data Backlog. The current sprint list is the newest "Beaver"
  list in the "2026 Data Sprints" folder of the Data Team space; ask a person if you are
  not sure which it is. Move the tickets it blocks to "to do" if nothing
  else blocks them. Adding a task to a list through the ClickUp API needs the internal ID
  (such as `17tnw2bcp8v`). The custom ID (`DATA-2792`) fails with "must be in the same
  workspace".
- Each ticket's description repeats its step from this doc. If you change a step here,
  change the ticket to match.
- Do not use real people's names in commits, PRs or code. Name roles instead ("Sales",
  "the data team").
- Databricks CLI logins expire. If a query fails with an expired token, ask a person to
  run `databricks auth login --profile dbc-3d8ca484-79f3`.
- Open clean-up: merge #1335 (approved), and close #1334, an old copy of #1336.
