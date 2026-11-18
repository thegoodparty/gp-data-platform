# HubSpot contact merge: plan and handoff

Last updated 2026-11-18. This file is for anyone, person or agent, who picks up the
HubSpot contact merge work. It says what is done, what is next, and the facts you need to
do each remaining step on your own. Update it when you finish a step.

## Summary

The goal of this work is one HubSpot contact for each person. Today many people have two
or more contacts. The plan is to merge these duplicates with an automatic job that runs
every night. We split the work into 29 steps, and each step has its own ticket in the
[DATA-2328](https://app.clickup.com/t/86ak1txhk) epic.

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
owners by hand or get special write access.

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

## Sources of truth

Read these before you start a step. If they disagree with this file, the code wins, then
the ticket, then this file.

| What | Where |
|---|---|
| Design, decisions and every step's "done when" | `docs/hubspot-contact-merge-job-design.md` |
| How to run the DAG, its Variables and trigger params | `airflow/astro/docs/hubspot_contact_merge.md` |
| The merge program | `hubspot-merge/` (README, `.env.example`, `src/hsmerge/`) |
| The DAG | `airflow/astro/dags/hubspot_contact_merge.py` |
| Why logs are append-only and live in ingest schemas | `dbt/project/README.md`, and "Job logs" in `CLAUDE.md` |
| Tickets, with "Waits on" and "Blocks" links | the [DATA-2328](https://app.clickup.com/t/86ak1txhk) epic; ticket names start "Merge step NN" |
| Original design | [TDD: HubSpot Data Flow](https://goodparty.clickup.com/90132012119/v/dc/2ky4jq2q-20493/2ky4jq2q-117633) |

## Key facts

**HubSpot portals.** Sandbox `51780263`, production `21589597`. The job asks HubSpot
which portal its key opens, and stops unless it matches `HUBSPOT_EXPECTED_PORTAL_ID`. The
merge keys are scoped `crm.objects.contacts.read` and `crm.objects.contacts.write`.

**Log tables.** Production:
`goodparty_data_catalog.airflow_source.hubspot_contact_merge_log`, owned by the `airflow`
service principal. Dev and sandbox:
`goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log`, owned by
`airflow_dev`. Neither table existed when this file was written. Steps 16 and 17 create
them.

**Log columns**, from `hubspot-merge/src/hsmerge/log.py`, in this order: `run_id`,
`object_type`, `primary_id`, `secondary_id`, `status`, `survivor_id`, `gp_person_id`,
`match_evidence`, `error`, `primary_snapshot`, `secondary_snapshot` (both JSON strings of
the pre-merge read), `attempted_at` (timestamp, UTC). The job only ever inserts rows. dbt
works out each pair's current state from the log.

**Statuses**, one log row per pair tried:

| status | meaning | does the queue drop the pair? |
|---|---|---|
| `merged` | merged, and HubSpot returned the primary's ID | yes |
| `already_merged` | the secondary already resolves to the primary | yes |
| `merged_elsewhere` | the secondary resolves to a third contact | yes, and someone reviews it |
| `secondary_missing` | the secondary does not resolve | yes |
| `primary_merged_away` | the primary resolves to another contact | no; the mart re-ranks after the next sync |
| `primary_missing` | the primary does not resolve | no; the mart drops it after the next sync |
| `id_changed` | merged, but HubSpot returned another ID | yes, and it alerts; the run stops after that chunk |
| `failed` | HubSpot refused, or retries ran out | no; the next run retries it |

**Pair input.** The job reads five columns from its pair relation:
`primary_hs_contact_id`, `hs_contact_id` (the secondary), `gp_person_id`,
`match_evidence`, `latest_election_date`. It merges every row up to the cap, oldest
`latest_election_date` first, with nulls last. It applies no rules of its own. It refuses
to run if a secondary appears twice, or if one ID is both a primary and a secondary.
Relation names must have the form `catalog.schema.table`.

**Commands.** `hsm init-log`, `hsm run --dry-run`, `hsm run`, `hsm verify --run-id ID`.
Every command except `init-log` checks the portal first. `run` exits 1 on any
`id_changed`.

**DAG.** `hubspot_contact_merge`, one pod running the image
`ghcr.io/thegoodparty/gp-data-platform/hubspot-merge` at a pinned sha. It is scheduled at
15:00 UTC, between the 12:02 UTC dbt build and the 17:00 UTC reverse-ETL send. It was
created paused. The `command` trigger param defaults to `dry_run`, and a scheduled run
always runs `run`. The `pair_relation` and `max_pairs` params override their Variables
for one run.

**DAG Variables**, per deployment:

| Variable | astro-dev | astro-prod |
|---|---|---|
| `hubspot_merge_image_tag` | a sha from the main image build | same |
| `hubspot_merge_image_pull_secret` | `ghcr-pull` | `ghcr-pull` |
| `hubspot_merge_hubspot_token` | sandbox merge key | production merge key |
| `hubspot_merge_expected_portal_id` | `51780263` | `21589597` |
| `hubspot_merge_log_table` | the `airflow_source_dev` table | the `airflow_source` table |
| `hubspot_merge_pair_relation` | leave unset until needed | the queue view, at step 27 |
| `hubspot_merge_max_pairs` | leave unset until needed | `100` at step 27 |
| `hubspot_merge_max_rps` | defaults to 5 | defaults to 5 |

If `hubspot_merge_pair_relation` or `hubspot_merge_max_pairs` is unset, a scheduled run
fails before it starts the pod. This keeps an unpaused DAG safe until step 27. Airflow does
not start manual runs on a paused DAG, so a manual trigger needs the DAG unpaused for a
short time.

**dbt files you will touch.**

- `dbt/project/models/marts/sales_reverse_etl/hubspot_contact_merge_candidates.sql`: the
  candidates mart. It has `primary_hs_contact_id`, `live_contact_count`, `exact_match`
  and `has_unique_key_conflict`, and its own `merged_away` CTE.
- `dbt/project/models/intermediate/hubspot/int__hubspot_contact_merges.sql`: today it
  reads only the survivors' `merged_object_ids` from Airbyte. `contact_attributes_hubspot`,
  `people`, `hubspot_contacts`, `int__civics_person_links` and `int__user_resolved_keys`
  all read it.
- `dbt/project/seeds/hubspot_contact_merge_exclusions.csv`: contacts or persons never to
  merge. It is empty.
- `dbt/project/models/staging/__sources.yaml`: the `airflow_source` source. It sets
  `loaded_at_field: loaded_at` and a 14-day warn for the whole source, so the log table
  needs its own settings (it has `attempted_at`, not `loaded_at`).
- `dbt/project/models/marts/civics/candidacy.sql`: has `gp_person_id` and the election
  dates for `latest_election_date`.

## Done

| Step | Ticket | What | PR |
|---|---|---|---|
| 2 | DATA-2780 | merge keys, and a probe of HubSpot's merge responses | #1316 |
| 3 | DATA-2781 | rank the most recently active app-linked contact as primary | #1315 (follow-up #1335 open) |
| 4-5 | DATA-2782 | `exact_match` and `has_unique_key_conflict` | #1317, #1328 |
| 6 | DATA-2783 | exclusion seed | #1333 |
| 7-8 | DATA-2784 | package scaffold, types, config | #1314 |
| 9 | DATA-2785 | HubSpot client with pacing and a portal check | #1327, #1332 |
| 10 | DATA-2786 | pair classification | #1329, #1332 |
| 11 | DATA-2787 | log writer and DDL | #1336 |
| 12 | DATA-2788 | pair reader | #1330 |
| 13 | DATA-2789 | executor | #1337 |
| 14 | DATA-2790 | CLI | #1338 |
| 15 | DATA-2791 | sandbox end-to-end test | #1339 |
| 24 | DATA-2798 | image | #1340 |
| 26 | DATA-2800 | DAG, deployed paused to astro-prod on 2026-11-18 | #1341 |
| 29 | DATA-2757 | reverse-ETL send logs moved to `airflow_source` | #1331 |

PR #1338 also records a live CLI run against the sandbox, with scratch pair and log
tables in `dbt_dball`. That run is evidence for step 16. It does not replace it.

## Next steps in detail

Each step below can be picked up alone once what it waits on is done. When you finish
one, set its ticket to "done", move the tickets it blocks to "to do" if nothing else
blocks them, and update the "Done" table above.

### Step 1: update the TDD and get sign-off (DATA-2801)

Waits on nothing. Blocks steps 22-23. This is people work.

1. Record the design doc's "Decisions that differ from the TDD" in the ClickUp TDD: the
   primary keeps its ID and its workflows, the workflow guardrail is dropped, the primary
   email comes from the ranking, retired keys are avoided, primary Company re-assertion
   is deferred, and rejections live in a seed.
2. Get Sales sign-off that the primary's owner wins. The owners differ in 22,146 of
   40,485 pairs.
3. Get the primary rule re-confirmed with the step 3 change, which moved the primary in
   65 merges.

Done when the TDD shows the decisions and both sign-offs are on the ticket.

### Step 16: run the job against the sandbox through Airflow (DATA-2792)

Waits on 14, 15 and 26, all done. Blocks steps 22-23.

1. Check that the DAG is on astro-dev. Its last image deploy was 2026-09-10.
2. Set the astro-dev Variables from the table above.
3. Trigger `command` = `init_log`. Confirm that `airflow_dev` owns the table.
4. Build a pair table of sandbox contact IDs in your dev schema, with the five pair
   columns. It is input, so it does not go in an ingest schema. Cover at least a live
   pair, an already-merged pair and a missing secondary.
5. Trigger `dry_run`, then `run`, then `verify`, each with `pair_relation` set to that
   table.

Done when the log rows match what HubSpot shows, and the results are written to
`docs/hubspot-contact-merge-job-verification.md`.

### Step 17: create the production log table (DATA-2793)

Waits on 14 and 26, both done. Blocks step 18 and steps 22-23.

1. Set the astro-prod Variables from the table above. Leave `hubspot_merge_pair_relation`
   and `hubspot_merge_max_pairs` unset.
2. Unpause the DAG, trigger `command` = `init_log`, then pause it again.
3. Confirm that the `airflow` principal owns the table and that the dbt principals can
   read it. They read the schema through catalog-level grants.

Done when the service principal owns the table and each identity can do what it needs
and nothing more.

### Step 18: declare the log as a dbt source (DATA-2794)

Waits on 17. Blocks 19 and 21.

Add `hubspot_contact_merge_log` to the `airflow_source` source in `__sources.yaml`. Give
it `loaded_at_field: attempted_at` and `freshness: null` until step 27, because the log
grows only when the job runs. Add `stg_airflow_source__hubspot_contact_merge_log`, which
types the columns and parses the two snapshot columns.

Done when `dbt build` passes on the empty table.

### Step 19: read the log in `int__hubspot_contact_merges` (DATA-2795)

Waits on 18. Blocks 20.

Union the log into the model. `merged`, `already_merged` and `id_changed` rows map
`secondary_id` to `survivor_id`. `merged_elsewhere` rows map `secondary_id` to the
contact it resolved to, which is also in `survivor_id`. Keep the existing
`merged_object_ids` derivation and remove duplicates. Then change the candidates mart's
own `merged_away` CTE to read this model.

Done when existing tests pass, and a test seeded with a log row shows the contact dropped
from both the candidates mart and `contact_attributes_hubspot`.

Why it matters: Airbyte lags HubSpot. Until the next sync, a merged-away contact looks
live, and a model that writes by contact ID can write the secondary's values onto the
survivor. Reading the log closes that gap for every model at once.

### Step 20: add the `hubspot_contact_merge_queue` view (DATA-2796)

Waits on 4-5, 6 and 19. Blocks 22-23 and 25.

A view over the candidates mart. Keep a row only when it is a secondary row,
`exact_match` is true, `has_unique_key_conflict` is false, `live_contact_count` is under
an agreed cap, it is not in the exclusion seed, and the secondary has no status in the
log that the queue drops (see the status table). Return no rows at all while the log has
an `id_changed` row that nobody has reviewed. Decide how a review is recorded, for
example a seed of reviewed run IDs, and document it. Add `latest_election_date`, cast to
a date, from the person's candidacies in `mart_civics`. Return exactly the five pair
columns. Document every rule in the model YAML. Test that no secondary appears twice and
that no secondary equals its primary.

Done when the row count by election year is on the ticket.

### Step 21: add log health tests (DATA-2797)

Waits on 18. Blocks 22-23.

dbt tests on the log source: error on any `id_changed` row, warn when a pair has `failed`
in three or more runs, and warn on any `merged_elsewhere` row.

Done when the tests run in the scheduled build at those severities.

### Steps 22-23: the first production merges, by hand (DATA-2521)

Waits on 1, 3, 16, 17, 20 and 21. Blocks 27-28 and the Office Holder feed (DATA-2645).
A merge cannot be undone.

Use "A hand-checked batch" in the DAG runbook, on astro-prod.

- Step 22: pick five low-risk pairs from the queue (same phone, same name, no owner
  conflict, no app account) and check each in the HubSpot UI. Put them in a table with
  the five pair columns. Trigger `dry_run`, `run`, then `verify` with `pair_relation` set
  to that table. Check the survivors in the UI, including any workflow enrollment. Done
  when the results are on the ticket and the five have left the queue.
- Step 23: the same, with 100 rows from the queue across a mix of `match_evidence`
  values. Spot-check ten. Done when there is no `failed` or `id_changed` row, or each one
  is explained on the ticket.

### Step 25: measure the primary Company conflict population (DATA-2799)

Waits on 20. Blocks 27-28.

Count the queued pairs where both contacts have a primary Company association and the
two differ. Decide, on the ticket, whether the job must re-set the primary Company after
a merge before the cap passes 1,000.

### Steps 27-28: turn on nightly merges and raise the cap (DATA-2562)

Waits on 22-23, 25, 26, DATA-2572 and DATA-2647.

- Step 27: on astro-prod, set `hubspot_merge_pair_relation` to the queue view and
  `hubspot_merge_max_pairs` to `100`, then unpause. Give the log source a real
  freshness threshold. Done when three scheduled runs finish with clean logs.
- Step 28: raise the cap in agreed steps, for example 500, then 2,000, then the whole
  queue, each after a clean week. Watch the log tests and check that the candidates count
  falls. The two gates below must be done before the cap passes 100. Done when the
  exact-match queue is empty apart from rows the log explains.

## Gates and related work

Gates for step 28 (the cap passing 100):

- [DATA-2572](https://app.clickup.com/t/17tnw2b061r): Engineering audits gp-api's HubSpot
  Contact lookups, and confirms nothing writes `hs_additional_emails`.
- [DATA-2647](https://app.clickup.com/t/17tnw2b5en7): Product decides whether a
  merged-away contact still verifies a campaign. The fix, if wanted, reads
  `int__hubspot_contact_merges`.

Not a gate: [DATA-2571](https://app.clickup.com/t/17tnw2b061a), Joe's test of a Segment
sync that keeps a contact's primary email current. It would close a gap of 216 contacts
whose HubSpot email differs from their gp-api email.

Waiting on this plan:

- [DATA-2522](https://app.clickup.com/t/17tnw2ayhem): make `gp_person_id` unique by
  stamping it only on each person's primary. Step 3 is done, so it can start once the
  proposal is agreed.
- The Office Holder work (DATA-2683, DATA-2645, DATA-2684). Its first production writes
  wait on steps 22-23.

## Not yet checked

- Whether the `reverse_etl_hubspot_log_table` Variable is set on both deployments, and
  whether the `reverse_etl` schema is dropped (step 29's operational part).
- Whether the installed `databricks-sql-connector` accepts the CLI's
  `credentials_provider` shape when run from the image. Only a laptop run has used the
  real connection.
- Whether the prod Airflow principal can read pair tables in personal dev schemas.
- Whether the GitHub token can delete `pr-N` image versions in the cleanup job.

## Working notes

- Branch from a fresh `origin/main`. Name branches `data-NNNN/short-name` and PR titles
  `[DATA-NNNN] ...`.
- Ask for review by commenting `delegate review` on the PR. Reply to any comments, push,
  and comment again until it is approved.
- In ClickUp, adding a task to a sprint list needs the task's internal ID (such as
  `17tnw2bcp8v`). The custom ID (`DATA-2792`) fails with "must be in the same
  workspace". Add each finished ticket to the current sprint list and keep its home in
  Data Backlog.
- Databricks CLI logins expire. Run `databricks auth login` before you query tables.
- Open clean-up: merge #1335 (approved), and close #1334, an old copy of #1336.
