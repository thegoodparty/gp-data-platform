# Gold-match daily DAG (`gold_match_daily`)

Operational reference for the DAG that matches the day's pending offices in the gold-match
container and leaves publication to the scheduled nightly build. It triggers nothing else and
deletes nothing.

## What it does

Daily at 14:30 UTC: `admission` asks dbt Cloud two questions and declines the day, writing nothing, unless
both answers are good: did the latest SCHEDULED run of the prod build (job 70471823431462) succeed, and is
no run of that job or the on-merge build (job 70471823431463) queued or running; it also declines when
dbt Cloud cannot be asked. `match_pod` then runs the gold-match image as a Kubernetes pod, executing the
unattended entry point (`stitch_golden_data.prod_gold_data.daily_run`) with this DagRun's own start
timestamp as the run key. `operator_signal` is a notification-only leaf: it fails the DAG (nothing
deleted) when a human should look at something: a declined day, or offices that first entered quarantine
this run.

That is the whole DAG. It triggers no dbt build and deletes nothing: the rows the pod writes reach the
internal marts at the next scheduled build and the product at the following election-api sync (see the
geometry below), and a wrong row found in the daily audit is removed by the operator, by key, with its
office quarantined, before that build. The nightly build's tests page; they do not roll back (below).

The cohort is post-cutover only: an office whose latest attempt predates the 2026-08-31 cutover run
belongs to a separately gated, supervised population and never enters this loop (the first automated
pass over that population, 2026-09-14, matched most of it wrongly and was rolled back).

The pod matches the whole cohort in memory, then appends the results (in chunks that each commit
on their own), then writes the quarantine upserts and the run-log row. A pod that dies mid-match
has written nothing; one that dies during or after the append leaves rows under its run key and
no run-log row (see the `match_pod` row below). One run key has exactly one writer (`retries=0` on
the pod, `max_active_runs=1` on the DAG, and the pod is deleted at task termination).

## Reading a failure (the task name carries the story)

Alerting is the existing failed-DAG Slack alert; there is no other machinery. When the DAG fails,
read which task failed:

| Failing task | What happened | What to do |
|---|---|---|
| `match_pod` | Pod crash, timeout, the cohort ceiling (a pending list over 20k is a de facto full re-match), or the quarantine circuit breaker (>10 response-shape failures in one run). | Check whether the pod got as far as writing: `select count(*) from model_predictions.llm_l2_br_match_results where attempted_at = <run key>` against a run-log row for the same key. Rows without a run-log row are an incomplete run: delete them by key before the 00:02 build (nothing automated does this any more). No rows: nothing to undo. Tomorrow retries either way. Ceiling or circuit breaker means something systemic — read the pod log's last lines before tomorrow's run. |
| `operator_signal` with "publication declined at admission" | The day was declined before anything was written, for one of three reasons named in the message: the latest scheduled prod build did not succeed (a red nightly: the universe and the marts are yesterday's; if the matcher's own rows made it red, the loop has paused itself), another prod build was in flight, or dbt Cloud could not be asked. | Nothing to undo. A red nightly: find the failing node in dbt Cloud; if it is the matcher's rows (the coverage floor or the label check), delete the offending rows by key and quarantine the offices, and the next nightly clears them; otherwise it is someone else's red and tomorrow retries. In flight: tomorrow retries, or trigger a manual run once it finishes. |
| `operator_signal` with "first entered quarantine" | Offices FIRST entered quarantine this run (the pod's response-shape failures; the operator's own `adjudicated_wrong` holds are not counted). Nothing deleted; the run's rows stand. | Check the quarantine table for the new rows. |

Two things that are NOT DAG failures but belong here:

| Situation | What serves | Who acts |
|---|---|---|
| A red nightly (the 00:02 or 12:02 scheduled build failed) | Depends on WHERE it went red. dbt materializes a model before it tests it, so a failing test (the voter-coverage floor, say) leaves the mart it tested already rebuilt, with the run's rows in it, and skips only the nodes downstream; a model that errored leaves its previous table in place. The sync reads tables, not dbt status, so a red nightly is a page, not a rollback. The next day's admission declines until a scheduled build succeeds, so the loop writes no new rows meanwhile. | dbt Cloud's own failure alert is the page. If the matcher's rows caused it, remove them (below) before 12:02; if the product must not see what the 12:02 build will carry, flip `election_api_swap_enabled` to false before the 22:00 sync (the existing product-side stop; the sync's own quality checks are the other). Otherwise the owner of the failing model. |
| Wrong rows found in the daily audit | Nothing yet, if the audit finishes before the 00:02 build; the internal marts from 00:02 and the product from the following 22:00 sync otherwise. | The operator, on the owner's decision: `delete from model_predictions.llm_l2_br_match_results where attempted_at = <run key> and br_database_id in (...)`, then insert the offices into the quarantine table as `held` with reason `adjudicated_wrong` (stamped with the run key). Expect the deleted rows gone from the marts at the next scheduled build. |

## The day's geometry (publication is next-day, by design)

- Day D 08:00 UTC: the L2 load; 12:02: the scheduled build (ends ~13:47) moves the universe.
- D 14:30: this DAG. Admission reads dbt Cloud; the pod writes at ~14:45 (a wave day's ~8k offices add
  roughly an hour). The rows sit in `model_predictions.llm_l2_br_match_results` only.
- D+1 00:02: the scheduled nightly lands them in the INTERNAL marts: the ICP flags (which feed lead
  sourcing, so "internal only" is not harmless), the zip funnel, Serve's district resolution, the
  position and district marts.
- D+1 12:02: the second scheduled build, the one the sync reads.
- D+1 22:00: the election-api sync publishes them to the product. Latency ~31 hours (the old post-write
  rebuild made it ~7; the owner ruled 2026-09-18 that a day or three of latency is fine in exchange for
  no rebuild).

Two audit deadlines, both the operator's: **before 00:02 D+1 keeps the internal marts clean**; **before
~12:02 D+1 keeps the product clean** (the backstop, not the target). The daily rule: audit the run the
same day; confirmed wrong rows are deleted by key and their offices quarantined before 00:02.

The nightly's tests (the voter-coverage floor at error severity, the staging label check at warn)
page on the run's rows but do not roll them back: the tested mart is rebuilt before its test runs
(see the red-nightly row above). The stops, in order, are the operator's same-day audit with
delete-by-key before 00:02, the sync's own quality checks, and the `election_api_swap_enabled`
Variable. A red nightly does pause the loop: the next day's admission declines until a scheduled
build succeeds. There is deliberately no dependency wiring to the sync and no dbt trigger in the DAG;
neither schedule should drift without rechecking this section.

## Retries

`admission` and `match_pod` run one attempt (`retries=0`): admission's checks are snapshots, so a retry
minutes later could admit a day the first attempt declined; the pod's daily retry is tomorrow's run.
`operator_signal` keeps the default two retries: it is a cheap idempotent read, and retrying a transient
query failure is what stops a healthy run from raising a false alarm over a network blip.

## Re-running

A re-run is a NEW manual trigger — never clear tasks inside an old run. The run key is the
DagRun's own start timestamp; a manual trigger during a scheduled run queues
(`max_active_runs=1`) rather than starting a second writer. Admission applies to manual runs too.

## Variables and connections

Set on the Astro deployment:

| Variable | Purpose |
|---|---|
| `databricks_conn_id` | Selects the Databricks connection (`databricks_dev` / `databricks`). |
| `gold_match_image_tag` | REQUIRED (no default, on both deployments): the sha of the gate-passed build. An unattended loop on `latest` would silently run whatever main last published after every merge, so the evaluated artifact IS the production artifact here: a matcher merge changes nothing until this Variable is deliberately updated to the new build's sha post-gate. Unset fails the run loudly at render; `latest` works only as an explicit override for supervised debugging. |
| `gold_match_image_pull_secret` | Kubernetes image pull secret name from Astronomer support (`ghcr-pull`). The GHCR package is private, so this is required, not optional. Provisioning and ROTATION: see `ghcr_pull_credential.md` in this directory — the backing token expires yearly and every private pull breaks at once when it does. |
| `BRAINTRUST_API_KEY` | Injected into the pod. The pinned prompt fails closed without it, so leaving it unset fails the run at pre_execute rather than mid-cohort. |
| `gold_match_aws_role_arn` | The GoodParty-account IAM role the pod assumes to call Bedrock (`gold-match-bedrock-<env>`, created by gp-terraform-dataplatform). The pod's own AWS identity is the deployment's Astronomer-managed workload identity, which lives in Astronomer's account and cannot hold the grant, so the role in our account trusts it and the pod assumes it with self-refreshing credentials. Unset fails the run at pre_execute. |
| `gold_match_aws_external_id` | The `sts:ExternalId` that role's trust policy requires (the loader's per-environment id; a fixed nonce, not a credential). Unset fails the run at pre_execute. |

**Connections:** `databricks` / `databricks_dev` (Generic, OAuth M2M) and `dbt_cloud` (read-only here:
admission lists runs), shared with the other DAGs. The pod's credentials resolve in `pre_execute` (never in the rendered-template
snapshot), mapped to the gold-match client's env names (`DATABRICKS_SERVER_HOSTNAME`, bare host).

**Resources:** one 8Gi / 4 CPU pod per run (requests = limits, declared in the DAG code, never by a
shared Variable), at most one run at a time, so no pool is needed. The deployment KPO quota is a
ceiling across all running pods and lives in gp-terraform-dataplatform (`resource_quota_cpu` /
`resource_quota_memory`); matcha's pod size lives in `matcha_er.py`. Both moved in September 2026, so
read them at source rather than trusting a number here. Pods are billed on their configured limits, so
8Gi / 4 CPU is a placeholder until the first production runs show peak usage.

## Which build a run used

`image_pull_policy: Always` plus the tag Variable: the `match_pod` log's provenance line says
whether the image was pinned. The image also bakes `GIT_SHA` at build, which the entry point
records in its run-log row — so every written run carries its own code provenance. In normal
operation the Variable is always a pinned sha (see the table above), so the mutable-tag warning
fires only when someone explicitly overrides to `latest` for supervised debugging.

## Bookkeeping tables (matcher-owned; nothing on the serving path reads them)

- `model_predictions.llm_l2_br_match_run_log` — one row per successfully written run: cohort and
  policy counts, model/prompt provenance, git sha. The first place to look when asking "what did
  yesterday's run decide".
- `model_predictions.llm_l2_br_match_quarantine` — per-office response-shape failures. `auto`
  rows retry after 30 days; `held` rows release only by a client fix or a manual UPDATE (UC ACLs
  govern who). Nothing deletes quarantine rows; they are the record `operator_signal` reads.
  A second kind of `held` row is written by hand: `reason_code = 'adjudicated_wrong'` marks an office
  whose match the run audit adjudicated wrong on the pinned build, stamped with the audited run's key,
  so the office leaves the next cohort and cannot re-fail deterministically; `operator_signal` ignores
  these rows. Release path when the quality lane re-pins: `update ... set released_at =
  current_timestamp(), release_note = '<build sha>' where reason_code = 'adjudicated_wrong' and
  released_at is null`; the offices re-enter as never-attempted on the next run.

## Dev rehearsals

A dev-deployment rehearsal (branch-mapped deploy) validates plumbing: the DAG parses, the pod
pulls and boots, credentials resolve. Do NOT let a dev-triggered run execute a real match: the
matcher's tables are production-only by construction, so there is no dev sandbox behind it. The
end-to-end proof is the supervised, owner-gated activation drill.

## Activation

The DAG deploys `is_paused_upon_creation=True` on every deployment. Unpausing the schedule is a
separate, owner-gated activation checklist (config, tables provisioned, alert registration, a
timed wave-scale drill) — deploying this code activates nothing.
