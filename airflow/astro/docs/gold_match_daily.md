# Gold-match daily DAG (`gold_match_daily`)

Operational reference for the DAG that matches the day's pending offices in the gold-match
container, rebuilds the warehouse so the results serve, gates the published labels, and repairs
itself on any failure.

## What it does

Daily at 14:30 UTC: `match_pod` runs the gold-match image as a Kubernetes pod, executing the
unattended entry point (`stitch_golden_data.prod_gold_data.daily_run`) with this DagRun's own
start timestamp as the run key; `rebuild` triggers the scheduled dbt Cloud build (job
70471823431462) so the new rows flow into the serving tables; `gates` re-checks the published
labels. Any failure among those three routes to `cleanup_finalizer`, which cancels a still-live
rebuild (and confirms it terminal), deletes the run's rows by key, ALWAYS triggers a repair
rebuild, and re-raises — so serving is back to yesterday's state and the DAG run ends FAILED.
`operator_signal` is a separate notification-only leaf: it fails the DAG (nothing deleted) when a
human should look at something that is not this run's fault. Tomorrow's scheduled run is the
retry for everything self-healing.

The pod writes at end: a pod that dies mid-match has written nothing, and one run key has exactly
one writer (`retries=0` on the pod, `max_active_runs=1` on the DAG, and the pod is deleted at
task termination).

## Reading a failure (the task name carries the story)

Alerting is the existing failed-DAG Slack alert; there is no other machinery. When the DAG fails,
read which task failed:

| Failing task | What happened | What to do |
|---|---|---|
| `match_pod` | Pod crash, timeout, the cohort ceiling (a pending list over 20k is a de facto full re-match), or the quarantine circuit breaker (>10 response-shape failures in one run). | Self-healing for crashes/timeouts: cleanup ran, tomorrow retries. Ceiling or circuit breaker means something systemic — read the pod log's last lines before tomorrow's run. |
| `rebuild` | The dbt build failed — including the voter-coverage floor, which runs in this job at error severity. | Cleanup cancelled/deleted/repaired. Find the run in dbt Cloud by its cause string ("gold-match daily: post-write rebuild"). A failure that repeats daily is deterministic: fix at source. |
| `gates` | THIS run matched district labels that are dead in the current universe. | Cleanup deleted the run. Investigate the universe churn (the run-scoped label check in the error message names the count). |
| `operator_signal` | Nothing was deleted. Either offices FIRST entered quarantine this run, or dead labels from OLDER runs exist in the serving state (deleting today's run cannot clear those). | Check the quarantine table for the new rows; for older dead labels, repair at source. The rest of the run may have succeeded. |
| `cleanup_finalizer` | Read its ERROR MESSAGE — this task fails by design even when cleanup worked. "cleanup completed after an upstream failure" means the delete and repair rebuild SUCCEEDED: nothing more to do here, read the failed upstream task's row instead. Any other message (cancel unconfirmed, delete raised, repair rebuild failed) is the one genuinely urgent story. | For the urgent messages only: if a run failed after writing and its cleanup cannot be confirmed, flip `election_api_swap_enabled` to false before the 22:00 UTC sync until repair lands (the existing product-side hard stop; no new wiring). |

## The day's geometry

The 08:00 L2 load and the 12:02 scheduled build (ends ~13:47) move the universe; this DAG runs
14:30; a steady-state write lands ~14:35 (a wave day's ~8k offices add roughly an hour); the
post-write rebuild ends ~16:25-17:30; gates follow. The election-api sync runs at 22:00 UTC (it
deliberately precedes the dev deployment's 01:00 hibernation). A typical failure day's delete +
repair rebuild ends ~18:20-19:30, comfortably before the sync; the one theoretical path that can
brush 22:00 is a repair rebuild running out the finalizer's full 4h ceiling after a late gate
failure, and the swap-gate rule in the table above exists for exactly that case. The margin is
the sync-race answer — there is deliberately no dependency wiring to the sync, and neither
schedule should drift without rechecking this section.

## Retries

`match_pod`, `rebuild`, and `cleanup_finalizer` run one attempt (`retries=0`): the daily retry is
tomorrow's run, a rebuild retry loop would erase the pre-sync margin above, and the finalizer must
not loop a ~100-minute repair cycle toward the 22:00 sync. `gates` and `operator_signal` keep the
default two retries — they are cheap idempotent reads, and retrying a transient query failure is
what stops a healthy (paid) run from being destroyed over a network blip.

## Re-running

A re-run is a NEW manual trigger — never clear tasks inside an old run. The run key is the
DagRun's own start timestamp; a manual trigger during a scheduled run queues
(`max_active_runs=1`) rather than starting a second writer.

## Variables and connections

Set on the Astro deployment:

| Variable | Purpose |
|---|---|
| `databricks_conn_id` | Selects the Databricks connection (`databricks_dev` / `databricks`). |
| `gold_match_image_tag` | REQUIRED (no default, on both deployments): the sha of the gate-passed build. An unattended loop on `latest` would silently run whatever main last published after every merge, so the evaluated artifact IS the production artifact here: a matcher merge changes nothing until this Variable is deliberately updated to the new build's sha post-gate. Unset fails the run loudly at render; `latest` works only as an explicit override for supervised debugging. |
| `gold_match_image_pull_secret` | Kubernetes image pull secret name from Astronomer support (`ghcr-pull`). The GHCR package is private, so this is required, not optional. Provisioning and ROTATION: see `ghcr_pull_credential.md` in this directory — the backing token expires yearly and every private pull breaks at once when it does. |
| `BRAINTRUST_API_KEY` | Injected into the pod. The pinned prompt fails closed without it, so leaving it unset fails the run at pre_execute rather than mid-cohort. |

**Connections:** `databricks` / `databricks_dev` (Generic, OAuth M2M) and `dbt_cloud`, shared with
the other DAGs. The pod's credentials resolve in `pre_execute` (never in the rendered-template
snapshot), mapped to the gold-match client's env names (`DATABRICKS_SERVER_HOSTNAME`, bare host).

**Resources:** one 8Gi / 4 CPU pod per run, at most one run at a time — no pool needed. Worst-case
coexistence with matcha's serialized weekly 8Gi pods is 16Gi against the 20GiB deployment quota.

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
  govern who). Cleanup never deletes quarantine rows; they are the record `operator_signal` reads.

## Dev rehearsals

A dev-deployment rehearsal (branch-mapped deploy) validates plumbing: the DAG parses, the pod
pulls and boots, credentials resolve. Do NOT let a dev-triggered run execute a real match: the
matcher's tables are production-only by construction, so there is no dev sandbox behind it. The
end-to-end proof is the supervised, owner-gated activation drill.

## Activation

The DAG deploys `is_paused_upon_creation=True` on every deployment. Unpausing the schedule is a
separate, owner-gated activation checklist (config, tables provisioned, alert registration, a
timed wave-scale drill) — deploying this code activates nothing.
