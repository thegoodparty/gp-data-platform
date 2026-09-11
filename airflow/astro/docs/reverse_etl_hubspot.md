# Reverse-ETL HubSpot contact sync, daily DAG (`reverse_etl_hubspot`)

Operational reference for the DAG that keeps HubSpot's contacts in sync with Databricks: one
daily task diffs the desired-state contact model against what HubSpot already holds, upserts
whatever differs, and logs what HubSpot confirmed. One contact sync carrying every property we
own, not a lead feed, so a new attribute is a column on its model rather than a second flow.

## What it does

Daily at 17:00 UTC: `send_pod` runs the `retl` image as a Kubernetes pod, executing
`retl --source=hubspot --destination=hubspot_contacts`. Everything about the diff — reading
the desired-state model, subtracting the flow's own send log, the volume and empty-log guards, the
batch upsert, appending confirmed payloads back to the log — happens inside that container. This
DAG's job is narrower: resolve the pod's credentials and config at task runtime, run it, and turn a
bare pod failure into one an operator can act on.

The daily invocation passes **neither** `--init-log` **nor** `--accept-empty-log`. Both are
ceremony-only (see "The one-time init ceremony" below); passing either on the daily schedule would
permanently disarm the guard that catches a lost or mis-pointed send-log table silently re-sending
the flow's entire population.

## Reading a failure

Alerting is the existing failed-DAG Slack alert; there is no other machinery. The task's own
exception text carries what happened:

| What the failure text shows | What happened | What to do |
|---|---|---|
| `retl FAILED: flow '...' is missing required environment variable ...` in the tail | An Airflow Variable this DAG reads is unset. | Set the named Variable; the next scheduled or manual run picks it up. |
| `retl FAILED: ... source model returned zero rows` | The desired-state model built empty. | Check the model's own dbt run before re-triggering; a broken upstream join must not read as a quiet day, so this is a hard stop. |
| `retl FAILED: ... has no logged rows for this flow` | The empty-log guard fired: the flow's send-log table is empty despite prior successful runs (lost, dropped and recreated, or a `LOG_TABLE` Variable pointed at the wrong table). | Investigate before doing anything else — this is the guard that stops a silent full re-send. Only run `retl --init-log` / a deliberate reset once you understand why the log came back empty. |
| `retl FAILED: ... exceeds cap ...` | The diff is larger than the flow's configured cap. | A viability recompute rewriting most rows is an expected loud day and should already fit under the cap; if it doesn't, or the source looks wrong, investigate before raising the cap. |
| `partial progress: N row(s) logged to ... since this run started` | Appended after the failure text: how many rows THIS run's earlier batches already confirmed and logged before a later batch killed the run. Distinguishes a died-at-batch-1 run from a died-at-batch-700 run — retl's own summary line never prints on an aborted run. | Use it to judge urgency; nothing to log further yourself — already-confirmed rows are safe and will not resend. |
| `partial progress probe failed: ...` | The partial-progress check itself could not reach the warehouse (transient) — the pod failure it is attached to is still the real story and is unaffected. | Read the underlying pod failure; retry as normal. |

Any other pod failure (crash, OOM, timeout, a HubSpot outage) carries the pod's last log lines (up to
40, trimmed to roughly 4000 characters) in the exception text, so the alert is readable without
opening the pod log.

## The day's geometry

The scheduled dbt build runs 00:02 and 12:02 UTC and takes 95-105 minutes, so the 12:02 build lands
~13:47. `gold_match_daily` runs 14:30 UTC and its post-write dbt rebuild can run to ~16:30. The
election-api sync runs 00:00 UTC. 17:00 sits roughly 3 hours after the 12:02 build's worst-case
completion, after gold-match's rebuild window, and 7 hours before the 00:00 sync — 13:00/12:00 US
Eastern, so a failure alert reaches a human the same working day and this task's own retries still
finish inside that margin.

There is deliberately **no dbt gate** in front of this task: a stale mart is self-correcting, because
yesterday's confirmed payloads are already logged and an unchanged mart produces an empty diff. Do
not add one.

## Retries

The DAG default (`retries=2`, 10-minute delay) applies to `send_pod` unmodified: the send log
checkpoints after every confirmed batch, so a retry only ever sends the remainder, never re-sends
what already succeeded.

## Re-running

A re-run is a new manual trigger. The task carries no run-key state of its own (unlike
`gold_match_daily`); a manual trigger during a scheduled run queues rather than starting a second
writer against the flow's log table (`max_active_runs=1`).

## The one-time init ceremony

`retl --init-log` creates the flow's send-log table if it does not exist yet (stamped with the
flow's identity) and exits without running a diff. It is run **by a human**, from outside this DAG,
before the flow's first-ever run and again only after a deliberate, supervised reset. The daily task
never passes `--init-log`, and never passes `--accept-empty-log` either (the explicit override for a
real run against a genuinely empty log — the first convergence, the sales preview, or a supervised
post-reset run). Force-resending a person is deleting their rows from the flow's own log table, not
re-running any ceremony.

## Variables and connections

Set on the Astro deployment:

| Variable | Purpose |
|---|---|
| `databricks_conn_id` | Selects the Databricks connection (`databricks_dev` / `databricks`). |
| `reverse_etl_image_tag` | REQUIRED, no default: the merged build's sha. An unattended loop on `latest` would silently run whatever main last published after every merge, so the evaluated artifact must be the production artifact — a merge changes nothing until this Variable is deliberately bumped to the new build's sha. Unset fails the run loudly at render, before any pod runs. |
| `reverse_etl_image_pull_secret` | Kubernetes image pull secret name from Astronomer support. The GHCR package is private, so a deployment needs this set before any pod can run there; empty leaves the pod pulling anonymously (fails at the registry, not silently). |
| `reverse_etl_hubspot_token` | HubSpot private-app token for the batch contact upsert ("token" in the name so the secrets masker redacts it). |
| `reverse_etl_hubspot_source_relation` | The desired-state model's fully qualified relation name. |
| `reverse_etl_hubspot_excluded_columns` | Comma-separated columns the payload never carries — must include the model's build-clock column, or every build resends its entire population. |
| `reverse_etl_hubspot_cap` | The flow's send-cap; sized at enable time to admit the full eligible population without tripping on a legitimate recompute day. |
| `reverse_etl_hubspot_log_table` | This flow's own send-log table. No default: a default would point a dev deployment at the production log. |

**Connections:** `databricks` / `databricks_dev` (Generic, OAuth M2M), shared with the other DAGs.
The pod's credentials resolve in `pre_execute` (never in the rendered-template snapshot), mapped to
retl's own env names (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLIENT_ID`,
`DATABRICKS_CLIENT_SECRET`) — retl imports no Airflow code, so every value crosses as a plain
environment variable.

**Resources:** one 2Gi / 1 CPU pod per run (requests == limits, Guaranteed QoS), at most one run at a
time.

## Which build a run used

`image_pull_policy: Always` plus the tag Variable: the `send_pod` log's provenance line says whether
the image was pinned to a reproducible sha or running on a mutable tag.

## Dev rehearsals

A dev-deployment rehearsal validates plumbing: the DAG parses, the pod pulls and boots, credentials
resolve. It needs its own namespace pull secret before any pod can run there (the package is
private). Do not let a dev-triggered run write to the production HubSpot contacts or the production
send-log table — point its Variables at a sandbox portal and a scratch log table first.

## Activation

The DAG deploys `is_paused_upon_creation=True` on every deployment. Unpausing the schedule is a
separate, owner-gated activation step (backfill/convergence preview, the sales-owned field list
ratified, the sandbox battery passed) — deploying this code activates nothing.
