# People API loader DAG (`load_people_api`)

Design and operational reference for the DAG that refreshes the People API serving database
on a fresh Aurora Postgres cluster from the L2 voter marts in Databricks. The loader builds
four tables: `Voter` and `DistrictVoter` (partitioned by State), and `District` and
`DistrictStats` (flat).

Ticket: DATA-1913 (DAG orchestration), epic DATA-1640 (People API data-loading revamp). This
document reflects the implementation on branch `feat/DATA-1913-loader-dag` / PR #536.

## What it does

The loader is a Python CLI (`people-api-loader/`, `loader <step> --date <ds_nodash>`) that follows a
train-deployment model: every refresh provisions a brand new Aurora cluster, loads and indexes it,
resizes it to Serverless v2, and (at cutover) swaps it in as the serving database. The existing
serving cluster is never mutated in place.

The DAG is a thin sequencer. All parallelism lives inside the loader; each Airflow task is a single
`BashOperator` invocation of one CLI subcommand. State flows between steps through S3 manifests
(`s3://<bucket>/voter_export_<date>/_manifest/<step>.json`), not xcom, so each step is
independently runnable and re-entrant for a given run date: a step short-circuits if its manifest
already exists.

## Serving schema (`public`, not the Prisma `green` schema)

The loader builds every table into the `public` schema — the data-platform serving replica — not the
Prisma `green` OLTP schema. The people-api Prisma models declare all four tables `@@schema("green")`,
and prod's `green.District` carries a `@@unique([type, name, state])` that the loader does **not**
reproduce. `extract-serving-structure` and every step scope to `nspname='public'`, so
`_serving_seed.py` mirrors the `public` replica (verified byte-identical to prod on 2026-07-16, so it
is not a wrong-schema bug — Voter has always been served the same way). Whether the `public` District
replica should gain that composite unique / DistrictVoter's secondary indexes is a DATA-1855 cutover
decision — it only matters if the serving read path does `ON CONFLICT (type, name, state)` upserts —
not part of this loader.

## Step sequence

```
inspect_prod -> dbt_test_voter_gate -> unload -> provision -> create_schema -> copy
             -> build_indexes -> resize -> validate
```

| Step | Purpose |
|---|---|
| `inspect_prod` | Capture the current prod per-state row counts and L2 snapshot dates as the validation baseline. Re-queries prod each run (does not short-circuit on a manifest). |
| `dbt_test_voter_gate` | Run the DATA-1906 voter tests in dbt Cloud (`DbtCloudRunJobOperator`, `steps_override=["dbt test --select m_people_api__voter"]`). Runs locally-in-dbt-Cloud because dbt cannot run on the image's Python 3.14. Always runs (no manifest). |
| `unload` | Export the voter marts per state to S3 (Databricks Statement Execution API, SQL warehouse only). |
| `provision` | Create a fresh Aurora cluster + writer on the **copy-phase** instance class, empty cluster parameter groups, connection string to SSM. Reuses the shared S3 gateway VPC endpoint. |
| `create_schema` | Apply CREATE TABLE statements from the committed snapshot for all four tables: `Voter` and `DistrictVoter` (partitioned by State with per-state LIST children) and `District` and `DistrictStats` (flat). |
| `copy` | Parallel server-side `aws_s3.table_import_from_s3`: for partitioned tables (Voter, DistrictVoter), per-state per-file copy with rows routed to partitions by `"State"`; for flat tables (District, DistrictStats), whole-table copy. Idempotent per state/table (count / DELETE + reload). |
| `build_indexes` | Scale the writer up to the **index-phase** class, then add primary keys and indexes for all four tables: for partitioned tables (Voter, DistrictVoter) build per-partition indexes and attach to parent; for flat tables (District, DistrictStats) build parent-level indexes. Then `ANALYZE` all tables. See below. |
| `resize` | Flip the writer to Serverless v2 with the prod ACU range, swap in the serve parameter group, bump backup retention, enable deletion protection. |
| `validate` | Row counts vs the `inspect_prod` baseline (per-state for partitioned tables within +/-10%, whole-table for flat tables), plus per-table schema/index structural checks (with `:<table>` suffix in check names). Failure halts the DAG. |
| `scale_down_on_failure` | Not in the happy path — a `trigger_rule=one_failed` branch off `provision`→`resize`. On any post-provision failure it runs `loader scale-down`, flipping the writer to `db.serverless` to stop provisioned-instance cost. Skipped on a successful run. See "Failure cost guard" below. |

## Connectivity: bastion

The Astro worker is outside the VPC, so the loader reaches private Aurora through the
`gp_bastion_host` SSH tunnel. `db.py` opens one shared tunnel per step and multiplexes all
connections as channels over a single SSH transport (a fresh tunnel per connection floods sshd
`MaxStartups`). TLS-safe: the real RDS host is kept for SNI/cert verification and the local forward
is dialed via `hostaddr`, so `sslmode=verify-*` works. With `LOADER_BASTION_*` unset the loader
connects directly (local/VPN). `COPY` stays server-side; boto3 uses the cross-account
`gp-people-rds-admin-*` role; Databricks uses one OAuth M2M credential.

## Instance sizing (two-tier)

`build_indexes` is the only CPU-bound step; `provision`, `create_schema`, and `copy` are trivial or
I/O/WAL-bound. So the loader uses two instance classes and scales between them:

- `load_instance_class` = `db.r8g.16xlarge` (64 vCPU): the box `provision` creates; carries
  provision, create_schema, copy.
- `index_instance_class` = `db.r8g.48xlarge` (192 vCPU): `build_indexes` scales the writer up to
  this at its start (`_ensure_instance_class`), then `resize` flips down to Serverless v2.

Overridable via `LOADER_LOAD_INSTANCE_CLASS` / `LOADER_INDEX_INSTANCE_CLASS`.

**Cost logic.** Index building is CPU-bound and embarrassingly parallel, so its cost in
instance-hours is roughly flat across instance sizes for the parallel bulk (a smaller box just takes
proportionally longer at a proportionally lower rate). The serial tail (PK add, ANALYZE, the few
giant-partition builds) has a fixed wall-time and makes a bigger box strictly more expensive, since
you pay for idle cores during it. So the cost-minimizing index box is the smallest one whose
wall-clock you can tolerate; do not upsize past what actually fills. Storage is `aurora-iopt1`
(I/O-Optimized), which is correct for the write-heavy load and build.

## Index build design

The partitioned tables (`Voter` and `DistrictVoter`) are LIST-partitioned by `"State"` (51 children each). `CREATE INDEX` on the partitioned parent recurses through every partition serially inside one statement, so plain indexes are built **per partition** instead: `CREATE INDEX ... ON ONLY` the parent (instant) + one child index per partition as independent `(index, partition)` work units, then `ALTER INDEX ... ATTACH PARTITION`. PKs and uniques stay parent-level builds. Flat tables (`District` and `DistrictStats`) have all indexes built at the parent level.

Two things make this fast and cheap:

1. **Largest-partition-first scheduling.** Postgres grants each `CREATE INDEX` its parallel workers
   at statement start, first-come, against the shared pool. With naive `(index, state)` ordering the
   giant partitions (CA ~4.7M blocks, TX/FL/NY in Voter; similar distribution in DistrictVoter)
   launched mid-flood and were starved to ~1 worker for hours while tiny partitions grabbed 5-8.
   `_order_children_largest_first` sorts units by `pg_relation_size(partition)` descending so the
   giants launch first into an open pool and grab their full worker allotment; the thousands of
   near-empty partitions backfill. This scheduling applies across all partitioned-table units.
2. **Filling the box.** Aurora defaults `max_parallel_workers` to about vCPU/2 (96 on the 192-vCPU
   box), which capped the build at ~125 active backends with ~67 cores idle. The build session sets
   `max_parallel_workers = 176` and `max_parallel_maintenance_workers = 16` (both user-context GUCs;
   `max_worker_processes` is 384, so no reboot-class change). To retune a live cluster without a
   redeploy, modify the load cluster parameter group (`max_parallel_workers` is dynamic); Aurora
   blocks `ALTER SYSTEM`.

Measured: a full build dropped from a ~30-hour trajectory to ~83 minutes on the 48xlarge with these
changes (run `manual__2026-07-09T20:30`, validated end-to-end).

**Scale-up reboot and shared control-plane helpers.** Changing the instance class reboots the
writer. `_ensure_instance_class` waits for the class change to fully apply (polls until the reported
class equals the target, status is available, and no class change is pending) before building,
because Aurora reports `available` for a few seconds after the modify before it actually reboots.
That poll and the tolerate-in-progress-fault retry are shared helpers in `core/aws.py`
(`wait_instance_class_applied`, `retry_after_settle`), reused by `resize`'s Serverless v2 flip (which
had the same stale-available reboot race) and by the on-failure scale-down. Airflow retries also
cover a mid-build drop since every step is idempotent for its date.

## Failure cost guard

`resize` (which flips the writer to Serverless v2) is downstream of `build_indexes`, so a run that
fails or is aborted mid-build never reaches it and would otherwise strand its scaled-up writer at
full provisioned cost. The `scale_down_on_failure` task (`trigger_rule=one_failed`, upstreams
`provision`/`create_schema`/`copy`/`build_indexes`/`resize`) runs `loader scale-down`, which flips
the writer to `db.serverless` to stop that cost. It deliberately skips the serve lockdown that
`resize` applies (no serve parameter group, backup-retention bump, deletion protection, or reboot),
so the failed run's cluster and loaded data survive for resume/forensics and stay easy to
`teardown`. It is skipped on a fully successful run (where `resize` already made the writer
serverless). Limit: it fires on an organic failure or a task marked failed, but not if the whole DAG
run is hard-deleted — that still needs a manual `loader teardown`/`scale-down`.

## Configuration

Set on the Astro deployment as **Environment Variables** (the CLI reads `LOADER_*` / `DATABRICKS_*`
from `os.environ`; BashOperators forward them with `append_env=True`):

- `LOADER_ENV`, `LOADER_S3_BUCKET`, `LOADER_S3_IMPORT_ROLE_ARN`, `LOADER_AWS_ACCOUNT_ID`
- `LOADER_VPC_ID`, `LOADER_DB_SUBNET_GROUP`, `LOADER_SECURITY_GROUP_ID`, `LOADER_KMS_KEY_ARN`
- `LOADER_DATABRICKS_WAREHOUSE_ID` (unload only)
- Optional sizing overrides: `LOADER_LOAD_INSTANCE_CLASS`, `LOADER_INDEX_INSTANCE_CLASS`

**Airflow Variables** (need a per-deployment override; an unset override resolves to empty at
runtime, it does not fall back to the workspace value):

- `databricks_conn_id` (`databricks_dev` / `databricks`) — the Databricks connection whose OAuth
  M2M creds are templated into `DATABRICKS_*` at task runtime (Astro does not expose deployment env
  vars to the DAG processor at parse time).
- `dbt_cloud_job_id` — the dbt Cloud job the voter gate triggers.

**Connections:** `gp_bastion_host` (ssh, unencrypted key), `dbt_cloud`. Postgres connection strings
come from SSM SecureStrings, never an env-var password.

## Operations

- **Trigger with a concrete PAST logical date.** A null logical date breaks `{{ ds_nodash }}`
  rendering; a future logical date silently never schedules. `ds_nodash` is the calendar date, so
  two triggers on the same day share a run date and its manifests.
- **Rerun just the index build.** unload re-exports and copy reloads on every fresh DAG trigger
  (only create_schema short-circuits cleanly), so to iterate on `build_indexes` alone, clear that
  single task instance on a run whose `copy` already succeeded rather than triggering a new run.
- **Deploying loader code.** The loader is pip-installed from this repo in `astro/requirements.txt`;
  that image layer is keyed on the file, so bump the `bust=` token whenever loader source changes or
  the image reinstalls stale code. A DAG-only push does not force an image deploy.
- **Monitoring without the UI.** The Astro/Airflow v2 REST API
  (`https://<deployment>.pm.astronomer.run/<ns>/api/v2/dags/load_people_api/dagRuns/<run>/...`) with
  a short-TTL `astro deployment token create` bearer works when the web session JWT expires. The API
  `duration` field is stale for running tasks; read log timestamps.
- **Teardown.** `resize` leaves a Serverless v2 cluster; teardown is not in this DAG. Clean up
  abandoned run clusters (`gp-people-db-<date>-<env>`) to avoid idle cost.

## Before merge

- [ ] Repin the loader install in `astro/requirements.txt` from `@feat/DATA-1913-loader-dag` to the
  squash-merge SHA (or a tag). It intentionally tracks the branch during review so pushes deploy;
  freeze it only at merge.
- [ ] Gate the merge on cutover readiness (DATA-1855). Merging swaps the canonical `load_people_api`
  dag_id to the new train-deployment loader, and the partitioned schema diverges from the current
  single-column Prisma model (the dbt write models' `ON CONFLICT` must move to
  `("LALVOTERID", "State")` at cutover, not before).
- [ ] Regenerate `_serving_seed.py` against current prod at cutover (verified byte-identical today, so
  a no-op unless prod drifts — e.g. the DATA-1855 work adding District structure to `public`). Run
  `extract-serving-structure` where the loader has prod access: the airflow SP on the worker
  (SSM-allowed + bastion), or a local engineer on VPN via a direct DSN (the `EngineerAccess` SSO
  identity is explicitly denied the SSM connection-string param). Then `ruff format` the file and
  `git diff` — an empty diff means no drift.

## References

- TDD (DATA-1735) sections 4.4, 4.5, 4.10, 7, 9.3 (bastion + BashOperator decisions)
- `people-api-loader/` CLI and `people-api-loader/CLAUDE.md`
- Related: DATA-1905 (S3 bucket + rds-s3-import role), DATA-1906 (voter gate), DATA-1855 (cutover)
