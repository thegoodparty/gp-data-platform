# CLAUDE.md

Short, non-obvious context for `people-api-loader`. The repo overview is in `README.md`; the full TDD is in `docs/PLAN_LOADER.md`.

## ai-rules submodule

`ai-rules/` is a git submodule at the repository root (`thegoodparty/ai-rules`). After a fresh clone, run `git submodule update --init --recursive` from the repo root. Don't edit files under `ai-rules/` directly — changes belong in the submodule's upstream repo. When reviewing changed code in this directory, consult the rule files at `../../ai-rules/` in addition to this file.

## Astral-only toolchain

This repo standardizes on Astral tools and is intentionally not aligned with `gp-data-platform`'s older stack:

| Tool | Use | Notes |
|---|---|---|
| `uv` | env + deps | Installs Python 3.14 itself via `.python-version`. No pyenv. |
| `ruff` | lint + format | Line length 110. Don't introduce black/isort/flake8. |
| `ty` | type checker | Pre-1.0 — version is pinned exactly in `pyproject.toml`. Don't loosen the pin without testing. Don't introduce pyright/mypy. |

CI runs `uv run ruff check`, `uv run ruff format --check`, `uv run ty check`, and `uv run pytest` on every PR.

## Pipeline steps

Each step is one module under `src/loader/people_api/steps/` (`inspect_prod`, `unload`, `provision`, `create_schema`, `copy_s3`, `build_indexes`, `resize`, `validate`, `teardown`), invoked as `loader <step> --date <ds_nodash>`. State flows between steps through S3 manifests, not in-process handoff, so each step is independently runnable and re-entrant for a given run date (a step short-circuits if its manifest already exists). The DAG at `gp-data-platform/airflow/astro/dags/load_people_api.py` sequences them, one BashOperator per step.

Each step's CLI entry point is typed against its real return type, so `ty` enforces the contract between the step and the CLI. See `steps/validate.py` for the pattern.

## Never

- Don't read or set `VOTER_DB_MASTER_PASSWORD`. `LoaderConfig.from_env()` hard-fails if it's set. Connections come from SSM SecureString connection strings (`people-db-connection-string-{env}` for the Present cluster, `-{date}`-suffixed for each provisioned cluster), never an env-var password.
- Don't put people-API-specific knowledge in `src/loader/core/`. The harness must stay consumer-agnostic — a second consumer (e.g. donor data) should sit alongside `people_api/`, not require core changes.
- Don't omit `Environment=dev` tags on any AWS resource the loader creates. The loader's IAM permissions-boundary requires it; missing tags = denied API call.
- Don't mutate the existing serving cluster. The model is train-deployment: every refresh provisions a fresh Aurora cluster and swaps in.
- Don't write a manifest mid-step. Manifest existence at `s3://{bucket}/voter_export_{date}/_manifest/{step}.json` means that step is complete.

## Deploying via the Airflow DAG (`load_people_api`)

The DAG at `gp-data-platform/airflow/astro/dags/load_people_api.py` sequences the CLI on the Astro worker — one `BashOperator` per step; parallelism stays in the loader. Real infra values are NOT committed here (this repo is public); they live in `gp-terraform-dataplatform` and on the Astro deployment.

**Identity: reuse the airflow service principal.** The loader runs as the existing `airflow` SP (prod) / `airflow_dev` SP (dev) — there is no dedicated loader SP. The airflow SP already has catalog-wide read; it additionally needs `WRITE_FILES` on the loader external location and `CAN_USE` on the loader's SQL warehouse (both granted in `gp-terraform-dataplatform`).

**Databricks auth: one OAuth M2M credential, reused from a connection.** `unload` gets `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` templated at task runtime from the shared Databricks Airflow connection the L2 dbt jobs use. WHICH connection is chosen at runtime from the `databricks_conn_id` Airflow Variable (`databricks_dev` dev / `databricks` prod), NOT a parse-time env var — Astro does not expose deployment env vars to the DAG processor at parse. The voter-data gate no longer uses this (it runs in dbt Cloud; see below).

**Compute.** `unload` uses the Statement Execution API, which is warehouse-only (it cannot target an all-purpose cluster), so it runs on the SQL warehouse `LOADER_DATABRICKS_WAREHOUSE_ID`. The voter-data gate runs in dbt Cloud (see below), so it needs no local Databricks compute.

**Env vars go in Astro → Environment Variables (NOT Airflow Variables).** The loader CLI reads `LOADER_*` / `DATABRICKS_*` from `os.environ`; the DAG's BashOperators forward them via `append_env=True`. Required: `LOADER_ENV`, `LOADER_S3_BUCKET`, `LOADER_S3_IMPORT_ROLE_ARN`, `LOADER_AWS_ACCOUNT_ID`, `LOADER_DATABRICKS_WAREHOUSE_ID` (unload only), `LOADER_VPC_ID`, `LOADER_DB_SUBNET_GROUP`, `LOADER_SECURITY_GROUP_ID`, `LOADER_KMS_KEY_ARN`. The `DATABRICKS_*` values are templated from the connection by the DAG, not set directly. The connection selection (`databricks_conn_id`) and the dbt Cloud gate (`dbt_cloud_job_id`) come from **Airflow Variables**, which need a per-deployment override on each deployment — a "None"/unset override resolves to empty at runtime, it does not fall back to the workspace value. AWS creds (to assume the cross-account RDS-admin role) come from the worker's standard AWS credential chain.

**Postgres connectivity — bastion.** The DAG reads the `gp_bastion_host` SSH connection and templates host/port/user/key (+ passphrase) into `LOADER_BASTION_*`. `db.py` opens an SSH tunnel when those are set, dialing the forward via `hostaddr` while keeping the RDS host for TLS SNI; with them unset it connects directly (local/VPN).

**Voter-data gate runs in dbt Cloud, not locally.** dbt cannot run on the image's Python 3.14 (mashumaro fails at import), the loader requires 3.14 so the image can't downgrade, and the full monorepo dbt project needs ~24 parse-time `DBT_*` env vars (dbt parses the whole project before `--select`). So the gate is a `DbtCloudRunJobOperator` (conn `dbt_cloud`, `job_id` from the `dbt_cloud_job_id` Airflow Variable, `steps_override=["dbt test --select m_people_api__voter"]`) that runs the voter tests in dbt Cloud's already-configured environment. No local dbt / Cosmos / dbt-project clone in the image.

**S3 gateway endpoint — reuse, never create.** Aurora's `copy` (server-side `aws_s3.table_import_from_s3`) relies on the VPC's existing shared S3 gateway endpoint. `provision` finds and reuses it (never creates or deletes — it's durable platform infra). A route table can bind only one S3 gateway endpoint, so a second can't be added.
