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

## Step bodies are stubs by design

Every file under `src/loader/people_api/steps/` raises `NotImplementedError` with a pointer to its ClickUp ticket (e.g. `DATA-1851` for `copy_s3`). Scaffolding shipped under DATA-1852; bodies land under their own per-step tickets in the [DATA-1640 epic](https://app.clickup.com/t/86ag66jjr). Don't fill in a stub unless that ticket is the task at hand.

Stub return-type rule: even when a stub only raises, declare the eventual real return type. The CLI is already wired up against those types, and `ty` will flag the mismatch. See `steps/validate.py` for the pattern.

## Never

- Don't read or set `VOTER_DB_MASTER_PASSWORD`. `LoaderConfig.from_env()` hard-fails if it's set. Connections come from SSM SecureString connection strings (`people-db-connection-string-{env}` for the Present cluster, `-{date}`-suffixed for each provisioned cluster), never an env-var password.
- Don't put people-API-specific knowledge in `src/loader/core/`. The harness must stay consumer-agnostic — a second consumer (e.g. donor data) should sit alongside `people_api/`, not require core changes.
- Don't omit `Environment=dev` tags on any AWS resource the loader creates. The loader's IAM permissions-boundary requires it; missing tags = denied API call.
- Don't mutate the existing serving cluster. The model is train-deployment: every refresh provisions a fresh Aurora cluster and swaps in.
- Don't write a manifest mid-step. Manifest existence at `s3://{bucket}/voter_export_{date}/_manifest/{step}.json` means that step is complete.

## Deploying via the Airflow DAG (`load_people_api`)

The DAG at `gp-data-platform/airflow/astro/dags/load_people_api.py` sequences the CLI on the Astro worker — one `BashOperator` per step; parallelism stays in the loader. Real infra values are NOT committed here (this repo is public); they live in `gp-terraform-dataplatform` and on the Astro deployment.

**Identity: reuse the airflow service principal.** The loader runs as the existing `airflow` SP (prod) / `airflow_dev` SP (dev) — there is no dedicated loader SP. The airflow SP already has catalog-wide read; it additionally needs `WRITE_FILES` on the loader external location and `CAN_USE` on the loader's SQL warehouse (both granted in `gp-terraform-dataplatform`).

**Databricks auth — one OAuth M2M credential.** `workspace_client()` uses ambient SDK auth, so set the airflow SP's creds as deployment env vars: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`. Both `unload` (databricks-sdk) and the dbt gate read the same credential.

**Compute is a SQL warehouse, not a cluster.** `unload` uses the Statement Execution API, which is warehouse-only — it cannot target an all-purpose cluster. Both the unload and the gate run on the warehouse named by `LOADER_DATABRICKS_WAREHOUSE_ID`.

**Env vars go in Astro → Environment Variables (NOT Airflow Variables).** The loader reads `LOADER_*` / `DATABRICKS_*` from `os.environ`; the DAG's BashOperators forward them via `append_env=True`. Required: `LOADER_ENV`, `LOADER_S3_BUCKET`, `LOADER_S3_IMPORT_ROLE_ARN`, `LOADER_AWS_ACCOUNT_ID`, `LOADER_DATABRICKS_WAREHOUSE_ID`, `LOADER_VPC_ID`, `LOADER_DB_SUBNET_GROUP`, `LOADER_SECURITY_GROUP_ID`, `LOADER_KMS_KEY_ARN`, `LOADER_DBT_SCHEMA` (`dbt` prod / `dbt_staging` dev), plus the three `DATABRICKS_*`. AWS creds (to assume the cross-account RDS-admin role) come from the worker's standard AWS credential chain.

**Postgres connectivity — bastion.** The DAG reads the `gp_bastion_host` SSH connection and templates host/port/user/key (+ passphrase) into `LOADER_BASTION_*`. `db.py` opens an SSH tunnel when those are set, dialing the forward via `hostaddr` while keeping the RDS host for TLS SNI; with them unset it connects directly (local/VPN).

**dbt gate runs in an isolated venv.** `dbt-databricks` pins `databricks-sdk <0.105`, which conflicts with the image's `databricks-sdk >=0.117`. So the image carries only `astronomer-cosmos`, and the gate is a `DbtTestVirtualenvOperator` with `py_requirements=["dbt-databricks…"]` — Cosmos builds an isolated venv at task runtime. The dbt project is cloned into the image in the Astro `Dockerfile` (it lives at the repo root, outside the Astro build context). The profile is `include/loader_dbt_profiles.yml`, authenticating via OAuth M2M from the same `DATABRICKS_*` env vars.

**S3 gateway endpoint — reuse, never create.** Aurora's `copy` (server-side `aws_s3.table_import_from_s3`) relies on the VPC's existing shared S3 gateway endpoint. `provision` finds and reuses it (never creates or deletes — it's durable platform infra). A route table can bind only one S3 gateway endpoint, so a second can't be added.
