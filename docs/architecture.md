# Architecture

A pointer-heavy doc. Detailed conventions live in `CLAUDE.md`, the per-subproject READMEs, `dbt/project/CLAUDE.md`, and `ai-rules/`.

## Repo shape: multi-project, not a monorepo

`gp-data-platform` is **not** a single Python package. It's a collection of sibling subprojects that share a repo because they belong to the same data team:

```
gp-data-platform/
├── airflow/   ── orchestration (Astronomer-hosted Airflow 3.2)
├── dbt/       ── transformation (dbt Cloud + Databricks SQL)
├── apps/      ── small standalone services and CLIs
├── genie/     ── Genie config exports
├── scripts/   ── shell glue between the above
├── tests/     ── cross-subproject pytest suites
└── .github/   ── shared CI (pre-commit + pytest)
```

Each Python subproject (`airflow/`, `dbt/`, `apps/genie-tools/`) has its **own** `pyproject.toml` + `poetry.lock`. Local development is per-subproject: you `cd <subproject>` and `poetry install` there. CI is **not** per-subproject — `.github/workflows/python_tests.yml` does a flat `pip install -r requirements_test.txt` at the root and runs `pytest tests`.

## Stack

- **Orchestration:** Apache Airflow 3.2 on **Astronomer** (`airflow/astro/`, Astro Runtime `3.2-2` per `airflow/astro/Dockerfile`). DAGs live in `airflow/astro/dags/`. Branch deploys: `main` auto-deploys to `astro-prod`; `astro-dev` is **manually** pointed at one feature branch at a time via the Astro UI (see `airflow/astro/README.md` § Environments).
- **Transformation:** **dbt Cloud** running against **Databricks SQL** (`dbt/project/`). The dbt Cloud CLI is installed at the system level — **don't** invoke dbt via `poetry`.
- **Apps:**
  - `apps/genie-tools/` — Python CLI (`genie-tools` entry point, src/ layout, setuptools-built via PEP 621 `[project]` metadata — no `poetry.lock` here, install with `pip install -e .`). Depends only on `databricks-sdk`. Used to export-only Databricks Genie space configs.
  - `apps/genie-slack-bot/` — Slack bot wrapping Genie. `requirements.txt`-based; no `pyproject.toml`.
- **Lineage / config:** `genie/civics/` holds Genie space configs.
- **Style enforcement:** `.pre-commit-config.yaml` with **black**, **isort**, **flake8**, **mypy**, **sqlfmt**, plus a local `pytest` hook. CI runs `pre-commit run --all-files` on every PR.
- **Python versions:** `airflow/` and `dbt/` pin **3.12** (`requires-python = ">=3.12,<3.13"`); `apps/genie-tools/` is `>=3.12` with no upper bound. CI workflows (`.github/workflows/python_tests.yml`, `.github/workflows/pre-commit.yml`) run on **Python 3.11**. This split is pre-existing — flagged in `CLAUDE.md` § Environment.

## Subproject details

### `airflow/`

- `airflow/astro/` is an [Astronomer](https://www.astronomer.io/) project (`Dockerfile`, `dags/`, `include/`, `tests/`, `requirements.txt`, `airflow_settings.yaml.example`). The Astro CLI runs Airflow locally in Docker; deploys go to Astronomer-hosted `astro-dev` / `astro-prod`.
- `airflow/pyproject.toml` (`name: astro-goodparty`, Python 3.12, poetry-managed) is for **local development of DAG code** outside the Astro container — i.e. when you want a poetry-managed Python env to import the same packages the DAGs use (`pyspark`, `databricks-sql-connector`, `paramiko`, etc.).
- DAG tests live at the repo root in `tests/airflow/`, not inside `airflow/`.

### `dbt/`

- `dbt/project/` is the dbt project root: `models/`, `macros/`, `seeds/`, `snapshots/`, `tests/`, `analyses/`, `dbt_project.yml`, `package-lock.yml`, `packages.yml`. **Read `dbt/project/CLAUDE.md`** before you touch anything here — it has the model-build/inspect/test workflow, the lateral-column-reference and NULL-in-NOT-IN gotchas, and naming conventions.
- `dbt/pyproject.toml` (`name: dbt-goodparty`, poetry-managed) is for the **Python utilities and pre-commit hooks**, not for dbt itself. The pre-commit pytest hook runs from `dbt/`'s poetry env so it can find `pyspark` and `airflow`. That's why `dbt/project/CLAUDE.md` says to `cd dbt && poetry run git commit ...`.
- `dbt/poetry.toml` sets `virtualenvs.in-project = false` — the venv lives in poetry's cache, not in `dbt/.venv/`.
- dbt model tests are run with **`dbt test`**, not pytest. The pytest tests at `tests/dbt/` are about the surrounding Python utilities.

### `apps/genie-tools/`

- A small CLI (`genie-tools`) for exporting Databricks Genie space configurations. Uses `setuptools` (not poetry) for build, with PEP 621 `[project]` metadata in `pyproject.toml`. No `poetry.lock` is committed; the standard install is `pip install -e .`. Source under `src/genie_tools/`.
- Depends only on `databricks-sdk`.
- Tests live at `tests/apps/genie_tools/`.

### `apps/genie-slack-bot/`

- A Slack bot. `requirements.txt`-based — no `pyproject.toml`, no poetry. Just `pip install -r requirements.txt`.
- Tests live at `tests/apps/genie_slack_bot/`.

### `genie/civics/spaces/`

- Genie space configs (likely YAML/JSON). Edited and exported via `apps/genie-tools/`.

### `scripts/`

- Shell scripts:
  - `dbt_codegen_stg_uniform_files.sh` — codegen for dbt staging models.
  - `tgp_to_gp/` — extract / transform / load between TGP and GP databases.
  - `local_to_election_db/` — local-to-election-DB sync helpers.

## Data flow (high-level)

```
External sources (Airbyte, files, APIs)
        │
        ▼  ingestion (Airbyte configs — YAML, not in this repo's Python)
   Databricks (raw / bronze)
        │
        ▼  dbt models (dbt Cloud, dbt/project/models/staging → intermediate → marts)
   Databricks (silver / gold)
        │
        ▼  consumed by:
        ├── Airflow DAGs (orchestration jobs in airflow/astro/dags/)
        ├── Genie spaces (genie/civics/spaces/, exported via apps/genie-tools)
        └── BI / downstream services
```

The dbt staging-→ intermediate → mart layering is documented in `dbt/project/CLAUDE.md` (mart names use domain-friendly names, no `m_` prefix; staging follows `stg_<source>__<table>.sql`; intermediate follows `int__<domain>_<object>_<year>.sql`).

## Cross-service edges

| Direction | Service | Protocol | Notes |
|-----------|---------|----------|-------|
| outbound | Databricks SQL warehouse | `databricks-sql-connector` / `databricks-sdk` | Used by dbt, airflow utils, genie-tools |
| outbound | dbt Cloud | dbt Cloud CLI (system-installed) | Triggered manually by devs and via Airflow |
| outbound | Astronomer (Airflow) | Git deploy | `main` → `astro-prod` (auto); `astro-dev` is manually mapped to one feature branch via the Astro UI |
| outbound | Slack | `slack-bolt` (+ `slack-sdk`) | `apps/genie-slack-bot` |
| outbound | TGP / election databases | `psycopg2`, ssh tunnel | `scripts/tgp_to_gp/`, `scripts/local_to_election_db/` |

There is no inbound HTTP API in this repo. Other services don't call into `gp-data-platform`; this repo runs scheduled or triggered jobs that read upstream sources and write to Databricks.

## Testing

- Root pytest is what CI runs:
  ```bash
  pip install -r requirements_test.txt
  pytest tests
  ```
  This installs deps (Airflow, pyspark, databricks-sql-connector, pandas, pytest, ...) at the repo root and discovers tests under `tests/`.
- The pre-commit `pytest` hook also runs on every commit — but **from the repo root** unless you `cd` somewhere else. From `dbt/`, you must run `poetry run git commit ...` so the hook finds Spark and Airflow inside the `dbt-goodparty` venv. See `dbt/project/CLAUDE.md` for the rationale.
- dbt model tests are run separately via `dbt test --select <model>` from `dbt/project/`.

## CI

Two workflows:

- `.github/workflows/python_tests.yml` — Python 3.11, `pip install -r requirements_test.txt`, `pytest tests`. Runs on every push.
- `.github/workflows/pre-commit.yml` — Python 3.11, `pip install -r requirements_test.txt && pip install pre-commit && pre-commit run --all-files`. Runs on PRs and pushes to `main` / `develop`.

## Skills

- `.claude/skills/l2-uniform-drift-remediator/` is an existing repo-local skill: scripts that detect and remediate L2 Uniform schema drift in Databricks, plus reference runbooks and tests. Don't touch it from a Python-onboarding PR.
- `.claude/skills/python-developer.md` (added in this onboarding) is a small skill that captures the multi-venv, multi-tool reality of this repo so agents don't reach for the wrong package manager or run the wrong test command.

## ADRs

`docs/adr/` is not yet seeded. Add one when a non-obvious decision lands — likely candidates are: why poetry per-subproject instead of a single monorepo lockfile, why Astronomer for Airflow, why dbt Cloud over dbt Core, why Databricks (vs Snowflake / BigQuery). Use `ai-rules/adr-template.md`.
