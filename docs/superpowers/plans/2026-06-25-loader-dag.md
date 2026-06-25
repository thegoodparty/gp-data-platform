# People-API Loader DAG Implementation Plan (DATA-1913 Plan B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the Astro/Airflow DAG `load_people_api_v2.py` that runs the People-API voter refresh by invoking the loader CLI step-by-step on the Astro worker, gated by a Cosmos dbt test.

**Architecture:** A thin sequencer. The loader is installed on the Astro worker image (pip-from-git), and each pipeline step is one `BashOperator` running `loader <step> --date {{ ds_nodash }}`. Parallelism stays inside the loader (TDD 4.5). State between steps flows through the loader's own S3 manifests (no Airflow xcom). Postgres is reached through `gp_bastion_host` over SSH — the DAG bridges that Airflow connection into the loader's `LOADER_BASTION_*` env vars (the SSH-tunnel support landed in Plan A). The `dbt test` gate is a Cosmos `DbtTestLocalOperator` scoped to the voter models.

**Tech Stack:** Astro Runtime 3.x (python-3.14, Celery/local executor — tasks run as subprocesses, not pods), Airflow 3 SDK (`airflow.sdk`), `BashOperator`, `astronomer-cosmos` + `dbt-core` + `dbt-databricks`, pendulum, pytest.

**Working directory for commands:** `airflow/astro/` unless noted. The Astro project's deps are `airflow/astro/requirements.txt`; DAGs live in `airflow/astro/dags/`.

**Depends on:** DATA-1913 Plan A (loader bastion connectivity) — already on this branch.

---

### Task 1: Add loader + Cosmos + dbt deps to the Astro image

**Files:**
- Modify: `airflow/astro/requirements.txt`

- [ ] **Step 1: Add the dependencies**

Append to `airflow/astro/requirements.txt`:

```
# People-API loader CLI (DATA-1913) — installed from this monorepo's subdirectory.
# Pin to a tag/SHA before merge; during PR dev this tracks the feature branch.
people-api-loader @ git+https://github.com/thegoodparty/gp-data-platform.git@feat/DATA-1913-loader-dag#subdirectory=people-api-loader
# dbt test gate via Cosmos (DATA-1906 voter gate)
astronomer-cosmos>=1.7,<2.0
dbt-core>=1.8,<2.0
dbt-databricks>=1.8,<2.0
```

- [ ] **Step 2: Verify the image builds locally**

Run: `cd airflow/astro && astro dev parse 2>&1 | tail -20`
Expected: build resolves the new deps with no dependency-resolution error. (If `astro` is unavailable, at minimum `pip install -r requirements.txt` in a scratch venv to confirm resolution.)

- [ ] **Step 3: Commit**

```bash
git add airflow/astro/requirements.txt
git commit -m "feat(airflow): add people-api-loader + cosmos/dbt deps to Astro image"
```

---

### Task 2: Loader-env helper + DAG skeleton (parse-tested)

**Files:**
- Create: `airflow/astro/dags/load_people_api_v2.py`
- Create: `airflow/astro/tests/dags/test_load_people_api_v2.py`

The helper bridges the `gp_bastion_host` Airflow connection into the loader's `LOADER_BASTION_*` env vars (Plan A reads these), and carries the loader's infra config from Airflow Variables. Steps that don't touch Postgres still receive the env harmlessly (the loader only opens the tunnel when it connects).

- [ ] **Step 1: Write the failing DAG-parse test**

Create `airflow/astro/tests/dags/test_load_people_api_v2.py`:

```python
"""The loader DAG parses and exposes the expected steps in order."""

from __future__ import annotations

from airflow.models import DagBag


def _dag():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("load_people_api_v2")
    assert dag is not None
    return dag


def test_dag_imports_clean():
    _dag()


def test_step_sequence():
    dag = _dag()
    ids = set(dag.task_ids)
    expected = {
        "inspect_prod", "dbt_test_voter_gate", "unload", "provision",
        "create_schema", "copy", "build_indexes", "resize", "validate",
    }
    assert expected <= ids
    # gate blocks unload; provision runs parallel to unload after the gate
    assert "dbt_test_voter_gate" in {t.task_id for t in dag.get_task("unload").upstream_list}
    assert "validate" == dag.get_task("resize").downstream_list[0].task_id
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd airflow/astro && uv run pytest tests/dags/test_load_people_api_v2.py -v`
Expected: FAIL — `load_people_api_v2` not found (DAG doesn't exist yet).

- [ ] **Step 3: Create the DAG with the env helper and a single placeholder task**

Create `airflow/astro/dags/load_people_api_v2.py`:

```python
"""People-API voter refresh (DATA-1913).

Thin sequencer over the loader CLI (installed on the worker image). Each step is a
BashOperator running `loader <step> --date {{ ds_nodash }}`; parallelism lives in the
loader, and inter-step state flows through the loader's S3 manifests (no xcom). Postgres
is reached via the gp_bastion_host SSH tunnel — we pass the bastion connection into the
loader's LOADER_BASTION_* env vars (the loader opens the tunnel only when it connects).
"""

from __future__ import annotations

from airflow.sdk import Variable, dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime as pendulum_datetime
from pendulum import duration


def _loader_env() -> dict[str, str]:
    """Build the LOADER_* env for the loader CLI from Airflow connections + Variables.

    Bastion: the gp_bastion_host connection (host, login, extra.private_key) maps to the
    LOADER_BASTION_* vars Plan A reads. An empty bastion Variable disables the tunnel
    (direct connect) for local/VPN runs.
    """
    from airflow.sdk import BaseHook

    bastion_conn_id = Variable.get("loader_bastion_conn_id", default="gp_bastion_host")
    env: dict[str, str] = {
        "LOADER_ENV": Variable.get("loader_env", default="dev"),
        "LOADER_S3_BUCKET": Variable.get("loader_s3_bucket", default=""),
        "LOADER_S3_IMPORT_ROLE_ARN": Variable.get("loader_s3_import_role_arn", default=""),
        "LOADER_AWS_ACCOUNT_ID": Variable.get("loader_aws_account_id", default=""),
        "LOADER_DATABRICKS_WAREHOUSE_ID": Variable.get("loader_databricks_warehouse_id", default=""),
        "LOADER_VPC_ID": Variable.get("loader_vpc_id", default=""),
        "LOADER_DB_SUBNET_GROUP": Variable.get("loader_db_subnet_group", default=""),
        "LOADER_SECURITY_GROUP_ID": Variable.get("loader_security_group_id", default=""),
        "LOADER_KMS_KEY_ARN": Variable.get("loader_kms_key_arn", default=""),
    }
    if bastion_conn_id:
        b = BaseHook.get_connection(bastion_conn_id)
        env["LOADER_BASTION_HOST"] = b.host or ""
        env["LOADER_BASTION_PORT"] = str(b.port or 22)
        env["LOADER_BASTION_USER"] = b.login or ""
        env["LOADER_BASTION_PRIVATE_KEY"] = b.extra_dejson.get("private_key", "")
    return env


def _step(task_id: str, subcommand: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"loader {subcommand} --date {{{{ ds_nodash }}}}",
        env=_loader_env(),
        append_env=True,
    )


@dag(
    dag_id="load_people_api_v2",
    schedule="@monthly",
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "DATA-1640"],
)
def load_people_api_v2():
    inspect_prod = _step("inspect_prod", "inspect-prod")


load_people_api_v2()
```

- [ ] **Step 4: Run the test to verify import passes (sequence test still fails)**

Run: `cd airflow/astro && uv run pytest tests/dags/test_load_people_api_v2.py::test_dag_imports_clean -v`
Expected: PASS. (`test_step_sequence` still fails — only one task so far.)

- [ ] **Step 5: Commit**

```bash
git add airflow/astro/dags/load_people_api_v2.py airflow/astro/tests/dags/test_load_people_api_v2.py
git commit -m "feat(airflow): loader DAG skeleton + bastion-env helper"
```

---

### Task 3: Wire the full BashOperator step sequence

**Files:**
- Modify: `airflow/astro/dags/load_people_api_v2.py`

- [ ] **Step 1: Add the remaining steps and dependencies**

Replace the body of `load_people_api_v2()` (keep the gate slot empty for Task 4 — it is added there):

```python
def load_people_api_v2():
    inspect_prod = _step("inspect_prod", "inspect-prod")
    unload = _step("unload", "unload")
    provision = _step("provision", "provision")
    create_schema = _step("create_schema", "create-schema")
    copy = _step("copy", "copy")
    build_indexes = _step("build_indexes", "build-indexes")
    resize = _step("resize", "resize")
    validate = _step("validate", "validate")

    # inspect-prod first; unload + provision run in parallel; both feed create-schema;
    # then the serial load chain. (The dbt gate between inspect_prod and unload is added
    # in Task 4.)
    inspect_prod >> [unload, provision]
    [unload, provision] >> create_schema
    create_schema >> copy >> build_indexes >> resize >> validate
```

- [ ] **Step 2: Run the sequence test (the gate assertion still fails — added in Task 4)**

Run: `cd airflow/astro && uv run pytest tests/dags/test_load_people_api_v2.py::test_dag_imports_clean -v`
Expected: PASS (imports clean with all 8 loader steps).

- [ ] **Step 3: Commit**

```bash
git add airflow/astro/dags/load_people_api_v2.py
git commit -m "feat(airflow): sequence the loader steps in the DAG"
```

---

### Task 4: dbt test gate via Cosmos

**Files:**
- Modify: `airflow/astro/dags/load_people_api_v2.py`

The DATA-1906 gate runs dbt tests on the voter models before `unload`. Use Cosmos's
`DbtTestLocalOperator` (test-only, no model rebuild) scoped to the voter mart.

- [ ] **Step 1: Confirm the databricks dbt profile shape**

The `databricks` Airflow connection is OAuth M2M. Determine the exact Cosmos profile
mapping before writing config:
- Inspect the connection: `cd airflow/astro && uv run python -c "from airflow.sdk import BaseHook; c=BaseHook.get_connection('databricks'); print(c.host, c.extra_dejson.keys())"` (or read it in the Astro UI). Note host, and whether auth is a token (`token`) or OAuth M2M (`client_id`/`client_secret`).
- dbt project: profile `default`, catalog `goodparty_data_catalog`, mart schema from the `databricks_dbt_schema` Variable (`dbt` in prod, `dbt_staging` in dev). Warehouse `http_path` is `/sql/1.0/warehouses/<warehouse_id>` from the `loader_databricks_warehouse_id` Variable.
- Choose the mapping: for a token connection use `cosmos.profiles.DatabricksTokenProfileMapping`; for OAuth M2M, supply `auth_type: oauth` + `client_id`/`client_secret` via a `ProfileConfig` with explicit `profile_args`. Confirm the installed `dbt-databricks` supports the chosen auth.

- [ ] **Step 2: Add the Cosmos gate (token-auth form shown; adjust per Step 1)**

Add near the top of `load_people_api_v2.py`:

```python
from cosmos import ProfileConfig
from cosmos.operators import DbtTestLocalOperator
from cosmos.profiles import DatabricksTokenProfileMapping

_DBT_PROJECT_DIR = "/usr/local/airflow/dbt/project"  # dbt project shipped in the image


def _voter_gate_profile() -> ProfileConfig:
    return ProfileConfig(
        profile_name="default",
        target_name="prod",
        profile_mapping=DatabricksTokenProfileMapping(
            conn_id="databricks",
            profile_args={
                "catalog": Variable.get("databricks_catalog", default="goodparty_data_catalog"),
                "schema": Variable.get("databricks_dbt_schema", default="dbt"),
                "http_path": "/sql/1.0/warehouses/"
                + Variable.get("loader_databricks_warehouse_id", default=""),
            },
        ),
    )
```

In `load_people_api_v2()`, add the gate task and rewire `inspect_prod >> gate >> [unload, provision]`:

```python
    dbt_test_voter_gate = DbtTestLocalOperator(
        task_id="dbt_test_voter_gate",
        project_dir=_DBT_PROJECT_DIR,
        profile_config=_voter_gate_profile(),
        select=["m_people_api__voter"],  # the voter mart's schema tests + the singular gate test
    )

    inspect_prod >> dbt_test_voter_gate >> [unload, provision]
    [unload, provision] >> create_schema
    create_schema >> copy >> build_indexes >> resize >> validate
```

Remove the old `inspect_prod >> [unload, provision]` line from Task 3.

- [ ] **Step 3: Ensure the dbt project ships in the image**

Confirm `airflow/astro/Dockerfile` copies the dbt project to `_DBT_PROJECT_DIR` (or adjust the path). If not present, add a `COPY` of `dbt/project` into the image build, or set `_DBT_PROJECT_DIR` to wherever the project lands. Document the chosen path in a comment.

- [ ] **Step 4: Run the full DAG test**

Run: `cd airflow/astro && uv run pytest tests/dags/test_load_people_api_v2.py -v`
Expected: PASS — imports clean, all 9 tasks present, gate upstream of `unload`, `resize >> validate`.

- [ ] **Step 5: Commit**

```bash
git add airflow/astro/dags/load_people_api_v2.py
git commit -m "feat(airflow): add Cosmos dbt-test voter gate before unload"
```

---

### Task 5: Render check + sweep

**Files:** none (verification)

- [ ] **Step 1: Parse + render the DAG**

Run: `cd airflow/astro && uv run pytest tests/dags/test_load_people_api_v2.py -v` and, if available, `astro dev parse`.
Expected: all tests pass; no DAG import errors.

- [ ] **Step 2: Lint**

Run: `cd airflow/astro && uv run ruff check dags/load_people_api_v2.py tests/dags/test_load_people_api_v2.py`
Expected: clean (fix and re-run if not).

- [ ] **Step 3: Commit any fixes**

```bash
git add -A airflow/astro
git commit -m "chore(airflow): lint fixes for loader DAG"
```

---

## Notes for the implementer

- **No xcom.** The loader persists inter-step state (including the inspect→validate baseline) as S3 manifests under `voter_export_<date>/`. Do not add xcom plumbing.
- **Env delivery.** `_loader_env()` reads Airflow Variables + the `gp_bastion_host` connection. Those Variables (`loader_s3_bucket`, `loader_s3_import_role_arn`, `loader_aws_account_id`, `loader_databricks_warehouse_id`, VPC/subnet/SG/KMS, `databricks_catalog`, `databricks_dbt_schema`) are set in the Astro Environment Manager — see the DATA-1905 comment on DATA-1913 for the S3/role values. AWS credentials come from the worker's assumed `gp-people-rds-admin-prod` role (ambient boto3 chain), not from this env.
- **Loader git pin.** `requirements.txt` pins `people-api-loader` to the feature branch during PR dev; change to a tag or the merge SHA before merge. For local image iteration, `pip install -e ../../people-api-loader` in a dev override instead.
- **Cosmos auth (Task 4 Step 1).** OAuth M2M vs token determines the profile mapping; confirm against the live `databricks` connection before finalizing.
- **dbt project path.** `_DBT_PROJECT_DIR` must match where the image places `dbt/project`. Verify in Task 4 Step 3.
- This plan does not run the pipeline end-to-end; that needs the DATA-1905 bucket applied, a dev bucket/Databricks path, and the S3 VPC endpoint (Plan C). DAG parse/render is the acceptance here.

## Post-execution outcome (what shipped vs the plan)

Implemented and pushed on `feat/DATA-1913-loader-dag`. Divergences from the draft above, all
verified locally (real-Airflow DagBag parse: clean, 9 tasks, gate upstream of `unload`):

- **Cosmos imports:** `from cosmos.operators.local import DbtTestLocalOperator` (not
  `cosmos.operators`); profile mapping is `DatabricksOauthProfileMapping` (the `databricks`
  connection is OAuth M2M — host/client_id/client_secret come from the connection).
- **Env delivery simplified:** `_loader_env()` carries only the bastion fields (from the
  `gp_bastion_host` connection, runtime-templated so parse never hits the metastore). All
  other `LOADER_*` infra config is set as **deployment env vars** and reaches the `loader`
  subprocess via `append_env=True` and the Cosmos gate via `os.environ` — one source.
- **Gate profile args:** `schema` from `LOADER_DBT_SCHEMA` (default `dbt`) and `http_path`
  from `LOADER_DATABRICKS_WAREHOUSE_ID`, read at parse time (env, not Variables).
- **Local dep:** only `astronomer-cosmos` is locked into `airflow/` (the DAG parses with it);
  `dbt-databricks` has no py3.14 stable release for uv to lock, so it lives only in the Astro
  image (`astro/requirements.txt`) and is used at task runtime.
- **No pytest DAG test in `astro/tests/`:** sibling tests stub `airflow` in `sys.modules`,
  which breaks a real-`DagBag` test. DAG integrity is covered by `.astro/test_dag_integrity_default.py`
  + `astro dev parse`.

## Deployment items (resolve on Astro dev)

1. **Ship the dbt project into the image** at `_DBT_PROJECT_DIR` (`/usr/local/airflow/dbt/project`).
   The Astro `Dockerfile` does not copy it and `dbt/project` is outside the `airflow/astro`
   build context. Options: a Dockerfile `RUN git clone … #subdirectory=dbt/project` (parallel to
   the loader's pip-from-git), a pre-`astro deploy` copy step, or set `LOADER_DBT_PROJECT_DIR`
   to wherever it lands. Without this the gate fails at runtime.
2. **Deployment env vars** (Astro Environment Manager): `LOADER_S3_BUCKET`,
   `LOADER_S3_IMPORT_ROLE_ARN`, `LOADER_AWS_ACCOUNT_ID`, `LOADER_DATABRICKS_WAREHOUSE_ID`,
   `LOADER_VPC_ID`, `LOADER_DB_SUBNET_GROUP`, `LOADER_SECURITY_GROUP_ID`, `LOADER_KMS_KEY_ARN`,
   `LOADER_DBT_SCHEMA` (`dbt` prod / `dbt_staging` dev). See the DATA-1905 comment on DATA-1913.
3. **Connections:** `gp_bastion_host` (host/login + `extra.private_key`; the key must be
   **unencrypted** — the loader's `_load_key` has no passphrase support) and `databricks`
   (OAuth M2M: client_id/client_secret/host). Confirm `DatabricksOauthProfileMapping` reads them.
4. **AWS credentials:** the worker assumes `gp-people-rds-admin-prod` cross-account for boto3.
5. **Loader git pin** in `astro/requirements.txt` tracks the feature branch for dev; change to a
   tag/SHA before merge.
