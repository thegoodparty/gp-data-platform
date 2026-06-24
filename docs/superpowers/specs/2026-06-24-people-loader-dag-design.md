# People-API Loader DAG (DATA-1913) — Design

**Status:** approved design, pre-implementation
**Ticket:** DATA-1913 (epic DATA-1640)
**Authoritative TDD:** DATA-1735 (ClickUp doc `2ky4jq2q-55033`); repo copy at `people-api-loader/docs/PLAN_LOADER.md`

## Goal

Wire the completed loader CLI into an Astro/Airflow DAG that runs the full People-API
voter refresh on a monthly schedule, following the TDD's decided model: a thin
`BashOperator` sequencer on the Astro worker, parallelism inside the loader, Postgres
reached through the existing bastion host, and the `copy` performed server-side by Aurora.

## Decisions locked during brainstorming

1. **Execution: `BashOperator` per subcommand on the Astro worker** (TDD 4.10), not
   containerized. Containerization (ECS / `KubernetesPodOperator`) was considered and
   dropped to follow the TDD (TDD 4.4, 7, 9.3 punt ECS/direct-VPC in favor of the bastion).
2. **Loader installed on the Astro worker image** so the `loader` CLI is on PATH.
3. **DB connectivity: bastion SSH**, with the tunnel opened *inside* the loader (not in the
   BashOperator). `copy` stays server-side (Aurora `aws_s3.table_import_from_s3`).
4. **dbt test gate via Cosmos** (`astronomer-cosmos`), consistent with the dbt-in-Airflow
   direction (DATA-1892).
5. **Loader install mechanism: pip-from-git-subdirectory**, pinned ref (option 1).

## Architecture

The DAG `load_people_api_v2.py` sequences the loader CLI. Each step is one task invoking
`loader <subcommand> --date {{ ds_nodash }}` on the Astro worker. The orchestrator is
lightweight: Databricks performs the `unload`, Aurora performs the server-side `copy`,
index builds, and resize, so the worker only issues SQL/boto3 and waits.

- **AWS:** the worker assumes `gp-people-rds-admin-prod` cross-account (existing trust,
  DATA-1856). The loader uses the ambient boto3 credential chain, so the assumed role must
  be active before tasks run.
- **Databricks:** the existing `databricks` Airflow connection (`databricks_dev` in non-prod).
- **Postgres:** through `gp_bastion_host` over SSH (see "Loader bastion connectivity").

The only non-`BashOperator` step is the `dbt test` gate, which is a Cosmos task group.

## DAG structure

- **File:** `airflow/astro/dags/load_people_api_v2.py`
- **Schedule:** `@monthly`; manual trigger always allowed; fully idempotent (the loader's
  manifest skip-guards make re-runs safe).
- **Sequence (TDD 4.10):**
  `inspect-prod` → `dbt test` (Cosmos) → `unload` → `provision` → `create-schema` →
  `copy` → `build-indexes` → `resize` → `validate`.
  `provision` has no dependency on `unload`, so both run in parallel after the gate.
- **xcom baseline:** `inspect-prod` emits a JSON baseline (prod per-state row counts +
  snapshot dates) to stdout; the task pushes it to xcom (`do_xcom_push`); `validate`
  receives it back (via `--baseline` arg or an env var) for the comparison.
- **Failure semantics:** any task failure halts the DAG. A failing `dbt test` blocks
  `unload` (no bad data is exported). A failing `validate` means no cutover. Cutover is
  manual for the first prod runs (runbook ticket DATA-1914).
- **Parallelism:** stays inside the loader (TDD 4.5); each Airflow task is a single
  invocation, not a dynamic task-mapping fan-out.

## dbt test gate via Cosmos

The `dbt test` step is a Cosmos `DbtTaskGroup` scoped with `RenderConfig(select=[...])` to
the voter models only, so it runs the DATA-1906 gate's tests (referential integrity,
unique keys, business-logic expectations) and not the whole project.

Requirements this introduces on the Astro image:
- `astronomer-cosmos`, `dbt-core`, and the `dbt-databricks` adapter.
- The `dbt/project` available on the worker (already in the repo/image build).
- A dbt profile derived from the existing `databricks` Airflow connection.

This is the first dbt-core execution path in the repo (today dbt is the dbt Cloud CLI);
that is the intended DATA-1892 direction.

## Loader bastion connectivity (loader code change)

The loader's `people_api/db.py` connects directly via psycopg today (`sslmode=require`
from an SSM connection string). Add SSH-tunnel support inside the loader:

- Open an SSH tunnel to `gp_bastion_host` from bastion config supplied via env
  (host, user, key path/material), then connect psycopg through the local forward —
  mirroring the legacy `airflow/astro/include/custom_functions/postgres_utils.py`
  `get_postgres_via_ssh` approach (`sshtunnel`/`paramiko`).
- Gate it behind bastion config: when bastion env is absent (local/VPN runs), connect
  directly as today. When present (Astro worker), tunnel.
- Applies to every step that opens a psycopg connection: `inspect-prod`, `create-schema`,
  `copy`, `build-indexes`, `validate`. The tunnel is per-process and must survive the
  multi-hour `copy` (keepalives), which is why it lives in the loader, not the BashOperator.
- New loader deps: `sshtunnel` (and/or `paramiko`).

## Getting the loader onto the Astro image

Install the loader as a package via pip-from-git-subdirectory in
`airflow/astro/requirements.txt`, pinned to a ref/tag:

```
people-api-loader @ git+https://github.com/thegoodparty/gp-data-platform.git#subdirectory=people-api-loader
```

- gp-data-platform is public, so no build credentials are needed.
- Installing the package (not vendoring source into `include/`) is required so the `loader`
  console script is on PATH and the loader's pinned deps are resolved by pip. `include/` is
  for loose helper modules whose deps are already in `requirements.txt`; the loader is a
  packaged CLI app and does not fit that shape.
- The ref is bumped when the loader changes. (If the self-referential pin becomes a burden,
  the alternative is a build-time copy into the Astro context + `pip install ./people-api-loader`;
  that is a build-pipeline detail, not a DAG-design change.)

Astro Runtime is `python-3.14`, matching the loader's `requires-python = ">=3.14"`. Most
loader deps (`databricks-sdk`, `psycopg`) already appear in the Astro `requirements.txt`.

## Config, secrets, env

Set on the Astro deployment (Environment Manager):

- DATA-1905 values: `LOADER_S3_BUCKET`, `LOADER_S3_IMPORT_ROLE_ARN`, `LOADER_AWS_ACCOUNT_ID`.
- VPC / subnet group / security group / KMS identifiers the loader's `provision` needs.
- Bastion host connection details (reuse the `gp_bastion_host` connection / SSH key).
- AWS: the worker's assumed `gp-people-rds-admin-prod` role.
- Databricks: the `databricks` (and `databricks_dev`) connection.

## File structure

- Create: `airflow/astro/dags/load_people_api_v2.py`
- Modify: `airflow/astro/requirements.txt` (add `astronomer-cosmos`, `dbt-core`,
  `dbt-databricks`, the `people-api-loader` git install)
- Possibly create: `airflow/astro/include/` helper for the Cosmos config / profile mapping
- Modify: `people-api-loader/src/loader/people_api/db.py` (+ `config.py`) for SSH-tunnel
  support, with unit tests; add `sshtunnel`/`paramiko` to the loader's deps

## Testing

- DAG parse/import test in CI (`dag.test()` or a parse check).
- Loader SSH-tunnel unit tests: mock the tunnel; assert direct-vs-tunneled selection by
  config; assert the connection targets the forwarded port when tunneling.
- Non-prod end-to-end run (the acceptance criterion), gated on the dependencies below.

## Dependencies / handoffs (out of scope for this ticket)

- **S3 VPC endpoint** — TDD flags it missing (line 99); needed for Aurora's server-side S3
  import to be efficient. Infra/admin item.
- **DATA-1905 (#38)** merged and applied so the bucket + `rds-s3-import` role exist; env
  vars set per the DATA-1905 comment on DATA-1913.
- A **dev bucket / Databricks path** for the non-prod run (the Terraform creates the prod
  bucket only).
- The loader's **cross-account role + bastion SG rule** confirmed for the worker (DATA-1856).

## Acceptance

- Loader CLI installed on the Astro worker image and invokable (`loader --help`).
- DAG renders in Astro, sequences the subcommands, connects to Postgres via the bastion.
- A full run in non-prod succeeds end-to-end.
