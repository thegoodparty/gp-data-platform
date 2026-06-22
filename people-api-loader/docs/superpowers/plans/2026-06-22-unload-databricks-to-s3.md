# Unload (Databricks → S3) Implementation Plan — DATA-1908

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `unload` CLI step — export the `m_people_api__voter` mart from Databricks to S3 as CSV files (one dir per state), in the column order and format the shipped `copy` step imports, recording an `UnloadManifest`.

**Architecture:** A thin orchestrator: build per-state `INSERT OVERWRITE DIRECTORY ... USING csv` SQL from the `target_schema.sql` column contract, submit via `databricks-sdk` statement-execution against a SQL warehouse, poll to terminal, then assemble the manifest from a per-state count query + an S3 object listing. Pure SQL builders are unit-tested; the orchestration is tested against fake SDK/S3 clients.

**Tech Stack:** Python 3.14, uv, ruff (line-length 110), ty, pytest, Typer, `databricks-sdk` (statement-execution), boto3 (S3 list), pydantic manifests.

**Branch:** `feat/DATA-1908-unload` (off latest `main`). Run `uv run pytest -q`, `uv run ruff check . && uv run ruff format .`, `uv run ty check` after each task; `pre-commit run --all-files` from repo root before any push. All paths relative to `people-api-loader/`.

**Spec:** `docs/superpowers/specs/2026-06-22-unload-databricks-to-s3-design.md`

---

## File structure

**New:**
- `src/loader/core/databricks.py` — thin SDK helper: cached `WorkspaceClient`, `run_statement(cfg, sql) -> StatementResponse` (submit to warehouse, poll to terminal, raise on FAILED/CANCELED).
- `src/loader/people_api/schema/unload_sql.py` — pure builders: ordered SELECT-expr list, the per-state `INSERT OVERWRITE DIRECTORY` SQL, the count-by-state SQL, and `column_types_from_ddl`.
- `tests/test_unload_sql.py`, `tests/test_unload.py`.

**Modified:**
- `src/loader/people_api/config.py` — add `databricks_warehouse_id` (`LOADER_DATABRICKS_WAREHOUSE_ID`).
- `src/loader/people_api/steps/unload.py` — replace the stub with `run()`.
- `src/loader/people_api/steps/copy_s3.py` — switch the import options `FORMAT text` → `FORMAT csv`.
- `tests/test_copy_s3.py` — update the expected options string.
- `.env.example` — document `LOADER_DATABRICKS_WAREHOUSE_ID`.

---

## Task 1: Config — Databricks warehouse id

**Files:** Modify `src/loader/people_api/config.py`; Test `tests/test_config.py`.

- [ ] **Step 1: Failing test** — append to `tests/test_config.py`:

```python
def test_databricks_warehouse_id_from_env(monkeypatch) -> None:
    from loader.people_api.config import LoaderConfig

    monkeypatch.setenv("LOADER_DATABRICKS_WAREHOUSE_ID", "wh-123")
    assert LoaderConfig.from_env().databricks_warehouse_id == "wh-123"


def test_databricks_warehouse_id_defaults_empty(monkeypatch) -> None:
    from loader.people_api.config import LoaderConfig

    monkeypatch.delenv("LOADER_DATABRICKS_WAREHOUSE_ID", raising=False)
    assert LoaderConfig.from_env().databricks_warehouse_id == ""
```

- [ ] **Step 2: Run** `uv run pytest tests/test_config.py -q` → FAIL (`databricks_warehouse_id` missing).

- [ ] **Step 3: Implement** in `config.py`:
  - Add field to the `LoaderConfig` dataclass, near `databricks_table`:
    ```python
    # Databricks SQL warehouse the unload step submits INSERT OVERWRITE DIRECTORY to.
    databricks_warehouse_id: str
    ```
  - In `from_env()`, add to the `cls(...)` kwargs:
    ```python
    databricks_warehouse_id=os.environ.get("LOADER_DATABRICKS_WAREHOUSE_ID", ""),
    ```

- [ ] **Step 4: Run** `uv run pytest tests/test_config.py -q` → PASS. Then `uv run ty check`.

- [ ] **Step 5: Commit**
```bash
git add src/loader/people_api/config.py tests/test_config.py
git commit -m "feat(loader): config for the Databricks SQL warehouse id"
```

---

## Task 2: Pure SQL builders (`unload_sql.py`)

**Files:** Create `src/loader/people_api/schema/unload_sql.py`; Test `tests/test_unload_sql.py`.

Context: `load_target_schema(cfg, run_date)` returns the committed `target_schema.sql`; `extract_create_tables(sql)["Voter"]` gives the Voter CREATE TABLE; `extract_column_names(create_sql)` gives the ordered column names. `schema_spec.TABLE_SPECS["Voter"].extra_columns` lists the Prisma-only `(name, pg_type, nullable)` tuples (currently just `Mailing_HHGender_Description`). `mart_fqns["Voter"]` is the mart FQN.

- [ ] **Step 1: Failing test** — `tests/test_unload_sql.py`:

```python
"""Pure SQL builders for the unload step."""

from __future__ import annotations

from loader.people_api.schema import unload_sql


def test_select_exprs_orders_by_ddl_and_nulls_prisma_extras() -> None:
    ddl_cols = ["id", "LALVOTERID", "State", "Mailing_HHGender_Description"]
    extras = {"Mailing_HHGender_Description"}
    exprs = unload_sql.select_exprs(ddl_cols, extras)
    assert exprs == [
        '`id`',
        '`LALVOTERID`',
        '`State`',
        'NULL AS `Mailing_HHGender_Description`',
    ]


def test_unload_statement_shape() -> None:
    sql = unload_sql.unload_statement(
        mart_fqn="cat.dbt.m_people_api__voter",
        select_exprs=['`id`', '`State`'],
        state="FL",
        s3_dir="s3://b/voter_export_20260622/state=FL/",
    )
    assert "INSERT OVERWRITE DIRECTORY 's3://b/voter_export_20260622/state=FL/'" in sql
    assert "USING csv" in sql
    assert "'sep' = '\\t'" in sql and "'nullValue' = ''" in sql
    assert "'quote' = '\"'" in sql and "'escape' = '\"'" in sql and "'header' = 'false'" in sql
    assert "SELECT `id`, `State`" in sql
    assert "FROM cat.dbt.m_people_api__voter" in sql
    assert "WHERE `State` = 'FL'" in sql


def test_count_by_state_statement() -> None:
    sql = unload_sql.count_by_state_statement("cat.dbt.m_people_api__voter")
    assert sql == (
        "SELECT `State` AS state, count(*) AS n "
        "FROM cat.dbt.m_people_api__voter GROUP BY `State`"
    )


def test_column_types_from_ddl() -> None:
    ddl = (
        'CREATE TABLE public."Voter" (\n'
        '    "id" UUID NOT NULL,\n'
        '    "Age_Int" INTEGER,\n'
        '    "State" TEXT NOT NULL\n'
        ");"
    )
    assert unload_sql.column_types_from_ddl(ddl) == {
        "id": "UUID",
        "Age_Int": "INTEGER",
        "State": "TEXT",
    }
```

- [ ] **Step 2: Run** `uv run pytest tests/test_unload_sql.py -q` → FAIL (module missing).

- [ ] **Step 3: Implement** `src/loader/people_api/schema/unload_sql.py`:

```python
"""Pure SQL builders for the unload step (no I/O — unit-tested in isolation).

The unload projects the mart onto the `target_schema.sql` column order so file column order
matches the `copy` step's column list exactly. Columns the mart lacks (declared Prisma-layer
extras, e.g. the Mailing_HHGender_Description NULL placeholder) are emitted as NULL.
"""

from __future__ import annotations

import re

# Spark CSV OPTIONS, pinned to mirror copy's PG `FORMAT csv` import (tab-delimited CSV, empty
# string = NULL, double-quote for both quote and escape, no header row).
_CSV_OPTIONS = (
    "'sep' = '\\t', 'header' = 'false', 'nullValue' = '', 'quote' = '\"', 'escape' = '\"'"
)

_DDL_COL_RE = re.compile(r'^\s*"(?P<name>[^"]+)"\s+(?P<type>[A-Z][A-Z0-9 ()]*?)(?:\s+NOT NULL)?,?\s*$')


def select_exprs(ddl_columns: list[str], extra_columns: set[str]) -> list[str]:
    """Backtick-quoted SELECT expressions in DDL order; NULL AS for Prisma-only extras."""
    out: list[str] = []
    for col in ddl_columns:
        if col in extra_columns:
            out.append(f"NULL AS `{col}`")
        else:
            out.append(f"`{col}`")
    return out


def unload_statement(*, mart_fqn: str, select_exprs: list[str], state: str, s3_dir: str) -> str:
    cols = ", ".join(select_exprs)
    return (
        f"INSERT OVERWRITE DIRECTORY '{s3_dir}'\n"
        f"USING csv OPTIONS ({_CSV_OPTIONS})\n"
        f"SELECT {cols}\n"
        f"FROM {mart_fqn}\n"
        f"WHERE `State` = '{state}'"
    )


def count_by_state_statement(mart_fqn: str) -> str:
    return f"SELECT `State` AS state, count(*) AS n FROM {mart_fqn} GROUP BY `State`"


def column_types_from_ddl(create_sql: str) -> dict[str, str]:
    """Parse {column: PG type} from a CREATE TABLE block (types are the authoritative PG types)."""
    open_idx = create_sql.find("(")
    close_idx = create_sql.rfind(")")
    body = create_sql[open_idx + 1 : close_idx]
    out: dict[str, str] = {}
    for line in body.splitlines():
        m = _DDL_COL_RE.match(line)
        if m:
            out[m.group("name")] = m.group("type").strip()
    return out
```

Note: `state` values come from the loader's own `STATES` tuple / the mart, not user input, so the literal-interpolated `WHERE` is safe here (matching how `create_schema` interpolates state into partition DDL).

- [ ] **Step 4: Run** `uv run pytest tests/test_unload_sql.py -q` → PASS (4 cases). Then `uv run ruff check . && uv run ruff format . && uv run ty check`.

- [ ] **Step 5: Commit**
```bash
git add src/loader/people_api/schema/unload_sql.py tests/test_unload_sql.py
git commit -m "feat(loader): pure SQL builders for the unload step"
```

---

## Task 3: Databricks statement-execution helper (`core/databricks.py`)

**Files:** Create `src/loader/core/databricks.py`; Test `tests/test_databricks.py`.

- [ ] **Step 1: Failing test** — `tests/test_databricks.py`:

```python
"""core.databricks.run_statement submits to the warehouse and polls to terminal."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.core import databricks
from loader.core.config import BaseLoaderConfig

_CFG = cast(BaseLoaderConfig, SimpleNamespace(databricks_warehouse_id="wh-1"))


class _FakeStmtApi:
    def __init__(self, states: list[str]) -> None:
        self._states = states  # successive statuses to return
        self.submitted: dict | None = None

    def execute_statement(self, **kw: object) -> object:
        self.submitted = kw
        return SimpleNamespace(statement_id="s1", status=SimpleNamespace(state=self._states[0]))

    def get_statement(self, statement_id: str) -> object:
        state = self._states.pop(0) if len(self._states) > 1 else self._states[0]
        return SimpleNamespace(statement_id=statement_id, status=SimpleNamespace(state=state))


def _client(api: _FakeStmtApi) -> object:
    return SimpleNamespace(statement_execution=api)


def test_run_statement_polls_to_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _FakeStmtApi(["PENDING", "RUNNING", "SUCCEEDED"])
    monkeypatch.setattr(databricks, "workspace_client", lambda cfg: _client(api))
    monkeypatch.setattr(databricks, "_poll_sleep", lambda: None)
    resp = databricks.run_statement(_CFG, "SELECT 1", warehouse_id="wh-1")
    assert resp.status.state == "SUCCEEDED"
    assert api.submitted["warehouse_id"] == "wh-1"
    assert api.submitted["statement"] == "SELECT 1"


def test_run_statement_raises_on_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _FakeStmtApi(["RUNNING", "FAILED"])
    monkeypatch.setattr(databricks, "workspace_client", lambda cfg: _client(api))
    monkeypatch.setattr(databricks, "_poll_sleep", lambda: None)
    with pytest.raises(RuntimeError, match="FAILED"):
        databricks.run_statement(_CFG, "SELECT bad", warehouse_id="wh-1")


def test_run_statement_raises_without_warehouse() -> None:
    with pytest.raises(RuntimeError, match="warehouse"):
        databricks.run_statement(_CFG, "SELECT 1", warehouse_id="")
```

- [ ] **Step 2: Run** `uv run pytest tests/test_databricks.py -q` → FAIL.

- [ ] **Step 3: Implement** `src/loader/core/databricks.py`:

```python
"""Thin databricks-sdk helpers (statement execution against a SQL warehouse).

Mirrors core/aws.py's thin-client style. The SDK is imported lazily so unit tests that patch
`workspace_client` don't require the SDK or live credentials.
"""

from __future__ import annotations

import time
from typing import Any

from loader.core.config import BaseLoaderConfig

_TERMINAL_OK = {"SUCCEEDED"}
_TERMINAL_BAD = {"FAILED", "CANCELED", "CLOSED"}
_POLL_SECONDS = 5


def workspace_client(cfg: BaseLoaderConfig) -> Any:
    """A databricks WorkspaceClient (auth from standard databricks env/config)."""
    del cfg  # auth is ambient; cfg reserved for a future explicit-host path
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _poll_sleep() -> None:
    time.sleep(_POLL_SECONDS)


def run_statement(cfg: BaseLoaderConfig, statement: str, *, warehouse_id: str) -> Any:
    """Submit `statement` to `warehouse_id` and poll to a terminal state.

    `warehouse_id` is passed in (not read off cfg) so this core helper stays consumer-agnostic
    — per CLAUDE.md, core/ must not depend on people-api-specific config fields. Raises if no
    warehouse is given or the statement ends FAILED/CANCELED/CLOSED.
    """
    if not warehouse_id:
        raise RuntimeError("no warehouse_id — set LOADER_DATABRICKS_WAREHOUSE_ID.")
    api = workspace_client(cfg).statement_execution
    resp = api.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="0s")
    statement_id = resp.statement_id
    state = resp.status.state
    while state not in _TERMINAL_OK and state not in _TERMINAL_BAD:
        _poll_sleep()
        resp = api.get_statement(statement_id)
        state = resp.status.state
    if state in _TERMINAL_BAD:
        raise RuntimeError(f"Databricks statement {statement_id} ended {state}: {statement[:120]}")
    return resp
```

Note: `core/` stays consumer-agnostic — `warehouse_id` is a parameter, not read from cfg. `cfg` is used only to build the `WorkspaceClient` (ambient auth). The people-api `unload` step passes `warehouse_id=cfg.databricks_warehouse_id`.

- [ ] **Step 4: Run** `uv run pytest tests/test_databricks.py -q` → PASS. `uv run ty check` (resolve the config-attr hint per the note). `uv run ruff check . && uv run ruff format .`.

- [ ] **Step 5: Commit**
```bash
git add src/loader/core/databricks.py tests/test_databricks.py
git commit -m "feat(loader): databricks statement-execution helper"
```

---

## Task 4: The `unload` step

**Files:** Modify `src/loader/people_api/steps/unload.py`; Test `tests/test_unload.py`.

Context: the stub is `run(cfg, run_date, *, state_filter=None, skip_submit=False) -> UnloadManifest`. `cfg.export_prefix(run_date)` → `voter_export_{run_date}`; `cfg.s3_bucket`; `cfg.aws_region`. `STATES` is `loader.people_api.schema.states.STATES`. S3 listing uses `s3(cfg)` from `core.aws` (paginator `list_objects_v2`, as in `teardown._delete_s3_prefix`).

- [ ] **Step 1: Failing test** — `tests/test_unload.py`:

```python
"""unload: builds per-state INSERT OVERWRITE DIRECTORY, polls, assembles the manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import unload as step

_DDL = (
    'CREATE TABLE public."Voter" (\n'
    '    "id" UUID NOT NULL,\n'
    '    "State" TEXT NOT NULL,\n'
    '    "Mailing_HHGender_Description" TEXT\n'
    ");"
)

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        s3_bucket="b",
        aws_region="us-west-2",
        databricks_warehouse_id="wh-1",
        mart_fqns={"Voter": "cat.dbt.m_people_api__voter"},
        export_prefix=lambda rd: f"voter_export_{rd}",
    ),
)


class _FakeS3:
    """Lists two part files for FL, one for CA."""

    def get_paginator(self, name: str) -> Any:
        pages = {
            "voter_export_20260622/state=FL/": [{"Key": "voter_export_20260622/state=FL/part-0", "Size": 10}],
            "voter_export_20260622/state=CA/": [{"Key": "voter_export_20260622/state=CA/part-0", "Size": 7}],
        }

        class _P:
            def paginate(self, Bucket: str, Prefix: str) -> Any:
                return iter([{"Contents": pages.get(Prefix, [])}])

        return _P()


def _patch(monkeypatch: pytest.MonkeyPatch, submitted: list[str]) -> None:
    monkeypatch.setattr(step, "STATES", ("FL", "CA"))
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(step, "s3", lambda cfg: _FakeS3())
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _run_statement(cfg: object, sql: str, **kw: object) -> object:
        submitted.append(sql)
        return SimpleNamespace(
            result=SimpleNamespace(data_array=[["FL", "3"], ["CA", "2"]])
        )

    monkeypatch.setattr(step, "run_statement", _run_statement)


def test_unload_builds_per_state_sql_and_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    manifest = step.run(_CFG, "20260622")
    # one INSERT per state + one count query
    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    assert len(inserts) == 2
    assert any("state=FL/" in s and "NULL AS `Mailing_HHGender_Description`" in s for s in inserts)
    assert manifest.status == "complete"
    assert manifest.columns == ["id", "State", "Mailing_HHGender_Description"]
    assert manifest.column_types_pg == {"id": "UUID", "State": "TEXT", "Mailing_HHGender_Description": "TEXT"}
    assert manifest.per_state_row_counts == {"FL": 3, "CA": 2}
    assert {f.state for f in manifest.files} == {"FL", "CA"}
    assert any(f.s3_key.endswith("state=FL/part-0") and f.size_bytes == 10 for f in manifest.files)


def test_unload_state_filter_submits_one_state(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    step.run(_CFG, "20260622", state_filter="FL")
    inserts = [s for s in submitted if "INSERT OVERWRITE DIRECTORY" in s]
    assert len(inserts) == 1 and "state=FL/" in inserts[0]


def test_unload_skip_submit_builds_no_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    submitted: list[str] = []
    _patch(monkeypatch, submitted)
    manifest = step.run(_CFG, "20260622", skip_submit=True)
    assert submitted == []  # nothing executed
    assert manifest.status == "complete"


def test_unload_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260622") is done
```

- [ ] **Step 2: Run** `uv run pytest tests/test_unload.py -q` → FAIL (stub raises NotImplementedError).

- [ ] **Step 3: Implement** `src/loader/people_api/steps/unload.py`:

```python
"""Step 1 — unload the people-api Voter mart from Databricks to S3 (DATA-1908).

Per state, issues `INSERT OVERWRITE DIRECTORY ... USING csv` against the SQL warehouse, writing
CSV files the `copy` step imports via `aws_s3.table_import_from_s3` (FORMAT csv). Columns are
ordered from `target_schema.sql` so file layout matches create-schema/copy by construction;
the declared Prisma-extra columns the mart lacks are emitted as NULL. Records an UnloadManifest.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import s3
from loader.core.databricks import run_statement
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import (
    UnloadFile,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema import unload_sql
from loader.people_api.schema.schema_spec import TABLE_SPECS
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.states import STATES
from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

log = get_logger(__name__)

_TARGET_TABLE = "Voter"


def _list_state_files(cfg: LoaderConfig, prefix: str, state: str) -> list[UnloadFile]:
    """Enumerate written part files for a state; row_count is best-effort (see spec)."""
    state_prefix = f"{prefix}/state={state}/"
    files: list[UnloadFile] = []
    paginator = s3(cfg).get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=cfg.s3_bucket, Prefix=state_prefix):
        for obj in page.get("Contents", []):
            files.append(UnloadFile(state=state, s3_key=obj["Key"], size_bytes=obj["Size"], row_count=0))
    return files


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    skip_submit: bool = False,
) -> UnloadManifest:
    bind(run_date=run_date, step="unload")
    existing = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if existing and existing.status == "complete" and state_filter is None:
        log.info("unload.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "unload"))
        return existing

    mart_fqn = cfg.mart_fqns[_TARGET_TABLE]
    create_sql = extract_create_tables(load_target_schema(cfg, run_date))[_TARGET_TABLE]
    ddl_columns = extract_column_names(create_sql)
    extras = {name for name, _type, _nullable in TABLE_SPECS[_TARGET_TABLE].extra_columns}
    exprs = unload_sql.select_exprs(ddl_columns, extras)
    column_types_pg = unload_sql.column_types_from_ddl(create_sql)

    states = [state_filter] if state_filter else list(STATES)
    prefix = cfg.export_prefix(run_date)
    started = datetime.now(UTC)
    log.info("unload.start", mart=mart_fqn, states=len(states), skip_submit=skip_submit)

    for state in states:
        s3_dir = f"s3://{cfg.s3_bucket}/{prefix}/state={state}/"
        sql = unload_sql.unload_statement(mart_fqn=mart_fqn, select_exprs=exprs, state=state, s3_dir=s3_dir)
        if skip_submit:
            log.info("unload.sql_preview", state=state, sql=sql)
            continue
        run_statement(cfg, sql, warehouse_id=cfg.databricks_warehouse_id)
        log.info("unload.state_written", state=state)

    per_state_row_counts: dict[str, int] = {}
    files: list[UnloadFile] = []
    if not skip_submit:
        count_resp = run_statement(
            cfg, unload_sql.count_by_state_statement(mart_fqn), warehouse_id=cfg.databricks_warehouse_id
        )
        for row in count_resp.result.data_array or []:
            per_state_row_counts[row[0]] = int(row[1])
        for state in states:
            files.extend(_list_state_files(cfg, prefix, state))

    manifest = UnloadManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        databricks_table=mart_fqn,
        columns=ddl_columns,
        column_types_pg=column_types_pg,
        per_state_row_counts=per_state_row_counts,
        files=files,
    )
    uri = write_manifest(cfg, manifest)
    log.info("unload.complete", uri=uri, states=len(states), files=len(files))
    return manifest
```

- [ ] **Step 4: Run** `uv run pytest tests/test_unload.py -q` → PASS (4 cases). Then `uv run ty check && uv run ruff check . && uv run ruff format .`.

- [ ] **Step 5: Commit**
```bash
git add src/loader/people_api/steps/unload.py tests/test_unload.py
git commit -m "feat(loader): implement the unload step (Databricks mart -> S3)"
```

---

## Task 5: Switch `copy` import to FORMAT csv

**Files:** Modify `src/loader/people_api/steps/copy_s3.py`; Test `tests/test_copy_s3.py`.

- [ ] **Step 1: Update the expected options in `tests/test_copy_s3.py`.** Find the assertion that inspects the `table_import_from_s3` params (around line 49, `import_params`) and assert the new options:

```python
    assert import_params["options"] == "(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8')"
```
(If the existing test only checks the call happened, add this exact-options assertion.)

- [ ] **Step 2: Run** `uv run pytest tests/test_copy_s3.py -q` → FAIL on the options mismatch.

- [ ] **Step 3: Implement** in `copy_s3.py` `_copy_one_file`, replace the options value:

```python
                "options": "(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8')",
```
Update the nearby comment to note: CSV (tab-delimited) paired with the unload's Spark CSV writer — quoting/escaping handle embedded tab/newline/quote; `NULL ''` round-trips nulls with the unload's `nullValue=''`.

- [ ] **Step 4: Run** `uv run pytest tests/test_copy_s3.py -q` → PASS. `uv run ty check`.

- [ ] **Step 5: Commit**
```bash
git add src/loader/people_api/steps/copy_s3.py tests/test_copy_s3.py
git commit -m "fix(loader): copy imports FORMAT csv to match the unload CSV contract"
```

---

## Task 6: Docs + final gate + push

**Files:** Modify `.env.example`.

- [ ] **Step 1:** add to `.env.example`:
```
# Databricks SQL warehouse the unload step submits to (statement execution).
LOADER_DATABRICKS_WAREHOUSE_ID=
```

- [ ] **Step 2: Full local gate** —
```bash
uv run pytest -q && uv run ty check && uv run ruff check . && uv run ruff format --check .
```
Expected: all pass (existing suite + the new unload/databricks/unload_sql tests; copy tests updated).

- [ ] **Step 3:** from repo root, `pre-commit run --all-files` → all pass.

- [ ] **Step 4: Commit + push**
```bash
git add people-api-loader/.env.example
git commit -m "docs(loader): document LOADER_DATABRICKS_WAREHOUSE_ID"
git push -u origin feat/DATA-1908-unload
```

- [ ] **Step 5:** open the PR; address any delegate-reviewer findings per the established loop.

---

## Notes for the implementer

- **Databricks creds / warehouse:** an end-to-end `loader unload` run needs `LOADER_DATABRICKS_WAREHOUSE_ID` + Databricks auth, and the DATA-1905 bucket + UC external location applied (Databricks WRITE) — those are operator/ops prerequisites, not unit-test concerns. The unit tests fully cover the code via fakes.
- **`data_array` shape:** the SQL-warehouse statement result returns rows as `result.data_array` (list of string lists). The count query maps `[state, n]`; cast `n` to int. If the SDK version returns a different result accessor, adapt `run_statement`'s return handling — keep the `(state, count)` contract the step depends on.
- **Per-file row_count is 0 by design** (the spec's documented choice): `copy` uses `s3_key` + `size_bytes`; `validate` uses `per_state_row_counts`.
- **Do not change** `target_schema.sql`, `schema_spec`, or `_serving_seed` — unload only reads them.
