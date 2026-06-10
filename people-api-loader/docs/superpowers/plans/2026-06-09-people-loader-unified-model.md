# People-API Loader — Unified Single-`Voter`-Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `create-schema`, `copy`, `build-indexes`, and `validate` against the real prod schema: one `public."Voter"` table (not 51 per-state tables).

**Architecture:** The committed prod `pg_dump` snapshot is the source of truth. `create-schema` applies the extracted `CREATE TABLE public."Voter"` (tables only); `copy` loads per-state S3 files in parallel into the one `Voter` table, idempotent on the `"State"` column; `build-indexes` applies the PK + 266 indexes parsed from the snapshot to the single table; `validate` runs five checks (per-state counts via `GROUP BY "State"` at ±10%). Reuses scaffolding from the closed branch `feat/DATA-1851-pg-data-plane-steps`.

**Tech Stack:** Python 3.12, uv, Typer, psycopg 3, boto3, pydantic v2, structlog. Tests: pytest + monkeypatch + a fake psycopg connection (no moto, no live DB).

**Spec:** `docs/superpowers/specs/2026-06-09-people-loader-unified-model-design.md`.

---

## Conventions for every task

- Work in repo `/Users/hugh/Documents/repos_4/gp-data-platform`, on branch `feat/DATA-1640-people-loader-unified` (already created from `main`). The loader package is under `people-api-loader/`.
- Run tests from `people-api-loader/`: `uv run pytest <path> -v`. Lint/type: `uv run ruff check . && uv run ruff format --check . && uv run ty check`.
- pre-commit runs ruff repo-wide on commit; commits must be ruff-clean. **RUF100 is enabled and BLE001 is not in the select set** — never use `# noqa: BLE001` (it is auto-stripped/flagged); use a plain comment on intentional broad `except Exception`. Dynamic-string `cur.execute(<str var>)` needs `# ty: ignore[no-matching-overload]`; literal SQL strings with a params tuple do not.
- The reusable, unchanged files are recreated from the closed branch via `git show feat/DATA-1851-pg-data-plane-steps:<path> > <path>`.
- The real prod snapshot is preserved at `/tmp/voter_prod_dump_real.sql`.
- Commit messages: `feat(DATA-XXXX): ...`.

---

## Task 0: Confirm branch

- [ ] **Step 1: Verify**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git branch --show-current   # expect: feat/DATA-1640-people-loader-unified
cd people-api-loader && uv run pytest -q   # baseline scaffolding tests pass
```
Expected: on the branch; existing tests green.

---

## Task 1: Carry over reusable scaffolding + commit prod snapshot

**Files:**
- Recreate: `src/loader/people_api/schema/__init__.py`, `src/loader/people_api/schema/index_specs.py`, `tests/_fakes.py`, `tests/test_index_specs.py`, `tests/test_resolve_writer_endpoint.py`
- Create: `src/loader/people_api/schema/snapshot.py`, `src/loader/people_api/schema/data/prod_dump.sql`, `tests/test_snapshot.py`
- Modify: `src/loader/people_api/db.py`, repo-root `.pre-commit-config.yaml`

- [ ] **Step 1: Recreate the verbatim-reusable files from the closed branch**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform/people-api-loader
mkdir -p src/loader/people_api/schema/data
git show feat/DATA-1851-pg-data-plane-steps:people-api-loader/src/loader/people_api/schema/index_specs.py > src/loader/people_api/schema/index_specs.py
git show feat/DATA-1851-pg-data-plane-steps:people-api-loader/tests/_fakes.py > tests/_fakes.py
git show feat/DATA-1851-pg-data-plane-steps:people-api-loader/tests/test_index_specs.py > tests/test_index_specs.py
git show feat/DATA-1851-pg-data-plane-steps:people-api-loader/tests/test_resolve_writer_endpoint.py > tests/test_resolve_writer_endpoint.py
```

- [ ] **Step 2: Minimal schema package `__init__.py`** (the old one re-exported the now-retired `voter_columns`; this must not)

Create `src/loader/people_api/schema/__init__.py`:

```python
"""Schema helpers for the people-API loader: pg_dump parsing + the committed snapshot."""
```

- [ ] **Step 3: `snapshot.py` (prod-dump only; no Databricks)**

Create `src/loader/people_api/schema/snapshot.py`:

```python
"""Committed static snapshot of the prod schema.

`inspect-prod` (DATA-1907) is out of scope, so `create-schema` and
`build-indexes` read the prod `pg_dump` from a versioned snapshot committed
under `schema/data/` instead of from S3. Override with `LOADER_PROD_DUMP_PATH`
(used by tests and ad-hoc runs).
"""

from __future__ import annotations

import os
from pathlib import Path

from loader.people_api.config import LoaderConfig

DATA_DIR = Path(__file__).parent / "data"


def load_prod_dump(cfg: LoaderConfig, run_date: str) -> str:
    del cfg, run_date  # reserved for a future inspect-manifest path
    override = os.environ.get("LOADER_PROD_DUMP_PATH")
    path = Path(override) if override else DATA_DIR / "prod_dump.sql"
    return path.read_text(encoding="utf-8")
```

- [ ] **Step 4: `resolve_writer_endpoint` in `db.py`**

`db.py` on `main` does not have it. Add `import os` if missing, and at module top add:

```python
from loader.people_api.manifests import ProvisionManifest, read_manifest
```

Append to `src/loader/people_api/db.py`:

```python
def resolve_writer_endpoint(cfg: LoaderConfig, run_date: str) -> str:
    """Resolve the new cluster's writer endpoint.

    `provision` (DATA-1909) is out of scope for this PR. Resolution order:
    1. `LOADER_NEW_WRITER_ENDPOINT` env override (point at an existing cluster).
    2. A completed `provision` manifest, if one exists for this run.
    3. Otherwise raise.
    """
    override = os.environ.get("LOADER_NEW_WRITER_ENDPOINT")
    if override:
        return override
    prov = read_manifest(cfg, run_date, "provision", ProvisionManifest)
    if prov is not None and prov.status == "complete":
        return prov.writer_endpoint
    raise RuntimeError(
        "No new-cluster writer endpoint available. Set "
        "$LOADER_NEW_WRITER_ENDPOINT to the target cluster, or run "
        "`loader provision --date <run_date>` first."
    )
```

- [ ] **Step 5: pre-commit exclude for the snapshot artifact**

In repo-root `.pre-commit-config.yaml`, add to the `sqlfmt` hook so the committed `pg_dump` is not reformatted (it would corrupt the artifact and break the `index_specs` parser):

```yaml
        exclude: ^people-api-loader/src/loader/people_api/schema/data/
```

- [ ] **Step 6: Commit the real prod snapshot**

```bash
cp /tmp/voter_prod_dump_real.sql src/loader/people_api/schema/data/prod_dump.sql
grep -c 'CREATE TABLE public."Voter"' src/loader/people_api/schema/data/prod_dump.sql   # expect 1
grep -c 'CREATE INDEX' src/loader/people_api/schema/data/prod_dump.sql                   # expect ~266
```

- [ ] **Step 7: `tests/test_snapshot.py` (prod-dump only)**

Create `tests/test_snapshot.py`:

```python
"""snapshot.load_prod_dump reads the committed file, honoring the env override."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema import snapshot

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_load_prod_dump_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "dump.sql"
    p.write_text('CREATE TABLE public."Voter" ("id" uuid);', encoding="utf-8")
    monkeypatch.setenv("LOADER_PROD_DUMP_PATH", str(p))
    assert 'public."Voter"' in snapshot.load_prod_dump(_CFG, "20260609")


def test_committed_snapshot_has_voter_table() -> None:
    text = (snapshot.DATA_DIR / "prod_dump.sql").read_text(encoding="utf-8")
    assert 'CREATE TABLE public."Voter"' in text
```

- [ ] **Step 8: Run the carried tests, then commit**

```bash
uv run pytest tests/test_index_specs.py tests/test_resolve_writer_endpoint.py tests/test_snapshot.py -v
uv run ruff check . && uv run ty check
```
Expected: all pass; clean. Then:

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/ .pre-commit-config.yaml
git commit -m "feat(DATA-1640): carry over loader scaffolding + commit prod snapshot"
```

---

## Task 2: Add `state` to `CopyTableResult`

**Files:**
- Modify: `src/loader/people_api/manifests.py`
- Test: `tests/test_copy_manifest.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_copy_manifest.py`:

```python
"""CopyTableResult carries a per-state discriminator (table is always 'Voter')."""

from __future__ import annotations

from loader.people_api.manifests import CopyTableResult


def test_copy_table_result_has_state() -> None:
    r = CopyTableResult(
        table="Voter", state="TX", expected_rows=100, actual_rows=100,
        files_loaded=3, seconds_elapsed=1.5,
    )
    assert r.state == "TX"
    assert r.table == "Voter"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_copy_manifest.py -v`
Expected: FAIL — `CopyTableResult` has no `state` field (pydantic rejects the kwarg).

- [ ] **Step 3: Add the field**

In `src/loader/people_api/manifests.py`, change `CopyTableResult`:

```python
class CopyTableResult(BaseModel):
    table: str
    state: str
    expected_rows: int
    actual_rows: int
    files_loaded: int
    seconds_elapsed: float
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_copy_manifest.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/manifests.py people-api-loader/tests/test_copy_manifest.py
git commit -m "feat(DATA-1851): add state field to CopyTableResult"
```

---

## Task 3: `CREATE TABLE` extraction helper

**Files:**
- Create: `src/loader/people_api/schema/table_ddl.py`
- Test: `tests/test_table_ddl.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_table_ddl.py`:

```python
"""extract_create_tables returns CREATE TABLE statements only (no indexes/PK)."""

from __future__ import annotations

from loader.people_api.schema.table_ddl import extract_create_tables

_DUMP = '''
CREATE TABLE public."Voter" (
    "LALVOTERID" text NOT NULL,
    "State" text NOT NULL,
    created_at timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id uuid NOT NULL
);

ALTER TABLE ONLY public."Voter"
    ADD CONSTRAINT "Voter_pkey" PRIMARY KEY (id);

CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");
'''


def test_extracts_voter_table_only() -> None:
    tables = extract_create_tables(_DUMP)
    assert set(tables) == {"Voter"}
    stmt = tables["Voter"]
    assert stmt.startswith('CREATE TABLE public."Voter" (')
    assert stmt.rstrip().endswith(");")
    # columns present, but no index/PK statements pulled in
    assert '"LALVOTERID" text NOT NULL' in stmt
    assert "timestamp(3)" in stmt  # a ')' inside the body must not terminate the match
    assert "CREATE INDEX" not in stmt
    assert "ADD CONSTRAINT" not in stmt
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_table_ddl.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement**

Create `src/loader/people_api/schema/table_ddl.py`:

```python
"""Extract CREATE TABLE statements from a pg_dump --schema-only file.

`create-schema` applies tables only; pg_dump emits indexes/PKs as separate
statements (CREATE INDEX / ALTER TABLE ADD CONSTRAINT) which are intentionally
ignored here and applied later by `build-indexes`.
"""

from __future__ import annotations

import re

# Matches one `CREATE TABLE public."X" ( ... );` block. The body is non-greedy
# up to the first `)` that begins the closing line; a `)` inside the body (e.g.
# `timestamp(3)`) is never followed by `;` so it cannot terminate the match.
_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:(?:public|"public")\.)?"(?P<name>[^"]+)"\s*'
    r"\(.*?\)\s*;",
    re.IGNORECASE | re.DOTALL,
)


def extract_create_tables(sql: str) -> dict[str, str]:
    """Return {table_name: full CREATE TABLE statement} parsed from a pg_dump."""
    out: dict[str, str] = {}
    for m in _CREATE_TABLE_RE.finditer(sql):
        out[m.group("name")] = m.group(0)
    return out
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_table_ddl.py -v`
Expected: PASS.

- [ ] **Step 5: Sanity-check against the real snapshot**

Run:
```bash
uv run python -c "
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.table_ddl import extract_create_tables
t = extract_create_tables(load_prod_dump(None, '0'))
print(sorted(t)); print('Voter stmt bytes:', len(t['Voter']))
assert list(t) == ['Voter'] and t['Voter'].rstrip().endswith(');')
"
```
Expected: `['Voter']`, a multi-thousand-byte statement, no assertion error.

- [ ] **Step 6: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/schema/table_ddl.py people-api-loader/tests/test_table_ddl.py
git commit -m "feat(DATA-1910): add CREATE TABLE extraction helper"
```

---

## Task 4: `create-schema` step (DATA-1910)

**Files:**
- Modify (replace stub): `src/loader/people_api/steps/create_schema.py`
- Test: `tests/test_create_schema.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_create_schema.py`:

```python
"""create-schema: extracts CREATE TABLE Voter, installs extensions, applies it."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import create_schema as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))
_DUMP = (
    'CREATE TABLE public."Voter" ("id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE INDEX "Voter_State_idx" ON public."Voter" USING btree ("State");\n'
)


def _patch(monkeypatch: pytest.MonkeyPatch, conn: FakeConn) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: f"s3://b/{sub}")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    return captured


def test_applies_table_and_extensions_only(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    captured = _patch(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE" in s for s in sql)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_commons" in s for s in sql)
    assert any('CREATE TABLE public."Voter"' in s for s in sql)
    # indexes are NOT applied by create-schema
    assert not any("CREATE INDEX" in s for s in sql)
    assert manifest.status == "complete"
    assert manifest.tables_created == ["Voter"]


def test_skips_when_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "s3://b/x")
    assert step.run(_CFG, "20260609") is done
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_create_schema.py -v`
Expected: FAIL — stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub**

Overwrite `src/loader/people_api/steps/create_schema.py`:

```python
"""Step 3 — create the Voter table on the new cluster (ClickUp DATA-1910).

Applies the `CREATE TABLE public."Voter"` extracted from the committed prod
snapshot (tables only — indexes/PK are deferred to build-indexes), after
installing the aws_s3/aws_commons extensions.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    SchemaManifest,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.table_ddl import extract_create_tables

log = get_logger(__name__)

_TARGET_TABLE = "Voter"


def run(cfg: LoaderConfig, run_date: str) -> SchemaManifest:
    bind(run_date=run_date, step="schema")
    existing = read_manifest(cfg, run_date, "schema", SchemaManifest)
    if existing and existing.status == "complete":
        log.info(
            "schema.skip",
            reason="manifest already complete",
            uri=manifest_uri(cfg, run_date, "schema"),
        )
        return existing

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("schema.start")

    dump = load_prod_dump(cfg, run_date)
    tables = extract_create_tables(dump)
    if _TARGET_TABLE not in tables:
        raise RuntimeError(
            f'snapshot has no CREATE TABLE public."{_TARGET_TABLE}" (found: {sorted(tables)})'
        )
    create_sql = tables[_TARGET_TABLE]

    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", create_sql)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(create_sql))

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        with conn.cursor() as cur:
            # create_sql is plain str (assembled from the snapshot, not user input);
            # psycopg's parameterless execute overloads want LiteralString.
            cur.execute(create_sql)  # ty: ignore[no-matching-overload]
        log.info("schema.ddl_applied")

    manifest = SchemaManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        target_schema_s3_uri=ddl_uri,
        tables_created=[_TARGET_TABLE],
        column_diff_from_prod={},
    )
    uri = write_manifest(cfg, manifest)
    log.info("schema.complete", uri=uri, tables=1)
    return manifest
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_create_schema.py -v` then `uv run ruff check . && uv run ty check`
Expected: PASS; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/steps/create_schema.py people-api-loader/tests/test_create_schema.py
git commit -m "feat(DATA-1910): implement create-schema for unified Voter table"
```

---

## Task 5: `copy` step (DATA-1851)

**Files:**
- Modify (replace stub): `src/loader/people_api/steps/copy_s3.py`
- Test: `tests/test_copy_s3.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_copy_s3.py`:

```python
"""copy: per-file aws_s3 import into public.\"Voter\", State-keyed idempotency."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import copy_s3 as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b", aws_region="us-west-2"))


def _unload(files, counts):
    return SimpleNamespace(status="complete", files=files, per_state_row_counts=counts)


def test_copy_one_file_targets_voter_with_session_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(_CFG, "20260609", "wh", "voter_export_20260609/state_id=TX/part-0.csv")
    sql = executed_sql(conn)
    assert any("aws_s3.table_import_from_s3" in s for s in sql)
    assert any("SET synchronous_commit = off" in s for s in sql)


def test_load_state_skips_when_count_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((100,))  # existing rows == expected
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_state(
        cfg=_CFG, run_date="20260609", writer_endpoint="wh", state="TX",
        expected_rows=100, s3_keys=["k"], parallelism=1,
    )
    assert r.files_loaded == 0 and r.state == "TX" and r.table == "Voter"


def test_run_completes_and_records_state(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(
        step, "_load_state",
        lambda **kw: step.CopyTableResult(
            table="Voter", state=kw["state"], expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"], files_loaded=1, seconds_elapsed=1.0),
    )
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert manifest.results[0].state == "TX"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_copy_s3.py -v`
Expected: FAIL — stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub**

Overwrite `src/loader/people_api/steps/copy_s3.py`:

```python
"""Step 4 — parallel COPY S3 → Aurora into the unified Voter table (DATA-1851).

A ThreadPoolExecutor issues one `aws_s3.table_import_from_s3` per file, all
targeting `public."Voter"`; the `"State"` column comes from the data. PG's COPY
is single-threaded per statement, so file-level parallelism is the lever.

Idempotency is per-state on the `"State"` column: count rows for the state vs
the unload baseline. Equal → skip. Zero → load. Partial → DELETE that state's
rows, then reload.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    CopyManifest,
    CopyTableResult,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

_DEFAULT_PARALLELISM = 128
_TARGET_TABLE = "Voter"

_SESSION_SQL: tuple[str, ...] = (
    "SET synchronous_commit = off",
    "SET maintenance_work_mem = '4GB'",
    "SET work_mem = '256MB'",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _copy_one_file(cfg: LoaderConfig, run_date: str, writer_endpoint: str, s3_key: str) -> None:
    """Import one S3 file into public.\"Voter\" on its own backend."""
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        for stmt in _SESSION_SQL:
            cur.execute(stmt)  # ty: ignore[no-matching-overload]
        cur.execute(
            """
            SELECT aws_s3.table_import_from_s3(
                %(table)s,
                '',
                %(options)s,
                aws_commons.create_s3_uri(%(bucket)s, %(key)s, %(region)s)
            )
            """,
            {
                "table": f'public."{_TARGET_TABLE}"',
                "options": "(FORMAT text, DELIMITER E'\\t', NULL '\\N', ENCODING 'UTF8')",
                "bucket": cfg.s3_bucket,
                "key": s3_key,
                "region": cfg.aws_region,
            },
        )


def _count_state_rows(conn: psycopg.Connection, state: str) -> int:
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM public."Voter" WHERE "State" = %s', (state,))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _delete_state(conn: psycopg.Connection, state: str) -> None:
    with conn.cursor() as cur:
        cur.execute('DELETE FROM public."Voter" WHERE "State" = %s', (state,))


def _load_state(
    *,
    cfg: LoaderConfig,
    run_date: str,
    writer_endpoint: str,
    state: str,
    expected_rows: int,
    s3_keys: list[str],
    parallelism: int,
) -> CopyTableResult:
    bind(state=state)
    started = time.time()

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_state_rows(conn, state)
        if actual == expected_rows and expected_rows > 0:
            log.info("copy.skip", state=state, rows=actual)
            return CopyTableResult(
                table=_TARGET_TABLE, state=state, expected_rows=expected_rows,
                actual_rows=actual, files_loaded=0, seconds_elapsed=0.0,
            )
        if actual > 0 and actual != expected_rows:
            log.info("copy.partial_reload", state=state, existing_rows=actual, expected=expected_rows)
            _delete_state(conn, state)

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(_copy_one_file, cfg, run_date, writer_endpoint, key): key
            for key in s3_keys
        }
        errors: list[tuple[str, Exception]] = []
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                fut.result()
                log.info("copy.file_done", state=state, key=key)
            except Exception as e:  # broad by design: aggregate worker failures, re-raise below
                log.error("copy.file_failed", state=state, key=key, error=str(e))
                errors.append((key, e))
        if errors:
            raise RuntimeError(f"{state}: {len(errors)} files failed — first: {errors[0][1]!r}")

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_state_rows(conn, state)
    elapsed = time.time() - started
    log.info("copy.state_done", state=state, rows=actual, files=len(s3_keys), seconds=round(elapsed, 1))
    return CopyTableResult(
        table=_TARGET_TABLE, state=state, expected_rows=expected_rows,
        actual_rows=actual, files_loaded=len(s3_keys), seconds_elapsed=elapsed,
    )


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    parallelism: int = _DEFAULT_PARALLELISM,
) -> CopyManifest:
    bind(run_date=run_date, step="copy")
    existing = read_manifest(cfg, run_date, "copy", CopyManifest)
    if existing and existing.status == "complete" and state_filter is None:
        log.info("copy.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "copy"))
        return existing

    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if unload is None or unload.status != "complete":
        raise RuntimeError("Step 4 requires a completed unload manifest.")

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("copy.start", state_filter=state_filter, parallelism=parallelism, writer_endpoint=writer_endpoint)

    files_by_state: dict[str, list[str]] = {}
    for f in unload.files:
        if f.size_bytes == 0:
            continue
        files_by_state.setdefault(f.state, []).append(f.s3_key)

    states_to_load = [state_filter] if state_filter else sorted(files_by_state.keys())

    results: list[CopyTableResult] = []
    if existing is not None and existing.results:
        results.extend(r for r in existing.results if r.actual_rows > 0)
    already_done = {r.state for r in results}

    for state in sorted(states_to_load, key=lambda s: -unload.per_state_row_counts.get(s, 0)):
        if state in already_done:
            log.info("copy.state_carried", state=state)
            continue
        result = _load_state(
            cfg=cfg, run_date=run_date, writer_endpoint=writer_endpoint, state=state,
            expected_rows=unload.per_state_row_counts.get(state, 0),
            s3_keys=files_by_state.get(state, []), parallelism=parallelism,
        )
        results = [r for r in results if r.state != state] + [result]

    covered = {r.state for r in results}
    expected_states = set(unload.per_state_row_counts)
    all_loaded = covered >= expected_states

    manifest = CopyManifest(
        run_date=run_date,
        status="complete" if all_loaded else "in_progress",
        started_at=started,
        finished_at=datetime.now(UTC) if all_loaded else None,
        results=results,
    )
    uri = write_manifest(cfg, manifest)
    log.info("copy.complete", uri=uri, all_loaded=all_loaded, covered=len(covered), expected=len(expected_states))
    return manifest
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_copy_s3.py -v` then `uv run ruff check . && uv run ty check`
Expected: PASS; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/steps/copy_s3.py people-api-loader/tests/test_copy_s3.py
git commit -m "feat(DATA-1851): implement copy into unified Voter table (State-keyed)"
```

---

## Task 6: `build-indexes` step (DATA-1853)

**Files:**
- Modify (replace stub): `src/loader/people_api/steps/build_indexes.py`
- Test: `tests/test_build_indexes.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_build_indexes.py`:

```python
"""build-indexes: applies PK + indexes to public.\"Voter\", then ANALYZE."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

_DUMP = '''
ALTER TABLE ONLY public."Voter" ADD CONSTRAINT "Voter_pkey" PRIMARY KEY (id);
CREATE UNIQUE INDEX "Voter_LALVOTERID_key" ON public."Voter" USING btree ("LALVOTERID");
CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");
'''


def test_rewrite_injects_if_not_exists() -> None:
    assert "CREATE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE INDEX "x" ON public."Voter" ("Active");'
    )
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE UNIQUE INDEX "u" ON public."Voter" ("LALVOTERID");'
    )


def test_run_builds_pk_indexes_and_analyzes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, we: [])
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("ADD CONSTRAINT" in s and "PRIMARY KEY" in s for s in sql)
    assert any("CREATE UNIQUE INDEX IF NOT EXISTS" in s for s in sql)
    assert any("CREATE INDEX IF NOT EXISTS" in s for s in sql)
    assert any('ANALYZE public."Voter"' in s for s in sql)
    assert manifest.status == "complete"
    assert manifest.analyzed_tables == ["Voter"]
    assert "Voter_pkey" in manifest.constraints_added
    assert {i.index_name for i in manifest.indexes} == {"Voter_LALVOTERID_key", "Voter_Active_idx"}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_build_indexes.py -v`
Expected: FAIL — stub raises `NotImplementedError`; `_rewrite_index_sql` undefined.

- [ ] **Step 3: Replace the stub**

Overwrite `src/loader/people_api/steps/build_indexes.py`:

```python
"""Step 5 — build the PK + indexes on the unified Voter table, then ANALYZE (DATA-1853).

Parses the PK, the LALVOTERID unique, and the plain indexes from the committed
prod snapshot and applies them to public."Voter" in parallel (concurrent
CREATE INDEX on one table run in separate sessions). `CREATE INDEX` (not
CONCURRENTLY) since the cluster is idle. No FKs exist in this schema.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, resolve_writer_endpoint
from loader.people_api.manifests import (
    IndexManifest,
    IndexSpec,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.index_specs import (
    IndexDef,
    PrimaryKey,
    parse_indexes,
    parse_primary_keys,
)
from loader.people_api.schema.snapshot import load_prod_dump

log = get_logger(__name__)

_INDEX_BUILDERS = 32
_TARGET_TABLE = "Voter"

_BUILD_SESSION_SQL: tuple[str, ...] = (
    "SET maintenance_work_mem = '8GB'",
    "SET max_parallel_maintenance_workers = 8",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _apply_session(cur: psycopg.Cursor) -> None:
    for stmt in _BUILD_SESSION_SQL:
        cur.execute(stmt)  # ty: ignore[no-matching-overload]


def _rewrite_index_sql(sql: str) -> str:
    """Inject IF NOT EXISTS so reruns are idempotent."""
    if "CREATE INDEX " in sql and "IF NOT EXISTS" not in sql.upper():
        sql = sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
    if "CREATE UNIQUE INDEX " in sql and "IF NOT EXISTS" not in sql.upper():
        sql = sql.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
    return sql


def _add_primary_key(cfg: LoaderConfig, run_date: str, writer_endpoint: str, pk: PrimaryKey) -> None:
    cols = ", ".join(f'"{c}"' for c in pk.columns)
    sql = f'ALTER TABLE public."{pk.table}" ADD CONSTRAINT "{pk.constraint}" PRIMARY KEY ({cols})'
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        try:
            cur.execute(sql)  # ty: ignore[no-matching-overload]
            log.info("indexes.pk_added", table=pk.table, constraint=pk.constraint)
        except (psycopg.errors.DuplicateObject, psycopg.errors.InvalidTableDefinition):
            log.info("indexes.pk_exists", table=pk.table, constraint=pk.constraint)


def _create_index(cfg: LoaderConfig, run_date: str, writer_endpoint: str, idx: IndexDef) -> None:
    sql = _rewrite_index_sql(idx.sql)
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        log.info("indexes.built", table=idx.table, name=idx.name, unique=idx.unique)


def _analyze(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute('ANALYZE public."Voter"')
        log.info("indexes.analyzed", table=_TARGET_TABLE)


def _l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> list[str]:
    """l2Type values in prod org_districts not present as columns on Voter."""
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts lives in the app DB, not always reachable
        log.warning("indexes.l2type.skip", error=str(e))
        return []

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='Voter'"
        )
        new_cols = {r[0] for r in cur.fetchall()}
    return sorted(v for v in distinct_l2types if v not in new_cols)


def run(cfg: LoaderConfig, run_date: str) -> IndexManifest:
    bind(run_date=run_date, step="indexes")
    existing = read_manifest(cfg, run_date, "indexes", IndexManifest)
    if existing and existing.status == "complete":
        log.info("indexes.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "indexes"))
        return existing

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("indexes.start")

    dump = load_prod_dump(cfg, run_date)
    pks = [p for p in parse_primary_keys(dump) if p.table == _TARGET_TABLE]
    idxs = [i for i in parse_indexes(dump) if i.table == _TARGET_TABLE]
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs))

    # 1. Primary key(s).
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        for fut in as_completed(
            [executor.submit(_add_primary_key, cfg, run_date, writer_endpoint, pk) for pk in pks]
        ):
            fut.result()

    # 2. Indexes (unique + plain), parallel on the single table.
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        for fut in as_completed(
            [executor.submit(_create_index, cfg, run_date, writer_endpoint, idx) for idx in idxs]
        ):
            fut.result()

    # 3. ANALYZE.
    _analyze(cfg, run_date, writer_endpoint)

    # 4. l2Type coverage.
    missing = _l2type_coverage(cfg, run_date, writer_endpoint)
    if missing:
        log.warning("indexes.l2type.missing", count=len(missing), examples=missing[:5])
    else:
        log.info("indexes.l2type.coverage_complete")

    manifest = IndexManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        indexes=[
            IndexSpec(table=i.table, index_name=i.name, columns=i.columns, unique=i.unique, where=i.where)
            for i in idxs
        ],
        constraints_added=[p.constraint for p in pks],
        analyzed_tables=[_TARGET_TABLE],
        l2type_coverage_missing=missing,
    )
    uri = write_manifest(cfg, manifest)
    log.info("indexes.complete", uri=uri, indexes=len(idxs), pks=len(pks))
    return manifest
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_build_indexes.py -v` then `uv run ruff check . && uv run ty check`
Expected: PASS; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/steps/build_indexes.py people-api-loader/tests/test_build_indexes.py
git commit -m "feat(DATA-1853): implement build-indexes for unified Voter table"
```

---

## Task 7: `validate` step (DATA-1911)

**Files:**
- Modify (replace stub): `src/loader/people_api/steps/validate.py`
- Test: `tests/test_validate.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_validate.py`:

```python
"""validate: per-state GROUP BY counts at ±10%, five checks, markdown."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ValidateManifest
from loader.people_api.steps import validate as step
from tests._fakes import FakeConn, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))


def test_row_counts_within_tolerance_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 105)])  # 105 vs expected 100 → within ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100}).passed is True


def test_row_counts_outside_tolerance_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 50)])  # 50 vs 100 → outside ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is False
    assert check.details["mismatch_count"] == 1


def test_row_counts_expected_zero_requires_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([])))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 0}).passed is True
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("TX", 1)])))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 0}).passed is False


def test_run_aggregates_and_writes_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    unload = SimpleNamespace(status="complete", per_state_row_counts={"TX": 100})
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: captured.setdefault("md", body) or "uri")
    ok = step.ValidationCheck(name="x", passed=True, details={})
    bad = step.ValidationCheck(name="y", passed=False, details={})
    monkeypatch.setattr(step, "_check_row_counts", lambda *a: ok)
    monkeypatch.setattr(step, "_check_schema_diff", lambda *a: ok)
    monkeypatch.setattr(step, "_check_indexes", lambda *a: bad)
    monkeypatch.setattr(step, "_check_sample_queries", lambda *a: ok)
    monkeypatch.setattr(step, "_check_l2type_coverage", lambda *a: ok)
    manifest = step.run(_CFG, "20260609")
    assert isinstance(manifest, ValidateManifest)
    assert manifest.all_passed is False
    assert "Validation" in captured["md"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_validate.py -v`
Expected: FAIL — stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub**

Overwrite `src/loader/people_api/steps/validate.py`:

```python
"""Step 7 — validate the new cluster before handoff (ClickUp DATA-1911).

Five checks against the unified public."Voter" table (all must pass):
1. row_counts_match_databricks — per-state count (GROUP BY "State") within ±10%.
2. schema_diff_clean — new Voter columns equal prod Voter columns.
3. index_constraint_diff_clean — every prod index present on new.
4. sample_queries_pass — voterFile.util.ts-shaped queries return without error.
5. l2Type_coverage — every distinct org_districts l2Type maps to a column.

Writes validate.json + a Markdown companion. all_passed=False blocks handoff
(cli.py exits non-zero).
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, resolve_writer_endpoint
from loader.people_api.manifests import (
    UnloadManifest,
    ValidateManifest,
    ValidationCheck,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

# Per-state row-count gate: within ±10% of the unload baseline (decided 2026-06-09).
_ROW_COUNT_TOLERANCE = 0.10


def _check_row_counts(
    cfg: LoaderConfig, run_date: str, writer_endpoint: str, expected: dict[str, int]
) -> ValidationCheck:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute('SELECT "State", count(*) FROM public."Voter" GROUP BY "State"')
        actual_by_state = {row[0]: int(row[1]) for row in cur.fetchall()}

    mismatches: dict[str, dict[str, int]] = {}
    for state, expected_count in expected.items():
        actual = actual_by_state.get(state, 0)
        low = expected_count * (1 - _ROW_COUNT_TOLERANCE)
        high = expected_count * (1 + _ROW_COUNT_TOLERANCE)
        if not (low <= actual <= high):
            mismatches[state] = {"expected": expected_count, "actual": actual}
    return ValidationCheck(
        name="row_counts_match_databricks",
        passed=not mismatches,
        details={
            "states": len(expected),
            "tolerance": _ROW_COUNT_TOLERANCE,
            "mismatch_count": len(mismatches),
            "mismatches": dict(list(mismatches.items())[:10]),
        },
    )


def _voter_columns(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='Voter'"
        )
        return {r[0] for r in cur.fetchall()}


def _check_schema_diff(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    with connect_prod(cfg) as prod_conn:
        prod_cols = _voter_columns(prod_conn)
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        new_cols = _voter_columns(conn)
    missing_from_new = prod_cols - new_cols
    extra_in_new = new_cols - prod_cols
    return ValidationCheck(
        name="schema_diff_clean",
        passed=not missing_from_new and not extra_in_new,
        details={
            "prod_cols": len(prod_cols),
            "new_cols": len(new_cols),
            "missing_from_new": sorted(missing_from_new)[:20],
            "extra_in_new": sorted(extra_in_new)[:20],
        },
    )


def _check_indexes(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    query = (
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname='public' AND tablename='Voter' ORDER BY indexname"
    )
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute(query)
            prod_idx = {r[0] for r in cur.fetchall()}
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        return ValidationCheck(name="index_constraint_diff_clean", passed=False, details={"error_reading_prod": str(e)})
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(query)
        new_idx = {r[0] for r in cur.fetchall()}
    missing = sorted(prod_idx - new_idx)
    return ValidationCheck(
        name="index_constraint_diff_clean",
        passed=not missing,
        details={"prod_count": len(prod_idx), "new_count": len(new_idx), "missing_from_new": missing[:20]},
    )


_SAMPLE_QUERIES: tuple[tuple[str, str], ...] = (
    ("party_filter", 'SELECT count(*) FROM public."Voter" WHERE "Parties_Description" = \'Democratic\''),
    ("age_cast_filter", 'SELECT count(*) FROM public."Voter" WHERE "Age"::integer BETWEEN 18 AND 35'),
    ("state_filter", 'SELECT count(*) FROM public."Voter" WHERE "State" = \'TX\''),
    ("lalvoterid_lookup", 'SELECT count(*) FROM public."Voter" WHERE "LALVOTERID" IS NOT NULL'),
)


def _check_sample_queries(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    results: dict[str, str] = {}
    failures: dict[str, str] = {}
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        for label, sql in _SAMPLE_QUERIES:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)  # ty: ignore[no-matching-overload]
                    row = cur.fetchone()
                    results[label] = "ok" if row is not None else "empty"
            except Exception as e:  # broad by design: query failures are captured as check results
                failures[label] = str(e)
    return ValidationCheck(name="sample_queries_pass", passed=not failures, details={"pass": results, "fail": failures})


def _check_l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts is in the app DB, not always reachable
        return ValidationCheck(name="l2Type_coverage", passed=False, details={"error_reading_org_districts": str(e)})
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        new_cols = _voter_columns(conn)
    missing = sorted(v for v in distinct_l2types if v not in new_cols)
    return ValidationCheck(
        name="l2Type_coverage",
        passed=not missing,
        details={"distinct_l2types": len(distinct_l2types), "missing_columns": missing},
    )


def _to_markdown(manifest: ValidateManifest) -> str:
    lines: list[str] = [
        f"# Voter-DB Refresh Validation — {manifest.run_date}",
        "",
        f"**Status:** {'PASS' if manifest.all_passed else 'FAIL'}",
        f"**Started:** {manifest.started_at.isoformat()}",
        f"**Finished:** {manifest.finished_at.isoformat() if manifest.finished_at else '—'}",
        "",
        "## Checks",
        "",
    ]
    for c in manifest.checks:
        lines.append(f"### {c.name} — {'PASS' if c.passed else 'FAIL'}")
        lines.append("")
        for k, v in c.details.items():
            pretty = [*v[:10], "..."] if isinstance(v, list) and len(v) > 10 else v
            lines.append(f"- **{k}:** `{pretty}`")
        lines.append("")
    return "\n".join(lines)


def run(cfg: LoaderConfig, run_date: str) -> ValidateManifest:
    bind(run_date=run_date, step="validate")
    existing = read_manifest(cfg, run_date, "validate", ValidateManifest)
    if existing and existing.status == "complete":
        log.info("validate.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "validate"))
        return existing

    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if unload is None or unload.status != "complete":
        raise RuntimeError("Step 7 requires a completed unload manifest (per-state baseline).")

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("validate.start")

    checks: list[ValidationCheck] = [
        _check_row_counts(cfg, run_date, writer_endpoint, unload.per_state_row_counts),
        _check_schema_diff(cfg, run_date, writer_endpoint),
        _check_indexes(cfg, run_date, writer_endpoint),
        _check_sample_queries(cfg, run_date, writer_endpoint),
        _check_l2type_coverage(cfg, run_date, writer_endpoint),
    ]
    for c in checks:
        log.info("validate.check", name=c.name, passed=c.passed)

    all_passed = all(c.passed for c in checks)
    manifest = ValidateManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        checks=checks,
        all_passed=all_passed,
    )
    md_uri = put_artifact(cfg, run_date, "_manifest/validate.md", _to_markdown(manifest))
    log.info("validate.markdown", uri=md_uri)
    uri = write_manifest(cfg, manifest)
    log.info("validate.complete", uri=uri, all_passed=all_passed)
    return manifest
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_validate.py -v` then `uv run ruff check . && uv run ty check`
Expected: PASS; clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add people-api-loader/src/loader/people_api/steps/validate.py people-api-loader/tests/test_validate.py
git commit -m "feat(DATA-1911): implement validate for unified Voter table (±10%)"
```

---

## Task 8: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Whole suite + lint + types**

Run from `people-api-loader/`:
```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check . && uv run ty check
```
Expected: all green.

- [ ] **Step 2: CLI smoke + offline dry-run**

```bash
uv run pytest tests/test_cli_smoke.py -v
env -u LOADER_NEW_WRITER_ENDPOINT -u AWS_PROFILE AWS_REGION=us-west-2 uv run loader create-schema --date 20260609 || echo "expected clean failure (AWS caller check or writer endpoint)"
```
Expected: smoke passes; dry-run fails cleanly (AWS caller check or resolver error), not `NotImplementedError`.

- [ ] **Step 3: End-to-end offline schema/index parse against the real snapshot**

```bash
uv run python -c "
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.table_ddl import extract_create_tables
from loader.people_api.schema.index_specs import parse_indexes, parse_primary_keys
d = load_prod_dump(None, '0')
t = extract_create_tables(d)
idx = [i for i in parse_indexes(d) if i.table == 'Voter']
pk = [p for p in parse_primary_keys(d) if p.table == 'Voter']
print('tables:', sorted(t), '| indexes:', len(idx), '| pks:', len(pk))
assert list(t) == ['Voter'] and len(idx) >= 200 and len(pk) == 1
"
```
Expected: `tables: ['Voter'] | indexes: 266 | pks: 1` (or close), no assertion error.

- [ ] **Step 4: Commit if formatting changed**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git add -A && git commit -m "chore(DATA-1640): full-suite verification" || echo "nothing to commit"
```

---

## Out of scope

`inspect-prod` (1907), `unload` (1908), `provision` (1909), `resize` (1854), `status`/`teardown` (1912); the Airflow DAG; production cutover. The `unload` export must provide `id`, `"State"`, and `updated_at` in table column order (documented dependency).
