# People-API Loader — Postgres Data-Plane Steps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the four Postgres data-plane subcommands of the people-API loader — `create-schema`, `copy`, `build-indexes`, `validate` — in `gp-data-platform/people-api-loader`, adapting the proven POC implementations into the monorepo's contracts.

**Architecture:** Follow the repo's `docs/PLAN_LOADER.md` design (NOT the ClickUp tickets' newer partitioned/Prisma model). The data model is **51 standalone `public."Voter{ST}"` tables + `public."VoterFile"`** (no native partitioning). DDL is produced by `emit_ddl`, which merges a prod `pg_dump --schema-only` snapshot with the curated `voter_columns.VOTER_TARGET_COLUMNS` list. Indexes/PKs/FKs (~253/state) are parsed from the same prod dump by `index_specs` and built after COPY. The four steps share the existing `core`/`people_api` infrastructure (manifests, db helpers, structured logging) which is already built out; the step bodies are currently `NotImplementedError` stubs.

Because the upstream steps `inspect-prod` (DATA-1907), `provision` (DATA-1909), and `unload` (DATA-1908) are **out of scope** for this PR, we decouple via:
- a **committed static snapshot** (`prod_dump.sql` + `databricks_columns.json`) that replaces `inspect-prod`'s S3 artifacts as the DDL/index source;
- a **writer-endpoint resolver** (env override → provision manifest → error) that replaces the direct `provision.writer_endpoint` read;
- continued use of the `unload` manifest as the row-count/file-listing contract for `copy`/`validate` (stubbed in tests; required at real runtime).

**Validate row-count gate:** ±10% per-state envelope vs the unload baseline (named tolerance constant, not exact match).

**Tech Stack:** Python 3.12, uv, Typer, psycopg 3, boto3, pydantic v2, structlog. Tests: pytest + `monkeypatch` with `SimpleNamespace`/fake context managers (no moto, no live DB).

**Reference sources (already cloned locally):**
- POC: `/Users/hugh/Documents/repos_4/poc-voterfile-loader` (code on `main` branch)
- Target repo: `/Users/hugh/Documents/repos_4/gp-data-platform/people-api-loader`
- All paths below are relative to the target repo unless absolute.

---

## File Structure

**Created:**
- `src/loader/people_api/schema/__init__.py` — vendored from POC
- `src/loader/people_api/schema/voter_columns.py` — vendored; curated column source of truth (pure data)
- `src/loader/people_api/schema/index_specs.py` — vendored; pg_dump index/PK/FK parsers (pure)
- `src/loader/people_api/schema/emit_ddl.py` — vendored; DDL emitter (one import-path edit)
- `src/loader/people_api/schema/snapshot.py` — NEW; loads committed prod-dump + databricks-columns snapshot
- `src/loader/people_api/schema/data/prod_dump.sql` — NEW; committed static snapshot (captured from prod)
- `src/loader/people_api/schema/data/databricks_columns.json` — NEW; committed static snapshot
- `tests/_fakes.py` — NEW; shared fake psycopg connection/cursor recorder
- `tests/test_emit_ddl.py`, `tests/test_index_specs.py`, `tests/test_snapshot.py`
- `tests/test_resolve_writer_endpoint.py`
- `tests/test_create_schema.py`, `tests/test_copy_s3.py`, `tests/test_build_indexes.py`, `tests/test_validate.py`

**Modified:**
- `src/loader/people_api/db.py` — add `resolve_writer_endpoint(...)`
- `src/loader/people_api/steps/create_schema.py` — replace stub
- `src/loader/people_api/steps/copy_s3.py` — replace stub
- `src/loader/people_api/steps/build_indexes.py` — replace stub
- `src/loader/people_api/steps/validate.py` — replace stub

**Unchanged (already correct):** `people_api/cli.py` (lazily imports and calls `step.run(...)`), `people_api/manifests.py` (all four manifest models + re-exported IO helpers exist), `people_api/config.py`, all of `core/`.

---

## Conventions for every task

- Work in repo `/Users/hugh/Documents/repos_4/gp-data-platform/people-api-loader`.
- Run tests with `uv run pytest <path> -v` (the repo uses `uv`; `pyproject.toml` sets `testpaths=["tests"]`).
- Lint/type after each step: `uv run ruff check . && uv run ruff format --check . && uv run ty check`.
- Commit messages: `feat(DATA-XXXX): <summary>` referencing the ticket for that step.
- `STATES` (51 entries) is defined in `loader.people_api.schema.emit_ddl` after Task 2; import it from there everywhere.

---

## Task 0: Feature branch

**Files:** none (git only)

- [ ] **Step 1: Create the branch**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform
git checkout main && git pull --ff-only
git checkout -b feat/DATA-1851-pg-data-plane-steps
```

- [ ] **Step 2: Confirm baseline is green**

Run: `cd people-api-loader && uv run pytest -q && uv run ruff check .`
Expected: existing tests pass (test_cli_smoke, test_config, test_db, test_read_manifest, test_status_command), ruff clean.

---

## Task 1: Vendor the schema package (pure modules)

**Files:**
- Create: `src/loader/people_api/schema/__init__.py`
- Create: `src/loader/people_api/schema/voter_columns.py`
- Create: `src/loader/people_api/schema/index_specs.py`
- Create: `src/loader/people_api/schema/emit_ddl.py`
- Test: `tests/test_index_specs.py`, `tests/test_emit_ddl.py`

These three modules are pure (no DB/AWS). `voter_columns.py` and `index_specs.py` have **no `loader.*` imports** and are copied verbatim. `emit_ddl.py` imports `voter_columns` and needs one import-path edit.

- [ ] **Step 1: Copy the four files verbatim**

```bash
cd /Users/hugh/Documents/repos_4/gp-data-platform/people-api-loader
mkdir -p src/loader/people_api/schema
cp /Users/hugh/Documents/repos_4/poc-voterfile-loader/src/loader/schema/__init__.py        src/loader/people_api/schema/__init__.py
cp /Users/hugh/Documents/repos_4/poc-voterfile-loader/src/loader/schema/voter_columns.py    src/loader/people_api/schema/voter_columns.py
cp /Users/hugh/Documents/repos_4/poc-voterfile-loader/src/loader/schema/index_specs.py      src/loader/people_api/schema/index_specs.py
cp /Users/hugh/Documents/repos_4/poc-voterfile-loader/src/loader/schema/emit_ddl.py         src/loader/people_api/schema/emit_ddl.py
```

- [ ] **Step 2: Fix the one import path in `emit_ddl.py`**

The only `loader.*` import in these files is in `emit_ddl.py`. Change:

```python
from loader.schema.voter_columns import (
    LEGACY_RENAMES,
    VOTER_TARGET_COLUMNS,
    TargetColumn,
)
```

to:

```python
from loader.people_api.schema.voter_columns import (
    LEGACY_RENAMES,
    VOTER_TARGET_COLUMNS,
    TargetColumn,
)
```

Then verify no other stale import paths remain:

Run: `grep -rn "from loader.schema" src/loader/people_api/schema/ ; grep -rn "loader.schema" src/loader/people_api/schema/`
Expected: no output (all references now use `loader.people_api.schema`).

- [ ] **Step 3: Write failing tests for the parsers + emitter**

Create `tests/test_index_specs.py`:

```python
"""Parser tests for index_specs — pure regex over a fixture pg_dump."""

from __future__ import annotations

from loader.people_api.schema.index_specs import (
    parse_foreign_keys,
    parse_indexes,
    parse_primary_keys,
)

_FIXTURE = '''
ALTER TABLE ONLY public."VoterTX"
    ADD CONSTRAINT "VoterTX_pkey" PRIMARY KEY ("LALVOTERID");

CREATE INDEX "VoterTX_Voters_LastName_idx" ON public."VoterTX" USING btree ("Voters_LastName");

CREATE UNIQUE INDEX "VoterTX_uniq_idx" ON public."VoterTX" USING btree ("LALVOTERID");

CREATE INDEX "VoterTX_family_idx" ON public."VoterTX" USING btree ("Mailing_Families_FamilyID")
    WHERE ("Mailing_Families_FamilyID" IS NOT NULL);

ALTER TABLE ONLY public."DistrictVoterTX"
    ADD CONSTRAINT "dv_fk" FOREIGN KEY ("voterId") REFERENCES public."VoterTX"("LALVOTERID");
'''


def test_parse_primary_keys() -> None:
    pks = parse_primary_keys(_FIXTURE)
    assert len(pks) == 1
    assert pks[0].table == "VoterTX"
    assert pks[0].constraint == "VoterTX_pkey"
    assert pks[0].columns == ["LALVOTERID"]


def test_parse_indexes_unique_and_where() -> None:
    idxs = {i.name: i for i in parse_indexes(_FIXTURE)}
    assert idxs["VoterTX_Voters_LastName_idx"].unique is False
    assert idxs["VoterTX_Voters_LastName_idx"].columns == ["Voters_LastName"]
    assert idxs["VoterTX_uniq_idx"].unique is True
    assert idxs["VoterTX_family_idx"].where is not None
    assert "IS NOT NULL" in idxs["VoterTX_family_idx"].where


def test_parse_foreign_keys() -> None:
    fks = parse_foreign_keys(_FIXTURE)
    assert len(fks) == 1
    assert fks[0].table == "DistrictVoterTX"
    assert fks[0].constraint == "dv_fk"
    assert fks[0].sql.strip().endswith(";")
```

Create `tests/test_emit_ddl.py`:

```python
"""emit_ddl produces CREATE TABLE per state with no indexes/PKs."""

from __future__ import annotations

from loader.people_api.schema.emit_ddl import STATES, emit_target_schema

_PROD_DUMP = '''
CREATE TABLE public."VoterTX" (
    "LALVOTERID" text,
    "Voters_FirstName" text,
    "Voters_Age" text
);
CREATE TABLE public."VoterFile" (
    "Filename" text,
    "State" text,
    "Lines" integer,
    "Loaded" timestamp,
    "updatedAt" timestamp
);
'''


def test_states_count() -> None:
    assert len(STATES) == 51
    assert "TX" in STATES and "DC" in STATES


def test_emit_target_schema_shape() -> None:
    ddl = emit_target_schema(_PROD_DUMP, databricks_columns={})
    # One CREATE TABLE per state + VoterFile, wrapped in a transaction.
    assert ddl.count('CREATE TABLE public."Voter') == len(STATES)  # 51 Voter{ST}
    assert 'CREATE TABLE public."VoterFile"' in ddl
    assert "BEGIN;" in ddl and "COMMIT;" in ddl
    # No indexes / PKs in the emitted schema.
    assert "CREATE INDEX" not in ddl.upper()
    assert "PRIMARY KEY" not in ddl.upper()
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_index_specs.py tests/test_emit_ddl.py -v`
Expected: collection succeeds, tests FAIL only if the vendored files are missing/mis-imported. If Steps 1–2 were done correctly they should PASS immediately (these are vendored, working modules). If they fail, fix the import path / copy. (This task vendors known-good code; the tests pin the contract we depend on.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_index_specs.py tests/test_emit_ddl.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/loader/people_api/schema tests/test_index_specs.py tests/test_emit_ddl.py
git commit -m "feat(DATA-1910): vendor schema package (voter_columns, emit_ddl, index_specs)"
```

---

## Task 2: Writer-endpoint resolver

**Files:**
- Modify: `src/loader/people_api/db.py`
- Test: `tests/test_resolve_writer_endpoint.py`

`provision` is out of scope, so the four steps can't read `provision.writer_endpoint` directly. Add a resolver: env override → completed provision manifest (if it happens to exist) → clear error.

- [ ] **Step 1: Write the failing test**

Create `tests/test_resolve_writer_endpoint.py`:

```python
"""resolve_writer_endpoint: env override > provision manifest > error."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.db import resolve_writer_endpoint

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_NEW_WRITER_ENDPOINT", "host-from-env")
    assert resolve_writer_endpoint(_CFG, "20260609") == "host-from-env"


def test_falls_back_to_provision_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_NEW_WRITER_ENDPOINT", raising=False)
    fake = SimpleNamespace(status="complete", writer_endpoint="host-from-manifest")
    monkeypatch.setattr(
        "loader.people_api.db.read_manifest",
        lambda cfg, run_date, step, model: fake,
    )
    assert resolve_writer_endpoint(_CFG, "20260609") == "host-from-manifest"


def test_raises_when_nothing_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_NEW_WRITER_ENDPOINT", raising=False)
    monkeypatch.setattr(
        "loader.people_api.db.read_manifest",
        lambda cfg, run_date, step, model: None,
    )
    with pytest.raises(RuntimeError, match="LOADER_NEW_WRITER_ENDPOINT"):
        resolve_writer_endpoint(_CFG, "20260609")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_resolve_writer_endpoint.py -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_writer_endpoint'`.

- [ ] **Step 3: Implement the resolver**

Append to `src/loader/people_api/db.py` (add `import os` at top with the other stdlib imports, and the `read_manifest`/`ProvisionManifest` import at module level so tests can monkeypatch `loader.people_api.db.read_manifest`):

```python
import os

from loader.people_api.manifests import ProvisionManifest, read_manifest


def resolve_writer_endpoint(cfg: LoaderConfig, run_date: str) -> str:
    """Resolve the new cluster's writer endpoint.

    `provision` (DATA-1909) is out of scope for this PR, so this step never
    creates the cluster itself. Resolution order:

    1. `LOADER_NEW_WRITER_ENDPOINT` env override (point at an existing cluster).
    2. A completed `provision` manifest, if one happens to exist for this run.
    3. Otherwise raise — there is nothing to connect to.
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

> Note: `people_api/db.py` already imports `psycopg`, `LoaderConfig`, and the `core.db` helpers. Adding a top-level `from loader.people_api.manifests import ...` is safe — `manifests.py` imports only `core.manifest.*` and pydantic, so there is no import cycle with `db.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_resolve_writer_endpoint.py -v`
Expected: PASS (all three cases).

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/db.py tests/test_resolve_writer_endpoint.py
git commit -m "feat(DATA-1851): add resolve_writer_endpoint (provision out of scope)"
```

---

## Task 3: Static-snapshot source module

**Files:**
- Create: `src/loader/people_api/schema/snapshot.py`
- Create (placeholder for now): `src/loader/people_api/schema/data/prod_dump.sql`, `src/loader/people_api/schema/data/databricks_columns.json`
- Test: `tests/test_snapshot.py`

`create-schema` and `build-indexes` get the prod DDL/index source from a committed snapshot instead of `inspect-prod`'s S3 artifacts. The real snapshot content is captured in Task 4; this task builds the loader + tests against fixtures.

- [ ] **Step 1: Write the failing test**

Create `tests/test_snapshot.py`:

```python
"""snapshot loaders read committed files, honoring env path overrides."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema import snapshot

_CFG = cast(LoaderConfig, SimpleNamespace())


def test_load_prod_dump_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "dump.sql"
    p.write_text('CREATE TABLE public."VoterTX" ("LALVOTERID" text);', encoding="utf-8")
    monkeypatch.setenv("LOADER_PROD_DUMP_PATH", str(p))
    assert "VoterTX" in snapshot.load_prod_dump(_CFG, "20260609")


def test_load_databricks_columns_list_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "cols.json"
    p.write_text(json.dumps([{"name": "Voters_Age", "type": "string"}]), encoding="utf-8")
    monkeypatch.setenv("LOADER_DATABRICKS_COLUMNS_PATH", str(p))
    cols = snapshot.load_databricks_columns(_CFG, "20260609")
    assert cols == {"Voters_Age": "string"}


def test_load_databricks_columns_dict_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "cols.json"
    p.write_text(json.dumps({"Voters_Age": "string"}), encoding="utf-8")
    monkeypatch.setenv("LOADER_DATABRICKS_COLUMNS_PATH", str(p))
    assert snapshot.load_databricks_columns(_CFG, "20260609") == {"Voters_Age": "string"}


def test_committed_snapshot_files_exist() -> None:
    # Task 4 must populate these with real prod data before merge.
    assert (snapshot.DATA_DIR / "prod_dump.sql").exists()
    assert (snapshot.DATA_DIR / "databricks_columns.json").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_snapshot.py -v`
Expected: FAIL with `ModuleNotFoundError: loader.people_api.schema.snapshot`.

- [ ] **Step 3: Implement `snapshot.py` and seed placeholder data files**

Create `src/loader/people_api/schema/snapshot.py`:

```python
"""Committed static snapshot of the prod schema + Databricks column list.

`inspect-prod` (DATA-1907) is out of scope for this PR, so `create-schema`
and `build-indexes` read their prod-dump / databricks-columns inputs from a
versioned snapshot committed under `schema/data/` instead of from S3.

Override either input with an env var pointing at a file — used by tests and
by ad-hoc runs against a freshly captured dump:
- `LOADER_PROD_DUMP_PATH`
- `LOADER_DATABRICKS_COLUMNS_PATH`

When `inspect-prod` later lands, it can repopulate these files (or this
module can be extended to prefer a present inspect manifest's S3 artifacts).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from loader.people_api.config import LoaderConfig

DATA_DIR = Path(__file__).parent / "data"


def load_prod_dump(cfg: LoaderConfig, run_date: str) -> str:
    del cfg, run_date  # reserved for a future inspect-manifest path
    override = os.environ.get("LOADER_PROD_DUMP_PATH")
    path = Path(override) if override else DATA_DIR / "prod_dump.sql"
    return path.read_text(encoding="utf-8")


def load_databricks_columns(cfg: LoaderConfig, run_date: str) -> dict[str, str]:
    del cfg, run_date
    override = os.environ.get("LOADER_DATABRICKS_COLUMNS_PATH")
    path = Path(override) if override else DATA_DIR / "databricks_columns.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return {c["name"]: c.get("type", "") for c in raw}
    return raw
```

Seed placeholder data files so the package imports and `test_committed_snapshot_files_exist` passes; Task 4 overwrites them with real content:

```bash
mkdir -p src/loader/people_api/schema/data
printf -- '-- PLACEHOLDER prod schema dump. Replace via Task 4 before merge.\n' > src/loader/people_api/schema/data/prod_dump.sql
printf '{}\n' > src/loader/people_api/schema/data/databricks_columns.json
```

Ensure these data files ship in the wheel — confirm `pyproject.toml`'s `[tool.hatch.build.targets.wheel]` includes `packages = ["src/loader"]` (it does), so package data under `src/loader/...` is included.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_snapshot.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/schema/snapshot.py src/loader/people_api/schema/data tests/test_snapshot.py
git commit -m "feat(DATA-1910): static snapshot source loader + placeholder data files"
```

---

## Task 4: Capture the real static snapshot (DATA / manual)

**Files:**
- Overwrite: `src/loader/people_api/schema/data/prod_dump.sql`
- Overwrite: `src/loader/people_api/schema/data/databricks_columns.json`

This is a data-capture task, not code. It requires prod read access (pg_service `voters`) and Databricks CLI access. **You run these commands** (use the `! <cmd>` prompt prefix for the interactive ones).

- [ ] **Step 1: Capture the prod schema dump**

Use the corrected quoted-wildcard pattern (`public."Voter"*`, not `public."Voter*"` — the latter matches nothing; see PLAN_LOADER Test-Run-Log bug #7a):

```bash
pg_dump "service=voters" --schema-only --no-owner --no-acl --schema=public \
  -t 'public."Voter"*' -t 'public."VoterFile"' \
  > src/loader/people_api/schema/data/prod_dump.sql
```

Sanity-check it contains the expected shapes:

Run: `grep -c 'CREATE TABLE public."Voter' src/loader/people_api/schema/data/prod_dump.sql; grep -c 'CREATE INDEX' src/loader/people_api/schema/data/prod_dump.sql`
Expected: ~51 CREATE TABLE lines; a few thousand CREATE INDEX lines (~253/state).

- [ ] **Step 2: Capture the Databricks source columns**

```bash
databricks tables get goodparty_data_catalog.dbt.int__l2_nationwide_uniform \
  | jq '[.columns[] | {name: .name, type: .type_text}]' \
  > src/loader/people_api/schema/data/databricks_columns.json
```

Run: `jq 'length' src/loader/people_api/schema/data/databricks_columns.json`
Expected: a few hundred columns.

- [ ] **Step 3: Verify the real snapshot drives the emitter end-to-end (offline)**

Run:
```bash
uv run python -c "
from loader.people_api.schema.snapshot import load_prod_dump, load_databricks_columns
from loader.people_api.schema.emit_ddl import emit_target_schema, STATES
from loader.people_api.schema.index_specs import parse_indexes, parse_primary_keys
d = load_prod_dump(None, '0'); c = load_databricks_columns(None, '0')
ddl = emit_target_schema(d, c)
print('voter tables:', ddl.count('CREATE TABLE public.\"Voter'))
print('indexes parsed:', len(parse_indexes(d)), 'pks:', len(parse_primary_keys(d)))
assert ddl.count('CREATE TABLE public.\"Voter') == len(STATES)
"
```
Expected: voter tables == 51, indexes parsed in the thousands, pks ~51. No exceptions.

- [ ] **Step 4: Commit the real snapshot**

```bash
git add src/loader/people_api/schema/data
git commit -m "data(DATA-1910): commit prod schema + databricks column snapshot"
```

> If prod/Databricks access is not available at plan-execution time, leave the placeholders, mark this task blocked, and proceed — the code tasks below use fixtures and do not depend on the real snapshot. The snapshot must be real before the PR merges.

---

## Task 5: Shared test fakes

**Files:**
- Create: `tests/_fakes.py`

The four step tests all need a fake psycopg connection that records executed SQL and returns canned rows, usable as both a context manager and a `.cursor()` context manager.

- [ ] **Step 1: Create the fakes module**

Create `tests/_fakes.py`:

```python
"""Fake psycopg connection/cursor for step unit tests.

Records every executed statement and serves canned results queued via
`queue_result`. Works as a context manager (the `with connect_new(...) as
conn` form) and yields cursors that are themselves context managers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((" ".join(sql.split()), params))

    def fetchone(self) -> Any:
        return self._conn.next_result()

    def fetchall(self) -> Any:
        res = self._conn.next_result()
        return res if res is not None else []


class FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self._results: list[Any] = []

    def queue_result(self, value: Any) -> "FakeConn":
        """Queue a value to be returned by the next fetchone/fetchall."""
        self._results.append(value)
        return self

    def next_result(self) -> Any:
        return self._results.pop(0) if self._results else None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def __enter__(self) -> "FakeConn":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def fake_connect(conn: FakeConn):
    """Return a connect_* replacement that always yields `conn`."""

    @contextmanager
    def _connect(*args: object, **kwargs: object) -> Iterator[FakeConn]:
        yield conn

    return _connect


def executed_sql(conn: FakeConn) -> list[str]:
    return [sql for sql, _ in conn.executed]
```

- [ ] **Step 2: Sanity-check it imports**

Run: `uv run python -c "from tests._fakes import FakeConn, fake_connect; c=FakeConn().queue_result((5,)); print(c.cursor().fetchone()); print('ok')"`
Expected: prints `(5,)` then `ok`.

- [ ] **Step 3: Commit**

```bash
git add tests/_fakes.py
git commit -m "test: shared fake psycopg connection recorder"
```

---

## Task 6: `create-schema` step (DATA-1910)

**Files:**
- Modify: `src/loader/people_api/steps/create_schema.py` (replace stub)
- Test: `tests/test_create_schema.py`

Adapted from POC `steps/create_schema.py`. Deltas: monorepo import paths; `resolve_writer_endpoint` instead of `provision.writer_endpoint`; `snapshot.load_*` instead of inspect-manifest S3 reads; no inspect/provision hard gating; `unload` stays optional for column verification.

- [ ] **Step 1: Write the failing test**

Create `tests/test_create_schema.py`:

```python
"""create-schema: emits DDL, installs extensions, applies it, writes manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import create_schema as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))


def _patch_common(monkeypatch: pytest.MonkeyPatch, conn: FakeConn, unload=None) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: 'CREATE TABLE public."VoterTX" ("LALVOTERID" text);')
    monkeypatch.setattr(step, "load_databricks_columns", lambda cfg, rd: {})
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: f"s3://b/{sub}")
    # read_manifest: schema -> None (not done); unload -> provided
    def _read(cfg, rd, name, model):
        return None if name == "schema" else unload
    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("manifest", m) or "s3://b/_manifest/schema.json")
    return captured


def test_applies_extensions_and_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    captured = _patch_common(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE" in s for s in sql)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_commons" in s for s in sql)
    assert any('CREATE TABLE public."VoterTX"' in s for s in sql)
    assert manifest.status == "complete"
    assert "VoterFile" in manifest.tables_created
    assert len(manifest.tables_created) == 52  # 51 states + VoterFile


def test_skips_when_manifest_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "s3://b/x")
    assert step.run(_CFG, "20260609") is done
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_create_schema.py -v`
Expected: FAIL — the stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub with the adapted implementation**

Overwrite `src/loader/people_api/steps/create_schema.py`:

```python
"""Step 3 — create target tables on the new cluster (ClickUp DATA-1910).

Loads the committed prod schema snapshot + Databricks column list, runs
`emit_ddl` to produce the merged schema, connects to the new cluster as
master, installs `aws_s3`/`aws_commons`, applies the DDL, and (when an unload
manifest is present) verifies the `Voter{STATE}` column set.

No indexes/PKs/FKs here — `build-indexes` (step 5) adds those after COPY.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    SchemaManifest,
    UnloadManifest,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.emit_ddl import STATES, emit_target_schema
from loader.people_api.schema.snapshot import load_databricks_columns, load_prod_dump

log = get_logger(__name__)


def _verify_column_set(
    conn: psycopg.Connection, table: str, expected: list[str]
) -> list[str]:
    """Return the symmetric difference between actual and expected columns."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
            (table,),
        )
        actual = [r[0] for r in cur.fetchall()]
    return sorted(set(expected).symmetric_difference(set(actual)))


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
    # unload may not have run yet; if present we use it to verify columns.
    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)

    started = datetime.now(UTC)
    log.info("schema.start")

    prod_dump = load_prod_dump(cfg, run_date)
    databricks_cols = load_databricks_columns(cfg, run_date)

    ddl = emit_target_schema(prod_dump, databricks_cols)
    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", ddl)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(ddl))

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        with conn.cursor() as cur:
            cur.execute(ddl)
        log.info("schema.ddl_applied")

        tables_created = [f"Voter{s}" for s in STATES] + ["VoterFile"]

        column_diff: dict[str, list[str]] = {}
        if unload is not None:
            for state in STATES:
                table = f"Voter{state}"
                diff = _verify_column_set(conn, table, unload.columns)
                if diff:
                    column_diff[table] = diff
            if column_diff:
                log.warning(
                    "schema.column_diff",
                    tables=list(column_diff.keys())[:5],
                    total=len(column_diff),
                )
            else:
                log.info("schema.columns_verified", tables=len(STATES))

    manifest = SchemaManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        target_schema_s3_uri=ddl_uri,
        tables_created=tables_created,
        column_diff_from_prod=column_diff,
    )
    uri = write_manifest(cfg, manifest)
    log.info("schema.complete", uri=uri, tables=len(tables_created))
    return manifest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_create_schema.py -v`
Expected: PASS.

- [ ] **Step 5: Lint/type + commit**

```bash
uv run ruff check src/loader/people_api/steps/create_schema.py tests/test_create_schema.py
git add src/loader/people_api/steps/create_schema.py tests/test_create_schema.py
git commit -m "feat(DATA-1910): implement create-schema step"
```

---

## Task 7: `copy` step (DATA-1851)

**Files:**
- Modify: `src/loader/people_api/steps/copy_s3.py` (replace stub)
- Test: `tests/test_copy_s3.py`

Adapted from POC `steps/copy_s3.py`. Deltas: monorepo import paths; resolve the writer endpoint once via `resolve_writer_endpoint`; `unload` manifest stays required (it is the file-listing + per-state row-count contract). The parallel `aws_s3.table_import_from_s3` mechanics, 0-byte skip, largest-first ordering, three-way per-state idempotency (skip / truncate+reload / load), error aggregation, and the `complete`-only-when-all-states-covered logic are preserved verbatim.

- [ ] **Step 1: Write the failing test**

Create `tests/test_copy_s3.py`:

```python
"""copy step: per-file aws_s3 COPY, idempotency, error aggregation."""

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


def test_copy_one_file_emits_table_import_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(_CFG, "20260609", "wh", "VoterTX", "voter_export_20260609/state_id=TX/part-0.csv")
    sql = executed_sql(conn)
    assert any("aws_s3.table_import_from_s3" in s for s in sql)
    # all five session SETs precede the import
    assert any("SET synchronous_commit = off" in s for s in sql)
    assert any("SET statement_timeout = 0" in s for s in sql)


def test_run_loads_state_and_completes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="voter_export_20260609/state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")

    def _read(cfg, rd, name, model):
        return None if name == "copy" else unload
    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    # _load_state returns a complete result without touching a real DB.
    monkeypatch.setattr(
        step, "_load_state",
        lambda **kw: step.CopyTableResult(
            table=f"Voter{kw['state']}", expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"], files_loaded=1, seconds_elapsed=1.0),
    )
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert manifest.results[0].table == "VoterTX"


def test_skips_zero_byte_files(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_keys: list[str] = []
    files = [
        SimpleNamespace(state="TX", s3_key="k-empty", size_bytes=0),
        SimpleNamespace(state="TX", s3_key="k-good", size_bytes=10),
    ]
    unload = _unload(files, {"TX": 5})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _capture(**kw):
        seen_keys.extend(kw["s3_keys"])
        return step.CopyTableResult(table=f"Voter{kw['state']}", expected_rows=kw["expected_rows"],
                                    actual_rows=kw["expected_rows"], files_loaded=len(kw["s3_keys"]), seconds_elapsed=0.0)
    monkeypatch.setattr(step, "_load_state", _capture)
    step.run(_CFG, "20260609")
    assert "k-empty" not in seen_keys and "k-good" in seen_keys
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_copy_s3.py -v`
Expected: FAIL — stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub with the adapted implementation**

Overwrite `src/loader/people_api/steps/copy_s3.py`:

```python
"""Step 4 — parallel COPY S3 → Aurora via `aws_s3.table_import_from_s3` (DATA-1851).

A ThreadPoolExecutor issues one `aws_s3.table_import_from_s3` per file, many
in parallel. PG's COPY is single-threaded per statement; file-level
parallelism is the only lever. Session-level SETs apply load-tuned values so
they can't linger into serving.

Idempotency is per-state: compare `count(*)` against the UnloadManifest's
per-state count. Equal → skip. Zero → load. Partial → TRUNCATE + reload.
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

_SESSION_SQL: tuple[str, ...] = (
    "SET synchronous_commit = off",
    "SET maintenance_work_mem = '4GB'",
    "SET work_mem = '256MB'",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _copy_one_file(
    cfg: LoaderConfig,
    run_date: str,
    writer_endpoint: str,
    table: str,
    s3_key: str,
) -> None:
    """Run `aws_s3.table_import_from_s3` for one (table, key) pair on its own backend."""
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        for stmt in _SESSION_SQL:
            cur.execute(stmt)
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
                "table": f'public."{table}"',
                "options": "(FORMAT text, DELIMITER E'\\t', NULL '\\N', ENCODING 'UTF8')",
                "bucket": cfg.s3_bucket,
                "key": s3_key,
                "region": cfg.aws_region,
            },
        )


def _count_rows(conn: psycopg.Connection, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM public."{table}"')
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _truncate(conn: psycopg.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE public."{table}"')


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
    table = f"Voter{state}"
    bind(state=state)
    started = time.time()

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_rows(conn, table)
        if actual == expected_rows and expected_rows > 0:
            log.info("copy.skip", state=state, rows=actual)
            return CopyTableResult(
                table=table, expected_rows=expected_rows, actual_rows=actual,
                files_loaded=0, seconds_elapsed=0.0,
            )
        if 0 < actual < expected_rows:
            log.info("copy.partial_reload", state=state, existing_rows=actual, expected=expected_rows)
            _truncate(conn, table)

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(_copy_one_file, cfg, run_date, writer_endpoint, table, key): key
            for key in s3_keys
        }
        errors: list[tuple[str, Exception]] = []
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                fut.result()
                log.info("copy.file_done", state=state, key=key)
            except Exception as e:  # noqa: BLE001 — aggregate and re-raise below
                log.error("copy.file_failed", state=state, key=key, error=str(e))
                errors.append((key, e))
        if errors:
            raise RuntimeError(f"{state}: {len(errors)} files failed — first: {errors[0][1]!r}")

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_rows(conn, table)
    elapsed = time.time() - started
    log.info("copy.state_done", state=state, rows=actual, files=len(s3_keys), seconds=round(elapsed, 1))
    return CopyTableResult(
        table=table, expected_rows=expected_rows, actual_rows=actual,
        files_loaded=len(s3_keys), seconds_elapsed=elapsed,
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

    # Group files by state, skipping 0-byte parts (aws_s3 fails them with HTTP 416).
    files_by_state: dict[str, list[str]] = {}
    for f in unload.files:
        if f.size_bytes == 0:
            continue
        files_by_state.setdefault(f.state, []).append(f.s3_key)

    states_to_load = [state_filter] if state_filter else sorted(files_by_state.keys())

    results: list[CopyTableResult] = []
    if existing is not None and existing.results:
        results.extend(r for r in existing.results if r.actual_rows > 0)
    already_done = {r.table for r in results}

    for state in sorted(states_to_load, key=lambda s: -unload.per_state_row_counts.get(s, 0)):
        table = f"Voter{state}"
        if table in already_done:
            log.info("copy.state_carried", state=state)
            continue
        result = _load_state(
            cfg=cfg,
            run_date=run_date,
            writer_endpoint=writer_endpoint,
            state=state,
            expected_rows=unload.per_state_row_counts.get(state, 0),
            s3_keys=files_by_state.get(state, []),
            parallelism=parallelism,
        )
        results = [r for r in results if r.table != table] + [result]

    covered = {r.table for r in results}
    expected_tables = {f"Voter{s}" for s in unload.per_state_row_counts}
    all_loaded = covered >= expected_tables

    manifest = CopyManifest(
        run_date=run_date,
        status="complete" if all_loaded else "in_progress",
        started_at=started,
        finished_at=datetime.now(UTC) if all_loaded else None,
        results=results,
    )
    uri = write_manifest(cfg, manifest)
    log.info("copy.complete", uri=uri, all_loaded=all_loaded, covered=len(covered), expected=len(expected_tables))
    return manifest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_copy_s3.py -v`
Expected: PASS.

- [ ] **Step 5: Lint/type + commit**

```bash
uv run ruff check src/loader/people_api/steps/copy_s3.py tests/test_copy_s3.py
git add src/loader/people_api/steps/copy_s3.py tests/test_copy_s3.py
git commit -m "feat(DATA-1851): implement copy step (server-side S3->PG parallel COPY)"
```

---

## Task 8: `build-indexes` step (DATA-1853)

**Files:**
- Modify: `src/loader/people_api/steps/build_indexes.py` (replace stub)
- Test: `tests/test_build_indexes.py`

Adapted from POC `steps/build_indexes.py`. Deltas: monorepo import paths; `resolve_writer_endpoint`; `load_prod_dump` (committed snapshot) instead of `_load_s3_text(inspect.prod_ddl_s3_uri)`; `unload` becomes **optional** (used only for largest-first ordering — fall back to `STATES` order when absent), removing the hard inspect/provision/unload gating. PK→indexes→FKs→ANALYZE→l2Type-coverage phases, parallelism, round-robin interleave, and the `LEGACY_RENAMES` + `IF NOT EXISTS` SQL rewrite are preserved.

- [ ] **Step 1: Write the failing test**

Create `tests/test_build_indexes.py`:

```python
"""build-indexes: parses prod dump, builds PKs/indexes/FKs, ANALYZEs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

_DUMP = '''
ALTER TABLE ONLY public."VoterTX" ADD CONSTRAINT "VoterTX_pkey" PRIMARY KEY ("LALVOTERID");
CREATE INDEX "VoterTX_LastName_idx" ON public."VoterTX" USING btree ("Voters_LastName");
'''


def test_run_builds_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, we: [])

    def _read(cfg, rd, name, model):
        return None  # indexes not done; unload absent (ordering falls back)
    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("ADD CONSTRAINT" in s and "PRIMARY KEY" in s for s in sql)
    assert any("CREATE INDEX IF NOT EXISTS" in s for s in sql)
    assert any("ANALYZE" in s for s in sql)
    assert manifest.status == "complete"
    assert any(i.index_name == "VoterTX_LastName_idx" for i in manifest.indexes)
    assert "VoterTX_pkey" in manifest.constraints_added


def test_index_sql_rewrite_injects_if_not_exists() -> None:
    out = step._rewrite_index_sql('CREATE INDEX "x" ON public."VoterTX" (a);')
    assert "CREATE INDEX IF NOT EXISTS" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_build_indexes.py -v`
Expected: FAIL — stub raises `NotImplementedError`; `_rewrite_index_sql` not defined.

- [ ] **Step 3: Replace the stub with the adapted implementation**

Overwrite `src/loader/people_api/steps/build_indexes.py`. Note one refactor vs the POC: the inline index-SQL rewrite is extracted into a pure, testable `_rewrite_index_sql`.

```python
"""Step 5 — build primary keys, non-unique indexes, FK constraints, ANALYZE (DATA-1853).

Order: PKs (parallel across tables) → non-unique indexes (parallel,
round-robin interleaved across tables) → FKs (sequential) → ANALYZE
(parallel). Index/PK/FK specs are parsed from the committed prod-dump
snapshot. `CREATE INDEX` (not CONCURRENTLY) since the cluster is idle.

Tail check: `l2Type` coverage — every distinct `l2Type` in prod
`org_districts` must exist as a column on the new `Voter{STATE}` tables.
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
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.emit_ddl import STATES
from loader.people_api.schema.index_specs import (
    IndexDef,
    PrimaryKey,
    parse_foreign_keys,
    parse_indexes,
    parse_primary_keys,
)
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.voter_columns import LEGACY_RENAMES

log = get_logger(__name__)

_INDEX_BUILDERS = 32

_BUILD_SESSION_SQL: tuple[str, ...] = (
    "SET maintenance_work_mem = '8GB'",
    "SET max_parallel_maintenance_workers = 8",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _apply_session(cur: psycopg.Cursor) -> None:
    for stmt in _BUILD_SESSION_SQL:
        cur.execute(stmt)


def _rewrite_index_sql(sql: str) -> str:
    """Apply LEGACY_RENAMES and inject IF NOT EXISTS for idempotent reruns."""
    for old, new in LEGACY_RENAMES.items():
        if old in sql:
            sql = sql.replace(old, new)
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
            cur.execute(sql)
            log.info("indexes.pk_added", table=pk.table, constraint=pk.constraint)
        except (psycopg.errors.DuplicateObject, psycopg.errors.InvalidTableDefinition):
            log.info("indexes.pk_exists", table=pk.table, constraint=pk.constraint)


def _create_index(cfg: LoaderConfig, run_date: str, writer_endpoint: str, idx: IndexDef) -> None:
    sql = _rewrite_index_sql(idx.sql)
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(sql)
        log.info("indexes.built", table=idx.table, name=idx.name, unique=idx.unique)


def _add_foreign_key(cfg: LoaderConfig, run_date: str, writer_endpoint: str, fk_sql: str, name: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        try:
            cur.execute(fk_sql)
            log.info("indexes.fk_added", name=name)
        except (psycopg.errors.DuplicateObject, psycopg.errors.InvalidTableDefinition):
            log.info("indexes.fk_exists", name=name)


def _analyze(cfg: LoaderConfig, run_date: str, writer_endpoint: str, table: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(f'ANALYZE public."{table}"')
        log.info("indexes.analyzed", table=table)


def _l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> list[str]:
    """`l2Type` values in prod `org_districts` not present as columns on the new schema."""
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # noqa: BLE001 — org_districts lives in the app DB, not always reachable
        log.warning("indexes.l2type.skip", error=str(e))
        return []

    sample_table = f"Voter{STATES[0]}"
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        new_cols = {r[0] for r in cur.fetchall()}
    return sorted(v for v in distinct_l2types if v not in new_cols)


def _order_key(counts: dict[str, int]):
    return lambda table_name: -counts.get(table_name.removeprefix("Voter"), 0)


def run(cfg: LoaderConfig, run_date: str) -> IndexManifest:
    bind(run_date=run_date, step="indexes")
    existing = read_manifest(cfg, run_date, "indexes", IndexManifest)
    if existing and existing.status == "complete":
        log.info("indexes.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "indexes"))
        return existing

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    # unload is optional: used only to order builds largest-state-first.
    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    counts: dict[str, int] = unload.per_state_row_counts if unload else {}

    started = datetime.now(UTC)
    log.info("indexes.start")

    prod_dump = load_prod_dump(cfg, run_date)
    pks = parse_primary_keys(prod_dump)
    idxs = parse_indexes(prod_dump)
    fks = parse_foreign_keys(prod_dump)
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs), foreign_keys=len(fks))

    voter_tables = {f"Voter{s}" for s in STATES} | {"VoterFile"}
    pks = [p for p in pks if p.table in voter_tables]
    idxs = [i for i in idxs if i.table in voter_tables]
    fks = [f for f in fks if f.table in voter_tables]

    # 1. Primary keys — parallel across tables, biggest first.
    pks_ordered = sorted(pks, key=lambda p: _order_key(counts)(p.table))
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [executor.submit(_add_primary_key, cfg, run_date, writer_endpoint, pk) for pk in pks_ordered]
        for fut in as_completed(futures):
            fut.result()

    # 2. Non-unique indexes — round-robin interleave across tables, biggest first.
    by_table: dict[str, list[IndexDef]] = {}
    for i in idxs:
        by_table.setdefault(i.table, []).append(i)
    tables_ordered = sorted(by_table.keys(), key=_order_key(counts))
    interleaved: list[IndexDef] = []
    queues = [list(by_table[t]) for t in tables_ordered]
    while any(queues):
        for q in queues:
            if q:
                interleaved.append(q.pop(0))
    log.info("indexes.order", biggest_first=tables_ordered[:5], total_indexes=len(interleaved))

    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [executor.submit(_create_index, cfg, run_date, writer_endpoint, idx) for idx in interleaved]
        for fut in as_completed(futures):
            fut.result()

    # 3. FKs — sequential.
    for fk in fks:
        _add_foreign_key(cfg, run_date, writer_endpoint, fk.sql, fk.constraint)

    # 4. ANALYZE — parallel across tables.
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [executor.submit(_analyze, cfg, run_date, writer_endpoint, t) for t in sorted(voter_tables)]
        for fut in as_completed(futures):
            fut.result()

    # 5. l2Type coverage.
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
        constraints_added=[p.constraint for p in pks] + [f.constraint for f in fks],
        analyzed_tables=sorted(voter_tables),
        l2type_coverage_missing=missing,
    )
    uri = write_manifest(cfg, manifest)
    log.info("indexes.complete", uri=uri, indexes=len(idxs), pks=len(pks), fks=len(fks))
    return manifest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_build_indexes.py -v`
Expected: PASS.

- [ ] **Step 5: Lint/type + commit**

```bash
uv run ruff check src/loader/people_api/steps/build_indexes.py tests/test_build_indexes.py
git add src/loader/people_api/steps/build_indexes.py tests/test_build_indexes.py
git commit -m "feat(DATA-1853): implement build-indexes step"
```

---

## Task 9: `validate` step (DATA-1911) with ±10% tolerance

**Files:**
- Modify: `src/loader/people_api/steps/validate.py` (replace stub)
- Test: `tests/test_validate.py`

Adapted from POC `steps/validate.py`. Deltas: monorepo import paths; `resolve_writer_endpoint`; drop the `inspect` manifest dependency (use `unload.per_state_row_counts` for sample-state selection); **`_check_row_counts` uses a ±10% envelope** via a named `_ROW_COUNT_TOLERANCE = 0.10`. The five checks, the Markdown companion, and non-zero-exit-on-failure (handled by `cli.py`) are preserved.

- [ ] **Step 1: Write the failing test**

Create `tests/test_validate.py`:

```python
"""validate: five checks, ±10% row-count tolerance, all_passed aggregation."""

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
    conn = FakeConn().queue_result((105,))  # 105 vs expected 100 → within ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is True


def test_row_counts_outside_tolerance_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((50,))  # 50 vs 100 → outside ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is False
    assert check.details["mismatch_count"] == 1


def test_run_aggregates_and_writes_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    unload = SimpleNamespace(status="complete", per_state_row_counts={"TX": 100})
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: captured.setdefault("md", body) or "uri")
    # Stub the individual checks to avoid DB; assert orchestration + all_passed.
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

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_validate.py -v`
Expected: FAIL — stub raises `NotImplementedError`.

- [ ] **Step 3: Replace the stub with the adapted implementation**

Overwrite `src/loader/people_api/steps/validate.py`:

```python
"""Step 7 — validate the new cluster before handoff (ClickUp DATA-1911).

Five checks (all must pass):
1. row_counts_match_databricks — per-state count within ±10% of the unload baseline.
2. schema_diff_clean — new columns are a superset of prod minus retired typos.
3. index_constraint_diff_clean — every prod index/constraint present on new.
4. sample_queries_pass — voterFile.util.ts-shaped queries return without error.
5. l2Type_coverage — every distinct org_districts l2Type maps to a column.

Writes validate.json AND a Markdown companion for human sign-off.
`all_passed=False` blocks handoff — `cli.py` exits non-zero.
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
from loader.people_api.schema.emit_ddl import STATES
from loader.people_api.schema.voter_columns import LEGACY_RENAMES, VOTER_TARGET_COLUMNS

log = get_logger(__name__)

# Per-state row-count gate: new cluster must be within ±10% of the unload
# baseline. (Decided 2026-06-09; tickets said ±15%, repo plan said exact.)
_ROW_COUNT_TOLERANCE = 0.10


def _check_row_counts(
    cfg: LoaderConfig, run_date: str, writer_endpoint: str, expected: dict[str, int]
) -> ValidationCheck:
    mismatches: dict[str, dict[str, float]] = {}
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        for state, expected_count in expected.items():
            table = f"Voter{state}"
            with conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM public."{table}"')
                row = cur.fetchone()
                actual = int(row[0]) if row else 0
            low = expected_count * (1 - _ROW_COUNT_TOLERANCE)
            high = expected_count * (1 + _ROW_COUNT_TOLERANCE)
            if not (low <= actual <= high):
                mismatches[table] = {"expected": expected_count, "actual": actual}
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


def _check_schema_diff(
    cfg: LoaderConfig, run_date: str, writer_endpoint: str, per_state_row_counts: dict[str, int]
) -> ValidationCheck:
    sample_state = "TX" if "TX" in per_state_row_counts else STATES[0]
    sample_table = f"Voter{sample_state}"

    with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        prod_cols = {r[0] for r in cur.fetchall()}

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        new_cols = {r[0] for r in cur.fetchall()}

    target_cols = {c.name for c in VOTER_TARGET_COLUMNS}
    retired = set(LEGACY_RENAMES.keys())
    missing_from_new = (prod_cols - retired) - new_cols
    unexpected_in_new = new_cols - target_cols

    return ValidationCheck(
        name="schema_diff_clean",
        passed=not missing_from_new and not unexpected_in_new,
        details={
            "sample_table": sample_table,
            "prod_cols": len(prod_cols),
            "new_cols": len(new_cols),
            "target_cols": len(target_cols),
            "missing_from_new": sorted(missing_from_new)[:20],
            "unexpected_in_new": sorted(unexpected_in_new)[:20],
            "retired_typos": sorted(retired),
        },
    )


def _check_indexes(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    sample_table = "VoterTX"
    query = (
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=%s ORDER BY indexname"
    )
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute(query, (sample_table,))
            prod_idx = {r[0] for r in cur.fetchall()}
    except Exception as e:  # noqa: BLE001
        return ValidationCheck(name="index_constraint_diff_clean", passed=False, details={"error_reading_prod": str(e)})

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(query, (sample_table,))
        new_idx = {r[0] for r in cur.fetchall()}

    missing = sorted(prod_idx - new_idx)
    return ValidationCheck(
        name="index_constraint_diff_clean",
        passed=not missing,
        details={
            "sample_table": sample_table,
            "prod_count": len(prod_idx),
            "new_count": len(new_idx),
            "missing_from_new": missing[:20],
        },
    )


_SAMPLE_QUERIES: tuple[tuple[str, str], ...] = (
    ("party_filter", 'SELECT count(*) FROM public."VoterTX" WHERE "Parties_Description" = \'Democratic\''),
    ("gender_filter", 'SELECT count(*) FROM public."VoterTX" WHERE "Voters_Gender" = \'F\''),
    ("age_cast_filter", 'SELECT count(*) FROM public."VoterTX" WHERE "Voters_Age"::integer BETWEEN 18 AND 35'),
    ("district_lookup", 'SELECT "US_Congressional_District", count(*) FROM public."VoterTX" GROUP BY "US_Congressional_District" ORDER BY count(*) DESC LIMIT 10'),
    ("family_dedupe", 'SELECT count(*) FROM public."VoterTX" a WHERE EXISTS (SELECT 1 FROM public."VoterTX" b WHERE b."Mailing_Families_FamilyID" = a."Mailing_Families_FamilyID") LIMIT 1'),
    ("justice_of_the_peace", 'SELECT count(*) FROM public."VoterTX" WHERE "Judicial_Justice_of_the_Peace" IS NOT NULL'),
)


def _check_sample_queries(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    results: dict[str, str] = {}
    failures: dict[str, str] = {}
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        for label, sql in _SAMPLE_QUERIES:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
                    results[label] = "ok" if row is not None else "empty"
            except Exception as e:  # noqa: BLE001
                failures[label] = str(e)
    return ValidationCheck(name="sample_queries_pass", passed=not failures, details={"pass": results, "fail": failures})


def _check_l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # noqa: BLE001
        return ValidationCheck(name="l2Type_coverage", passed=False, details={"error_reading_org_districts": str(e)})

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='VoterTX'"
        )
        new_cols = {r[0] for r in cur.fetchall()}
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
        status = "PASS" if c.passed else "FAIL"
        lines.append(f"### {c.name} — {status}")
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
        _check_schema_diff(cfg, run_date, writer_endpoint, unload.per_state_row_counts),
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

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_validate.py -v`
Expected: PASS.

- [ ] **Step 5: Lint/type + commit**

```bash
uv run ruff check src/loader/people_api/steps/validate.py tests/test_validate.py
git add src/loader/people_api/steps/validate.py tests/test_validate.py
git commit -m "feat(DATA-1911): implement validate step (5 checks, ±10% row-count gate)"
```

---

## Task 10: Full-suite verification + CLI integration

**Files:** none (verification only) — confirms `cli.py` wiring works with real implementations.

- [ ] **Step 1: Whole test suite passes**

Run: `uv run pytest -v`
Expected: all tests pass (existing 5 files + the 8 new test files). No `NotImplementedError`.

- [ ] **Step 2: Lint + format + types clean**

Run: `uv run ruff check . && uv run ruff format --check . && uv run ty check`
Expected: no errors. (If `ruff format --check` reports diffs, run `uv run ruff format .` and amend.)

- [ ] **Step 3: CLI smoke still green and commands invoke real steps**

Run: `uv run pytest tests/test_cli_smoke.py -v`
Expected: PASS — all subcommands still registered.

Run: `uv run loader create-schema --help` and `uv run loader validate --help`
Expected: help renders, exit 0.

- [ ] **Step 4: Offline dry-run sanity (no AWS/DB) — confirms the missing-endpoint error path**

Run:
```bash
env -u LOADER_NEW_WRITER_ENDPOINT uv run loader create-schema --date 20260609 || echo "expected failure: no writer endpoint / no AWS"
```
Expected: a clear `RuntimeError` about `LOADER_NEW_WRITER_ENDPOINT` (or the AWS caller check at startup) — not a `NotImplementedError` and not a stack trace from missing wiring.

- [ ] **Step 5: Final commit if anything changed (format)**

```bash
git add -A
git commit -m "chore(DATA-1640): formatting + full-suite verification" || echo "nothing to commit"
```

---

## Out of scope for this PR (do not implement)

- `inspect-prod` (DATA-1907), `unload` (DATA-1908), `provision` (DATA-1909), `resize` (DATA-1854), `status`/`teardown` (DATA-1912) — their step files stay as-is.
- The Airflow DAG (DATA-1913), the Prisma-to-DDL emitter (DATA-1904), and production cutover (DATA-1855).
- The four steps consume the `unload`/`provision` manifests as contracts; this PR does not produce them (snapshot + env override stand in).

## Notes / known follow-ups to flag in the PR description

- The committed `prod_dump.sql` snapshot is a point-in-time capture; when DATA-1904 (Prisma-derived DDL) or DATA-1907 (`inspect-prod`) land, the snapshot source should be re-pointed at the live artifact and `snapshot.py` extended accordingly.
- `l2Type_coverage` (in both build-indexes and validate) degrades to a warning/skip when `org_districts` is unreachable — it lives in the gp-api app DB, not the voter DB. Real coverage needs a separate pg_service entry (tracked under the epic's tradeoffs).
- `build-indexes` memory math (`_INDEX_BUILDERS=32` × `maintenance_work_mem=8GB`) was sized for a 512 GB box; re-derive for the chosen load instance class before a real run.
