# Load-box right-sizing + index-build core fill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision the load cluster on a 64-vCPU box, scale it up to 192 vCPU only for `build_indexes`, and raise the parallel-worker pool so the index build fills the box it pays for.

**Architecture:** Split the single load instance class into a copy-phase class (`load_instance_class`, db.r8g.16xlarge) and an index-phase class (`index_instance_class`, db.r8g.48xlarge). `build_indexes` gains an idempotent `_ensure_instance_class` that scales the writer up (modify + waiter, mirroring `resize`'s pattern) before any build work; `resize` still flips down to Serverless afterward. `build_indexes`' session SQL raises `max_parallel_workers` (96 → 176) and `max_parallel_maintenance_workers` (8 → 16).

**Tech Stack:** Python 3.14, uv, Astral toolchain (ruff + ty), psycopg3, boto3 (RDS), pytest. All commands run from `people-api-loader/`.

## Global Constraints

- Astral toolchain only: `uv run ruff check`, `uv run ruff format --check`, `uv run ty check`, `uv run pytest` all clean. Line length 110. No black/isort/flake8/mypy/pyright.
- No live infra in tests. RDS is mocked (monkeypatch `step.rds`), Postgres via `tests/_fakes.py` (`FakeConn`, `fake_connect`, `executed_sql`).
- Copy-phase box = `db.r8g.16xlarge` (64 vCPU); index-phase box = `db.r8g.48xlarge` (192 vCPU) — exact values, verbatim.
- `ty` needs `# ty: ignore[no-matching-overload]` on dynamic-string `cur.execute(...)`. BLE001 is NOT selected and RUF100 (unused-noqa) IS — do not add `# noqa: BLE001`; use a plain comment on intentional broad excepts.
- Do NOT change copy parallelism (`_DEFAULT_PARALLELISM` stays 128) or `_DEFAULT_BUILDERS` (stays 128).
- Spec: `docs/superpowers/specs/2026-07-09-load-box-right-sizing-design.md`.

---

### Task 1: Split the load instance class into copy-phase and index-phase classes

**Files:**
- Modify: `src/loader/people_api/config.py` (defaults ~line 65-70; the `load_instance_class` field ~line 135; `from_env` return ~line 212)
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `LoaderConfig.index_instance_class: str` (new field), `config.DEFAULT_INDEX_INSTANCE_CLASS = "db.r8g.48xlarge"`, `config.DEFAULT_LOAD_INSTANCE_CLASS = "db.r8g.16xlarge"`, env override `LOADER_INDEX_INSTANCE_CLASS`. Consumed by Task 3.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_config.py`:

```python
def test_from_env_instance_classes_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_LOAD_INSTANCE_CLASS", raising=False)
    monkeypatch.delenv("LOADER_INDEX_INSTANCE_CLASS", raising=False)
    cfg = LoaderConfig.from_env()
    # copy/provision phase runs on the smaller box; build_indexes scales up to the large box.
    assert cfg.load_instance_class == "db.r8g.16xlarge"
    assert cfg.index_instance_class == "db.r8g.48xlarge"


def test_from_env_instance_classes_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_LOAD_INSTANCE_CLASS", "db.r8g.8xlarge")
    monkeypatch.setenv("LOADER_INDEX_INSTANCE_CLASS", "db.r8g.24xlarge")
    cfg = LoaderConfig.from_env()
    assert cfg.load_instance_class == "db.r8g.8xlarge"
    assert cfg.index_instance_class == "db.r8g.24xlarge"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_config.py::test_from_env_instance_classes_default tests/test_config.py::test_from_env_instance_classes_override -v`
Expected: FAIL — `AttributeError: ... 'index_instance_class'` and/or the load-class assertion fails (default is still `db.r8g.48xlarge`).

- [ ] **Step 3: Update the defaults and comment in config.py**

Replace the block at `config.py:65-70`:

```python
# Load-phase instance sizing. Prod serving is serverless; load uses provisioned (see
# PLAN_LOADER.md "Provisioned-vs-Serverless"). We use TWO classes: provision/create_schema/copy
# run on the smaller `load_instance_class` (copy is I/O/WAL-bound, not CPU-bound), and
# build_indexes scales the writer UP to `index_instance_class` (build_indexes is cleanly CPU-bound
# and scales with vCPU — see steps/build_indexes.py). resize then flips the writer to serverless.
# Override per-env with LOADER_LOAD_INSTANCE_CLASS / LOADER_INDEX_INSTANCE_CLASS; keep
# _DEFAULT_BUILDERS in build_indexes.py in step with the index box's vCPU.
DEFAULT_LOAD_INSTANCE_CLASS = "db.r8g.16xlarge"
DEFAULT_INDEX_INSTANCE_CLASS = "db.r8g.48xlarge"
```

- [ ] **Step 4: Add the field to the LoaderConfig dataclass**

At `config.py:135`, add `index_instance_class` right after `load_instance_class`:

```python
    engine_version: str
    load_instance_class: str
    index_instance_class: str
    serve_min_acu: float
    serve_max_acu: float
```

- [ ] **Step 5: Populate it in from_env**

At `config.py:212`, add the line after `load_instance_class=...`:

```python
            load_instance_class=_env("LOADER_LOAD_INSTANCE_CLASS", DEFAULT_LOAD_INSTANCE_CLASS),
            index_instance_class=_env("LOADER_INDEX_INSTANCE_CLASS", DEFAULT_INDEX_INSTANCE_CLASS),
```

- [ ] **Step 6: Run tests + lint to verify they pass**

Run: `uv run pytest tests/test_config.py -v && uv run ruff check src/loader/people_api/config.py tests/test_config.py && uv run ty check`
Expected: PASS (all config tests green, ruff + ty clean).

- [ ] **Step 7: Commit**

```bash
git add src/loader/people_api/config.py tests/test_config.py
git commit -m "feat(loader): split load instance class into copy-phase + index-phase

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Fill the box — raise the parallel-worker pool in build_indexes session SQL

**Files:**
- Modify: `src/loader/people_api/steps/build_indexes.py:55-61` (`_BUILD_SESSION_SQL`)
- Test: `tests/test_build_indexes.py`

**Interfaces:**
- Consumes: nothing new. Produces: nothing consumed by other tasks (self-contained tuning).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_build_indexes.py`:

```python
def test_build_session_sql_fills_the_box() -> None:
    # The load box is db.r8g.48xlarge (192 vCPU). Aurora defaults max_parallel_workers to ~96
    # (~vCPU/2), which caps the build at ~125 active backends and leaves ~67 cores idle. Raise the
    # pool so the index build uses the box it pays for, and widen per-build maintenance workers so
    # the long-pole giant partition builds spread wider.
    sql = step._BUILD_SESSION_SQL
    assert "SET max_parallel_workers = 176" in sql
    assert "SET max_parallel_maintenance_workers = 16" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_build_indexes.py::test_build_session_sql_fills_the_box -v`
Expected: FAIL — `max_parallel_maintenance_workers = 8` today and no `max_parallel_workers` set.

- [ ] **Step 3: Update `_BUILD_SESSION_SQL`**

Replace `build_indexes.py:55-61`:

```python
_BUILD_SESSION_SQL: tuple[str, ...] = (
    # 3 GB * up to 128 builders = ~384 GB, safe alongside the buffer pool on the 1.5 TiB r8g.48xl.
    "SET maintenance_work_mem = '3GB'",
    # Aurora defaults max_parallel_workers to ~vCPU/2 (=96 on the 192-vCPU index box), which caps
    # the build at ~125 active backends and leaves ~67 cores idle (measured 2026-07-09). Raise the
    # per-session pool ceiling so the tail (~33 leaders + parallel workers) fills the box.
    # max_parallel_workers is a user-context GUC, so a per-session SET is honored; max_worker_processes
    # is 384 (ample), so no reboot-class parameter change is needed.
    "SET max_parallel_workers = 176",
    # Widen per-build parallelism so the long-pole giant partition builds (common columns on
    # CA/TX/FL/NY) spread wider and the absolute tail shrinks.
    "SET max_parallel_maintenance_workers = 16",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)
```

- [ ] **Step 4: Run test + lint to verify it passes**

Run: `uv run pytest tests/test_build_indexes.py -v && uv run ruff check src/loader/people_api/steps/build_indexes.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/steps/build_indexes.py tests/test_build_indexes.py
git commit -m "perf(loader): raise max_parallel_workers so build_indexes fills the box

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Scale the writer up to the index class inside build_indexes

**Files:**
- Modify: `src/loader/people_api/steps/build_indexes.py` (imports ~line 27-31; new `_ensure_instance_class` helper; call site in `run()` after `log.info("indexes.start")` ~line 235)
- Modify: `src/loader/people_api/steps/resize.py:3` (stale "db.r7g" docstring)
- Test: `tests/test_build_indexes.py`

**Interfaces:**
- Consumes: `LoaderConfig.index_instance_class` (Task 1); `cfg.new_writer_instance_id(run_date)` (existing, used by resize); `loader.core.aws.rds` (existing).
- Produces: `build_indexes._ensure_instance_class(cfg: LoaderConfig, run_date: str) -> None`, callable and monkeypatchable via `step.rds`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_build_indexes.py` (near the top, after the existing imports add `from botocore.exceptions import ClientError`):

```python
class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class _FakeRds:
    """RDS double for the scale-up: records modify calls, can raise a one-shot in-progress fault."""

    def __init__(self, current_class: str, *, raise_once: str | None = None) -> None:
        self.current_class = current_class
        self.calls: list[tuple[str, dict]] = []
        self._raise_once = raise_once  # ClientError code to raise on the first modify, then clear
        self._raised = False

    def describe_db_instances(self, **kw: object) -> dict:
        self.calls.append(("describe", dict(kw)))
        return {"DBInstances": [{"DBInstanceClass": self.current_class}]}

    def modify_db_instance(self, **kw: object) -> None:
        self.calls.append(("modify", dict(kw)))
        if self._raise_once and not self._raised:
            self._raised = True
            raise ClientError({"Error": {"Code": self._raise_once, "Message": "in progress"}}, "ModifyDBInstance")

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


def _scale_cfg() -> LoaderConfig:
    from types import SimpleNamespace

    return cast(
        LoaderConfig,
        SimpleNamespace(
            index_instance_class="db.r8g.48xlarge",
            new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-dev-writer",
        ),
    )


def test_ensure_instance_class_scales_up_when_smaller(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(current_class="db.r8g.16xlarge")
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_scale_cfg(), "20260709")
    modify = dict(fake.calls)["modify"]
    assert modify["DBInstanceClass"] == "db.r8g.48xlarge"
    assert modify["DBInstanceIdentifier"] == "gp-people-db-20260709-dev-writer"
    assert modify["ApplyImmediately"] is True


def test_ensure_instance_class_noop_when_already_index_class(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(current_class="db.r8g.48xlarge")
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_scale_cfg(), "20260709")
    assert not any(op == "modify" for op, _ in fake.calls)  # already scaled => no modify


def test_ensure_instance_class_retries_after_in_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(current_class="db.r8g.16xlarge", raise_once="InvalidDBInstanceStateFault")
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_scale_cfg(), "20260709")
    modifies = [kw for op, kw in fake.calls if op == "modify"]
    assert len(modifies) == 2  # first raised the fault, settled, then re-issued
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_build_indexes.py -k ensure_instance_class -v`
Expected: FAIL — `AttributeError: module ... has no attribute '_ensure_instance_class'` (and `step.rds` not present to patch).

- [ ] **Step 3: Add imports to build_indexes.py**

At `build_indexes.py:27-31`, add `ClientError` and `rds`:

```python
import psycopg
from botocore.exceptions import ClientError

from loader.core.aws import rds
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, open_new_tunnel
```

- [ ] **Step 4: Add the `_ensure_instance_class` helper**

Add above `def run(` (i.e. before `build_indexes.py:225`):

```python
def _ensure_instance_class(cfg: LoaderConfig, run_date: str) -> None:
    """Scale the writer up to the index-build instance class if it is not already there.

    provision/create_schema/copy run on the smaller `load_instance_class`; build_indexes is the
    only CPU-bound step and needs the large `index_instance_class`. Mirror resize's modify + waiter,
    including tolerating a still-in-progress modify from a partial re-run (InvalidDBInstanceStateFault
    -> settle via the instance waiter -> re-issue). Idempotent: when the box is already the index
    class this is a no-op describe, so per-date re-runs are safe.
    """
    instance_id = cfg.new_writer_instance_id(run_date)
    target = cfg.index_instance_class
    rds_client = rds(cfg)
    current = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0][
        "DBInstanceClass"
    ]
    if current == target:
        log.info("indexes.instance_class_ok", instance=instance_id, instance_class=target)
        return

    waiter = rds_client.get_waiter("db_instance_available")

    def _wait() -> None:
        waiter.wait(DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    def _modify() -> None:
        rds_client.modify_db_instance(
            DBInstanceIdentifier=instance_id, DBInstanceClass=target, ApplyImmediately=True
        )

    log.info("indexes.scale_up", instance=instance_id, from_class=current, to_class=target)
    try:
        _modify()
    except ClientError as e:
        # Only tolerate a still-in-progress modify from a partial re-run; re-issue after it settles
        # so our class is actually applied. A genuine bad state re-raises on the retry.
        if e.response["Error"]["Code"] != "InvalidDBInstanceStateFault":
            raise
        log.warning("indexes.scale_up_retry_after_settle")
        _wait()
        _modify()
    _wait()
    log.info("indexes.scale_up_applied", instance=instance_id, instance_class=target)
```

- [ ] **Step 5: Call it in `run()`**

At `build_indexes.py:234-235`, add the call right after `log.info("indexes.start")`:

```python
    started = datetime.now(UTC)
    log.info("indexes.start")

    # Scale the writer up to the index box before any build work; provision/copy ran on the
    # smaller load box. No-op on a re-run where the box is already scaled.
    _ensure_instance_class(cfg, run_date)

    pk = primary_key_for(_TARGET_TABLE)
```

- [ ] **Step 6: Fix the stale resize docstring**

At `resize.py:3`, change the reference so it no longer says the load instance is db.r7g:

```python
"""Step 6 — resize the loaded cluster to Serverless v2 + serve params, lock down (DATA-1854).

After the load/index phase on the provisioned load instance, flip the writer to
```

- [ ] **Step 7: Run the full loader suite + lint to verify pass**

Run: `uv run pytest tests/test_build_indexes.py tests/test_resize.py -v && uv run ruff check src tests && uv run ruff format --check src tests && uv run ty check`
Expected: PASS (all green, ruff + format + ty clean).

- [ ] **Step 8: Commit**

```bash
git add src/loader/people_api/steps/build_indexes.py src/loader/people_api/steps/resize.py tests/test_build_indexes.py
git commit -m "feat(loader): scale writer up to the index box inside build_indexes

provision/copy run on the smaller load box; build_indexes scales the writer
to index_instance_class before building, resize flips it down to serverless.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run the whole people-api-loader suite + all lint gates**

Run: `uv run pytest -q && uv run ruff check && uv run ruff format --check && uv run ty check`
Expected: all tests pass; ruff, format, and ty clean. This mirrors the CI job for `people-api-loader`.

- [ ] **Step 2: Confirm the behavior contract by eye**

Verify: `provision` still uses `cfg.load_instance_class` (unchanged), `build_indexes.run()` calls `_ensure_instance_class` before the tunnel/build block, and `resize` still targets `db.serverless`. No copy-parallelism or `_DEFAULT_BUILDERS` change slipped in.

---

## Self-Review

**Spec coverage:**
- Part 1 two-tier sizing → Task 1 (config split) + Task 3 (scale-up in build_indexes) + resize docstring fix (Task 3 Step 6). Copy left unchanged (Global Constraints + Task 4 Step 2). ✓
- Part 2 fill the box → Task 2 (`max_parallel_workers` 176, maintenance workers 16). ✓
- Idempotency/re-runs → Task 3 `_ensure_instance_class` no-op-when-equal test + in-progress-fault test. ✓
- Testing (unit only, no live infra) → Tasks 1-3 tests use monkeypatched `step.rds` and pure assertions. ✓
- Cost outcome / out-of-scope → not code; noted in spec. `_DEFAULT_BUILDERS` auto-derive left as follow-up (Global Constraints pins it at 128). ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code. ✓

**Type consistency:** `_ensure_instance_class(cfg, run_date) -> None`, `cfg.index_instance_class`, `cfg.new_writer_instance_id`, `step.rds` used identically across Task 3 tests and impl. Config field `index_instance_class` defined in Task 1 and consumed in Task 3. ✓
