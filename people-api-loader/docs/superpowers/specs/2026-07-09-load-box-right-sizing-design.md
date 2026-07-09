# Right-size the load box per phase + fill it during index build

Date: 2026-07-09
Ticket: DATA-1913
Status: approved, pending implementation

## Problem

The loader provisions one instance class (`DEFAULT_LOAD_INSTANCE_CLASS = db.r8g.48xlarge`,
192 vCPU) and rides it through the entire load phase: `provision` -> `create_schema` ->
`copy` -> `build_indexes`, then `resize` flips the writer to Serverless v2. Only
`build_indexes` is CPU-bound and needs 192 vCPU. `provision` and `create_schema` are
trivial; `copy` is I/O / WAL / storage-bound. So the expensive box is paid for in full
during phases that cannot use it.

Worse, even during `build_indexes` the box is underused. Live `pg_stat_activity` on a
running build (r8g.48xlarge, 192 vCPU) showed 33 leader backends + 92 parallel workers =
125 active, with 124 of 126 active backends on CPU (null wait event) and no lock/LWLock
contention. ~67 cores sat idle. The cap is `max_parallel_workers = 96` (Aurora default,
~vCPU/2); `max_worker_processes` is 384 (ample) and the parameter groups are provisioned
empty, so the cluster runs on defaults. The 96-worker pool ceiling is why the box tops out
at ~125 active on a 192-vCPU instance.

## Goal

The 192-vCPU box should exist only while `build_indexes` runs, and while it runs it should
use all 192 vCPU rather than ~125.

## Design

### Part 1 — Two-tier instance sizing

**config.py.** Split the single load class into two:

- `DEFAULT_LOAD_INSTANCE_CLASS = "db.r8g.16xlarge"` (64 vCPU) — the box `provision`
  creates; carries `provision`, `create_schema`, `copy`. Env override
  `LOADER_LOAD_INSTANCE_CLASS` (existing name, unchanged).
- `DEFAULT_INDEX_INSTANCE_CLASS = "db.r8g.48xlarge"` (192 vCPU) — `build_indexes` scales
  up to this. New env override `LOADER_INDEX_INSTANCE_CLASS`. `LoaderConfig` gains an
  `index_instance_class` field parsed the same way as `load_instance_class`.

**build_indexes.run().** After the manifest short-circuit and before the PK/index work,
ensure the writer instance class equals `index_instance_class`:

1. Describe the writer instance's current `DBInstanceClass`.
2. If it already equals `index_instance_class`, skip (no-op — makes re-runs idempotent).
3. Otherwise `modify_db_instance(DBInstanceIdentifier=..., DBInstanceClass=index_class,
   ApplyImmediately=True)` and wait for `db_instance_available`.

Mirror `resize`'s `_modify_settle` + waiter pattern, including tolerating an in-progress
modify from a partial re-run (`InvalidDBInstanceStateFault` -> settle via the instance
waiter -> re-issue). Do not swallow-and-skip a genuine bad state.

**resize.py.** Behavior unchanged (still flips to Serverless v2 afterward). Fix the stale
docstring/comment that references "db.r7g".

**copy.** No change. Keep `_DEFAULT_PARALLELISM = 128`. Copy is I/O/WAL-bound, so high
concurrency overlaps I/O waits; the smaller box simply stops paying for cores that were
already idle. (Explicitly NOT lowering copy parallelism — that would only help a
CPU-bound copy, the opposite of the premise.)

### Part 2 — Fill the box in build_indexes

In `_BUILD_SESSION_SQL`:

- Add `SET max_parallel_workers = 176` (raise from the Aurora default 96). At the tail,
  ~33 leaders + up to ~176 workers keeps the 192 vCPU full instead of capping at ~125.
  `max_parallel_workers` is a user-context GUC, so a per-session `SET` in every builder
  session raises the effective ceiling; `max_worker_processes` (384) is ample, so no
  reboot-class parameter change is needed.
- Raise `SET max_parallel_maintenance_workers` from 8 to 16, so the long-pole giant
  partition builds (common columns on big states: CA/TX/FL/NY) spread wider and the
  absolute tail shrinks.

Both values are tuned for the 192-vCPU index box and want confirmation on the next clean
cold-date run. `_DEFAULT_BUILDERS` stays 128 (right for 192 vCPU in the bulk phase, where
builds are tiny and use ~1 core each).

## Idempotency / re-runs

- `build_indexes`'s completed-manifest short-circuit is unchanged; the scale-up only runs
  when the step actually runs.
- The ensure-class step is a no-op describe when the box is already scaled.
- The existing PK-exists precheck (42P16 guard) is unchanged.

## Testing

Unit tests only, no live infra:

- `build_indexes` scales up when the current class differs from `index_instance_class`.
- `build_indexes` skips the modify when the class already matches.
- `build_indexes` tolerates an in-progress modify (fault -> settle -> retry), mirroring
  the `resize` tests.
- `config` parses both classes and both env overrides (`LOADER_LOAD_INSTANCE_CLASS`,
  `LOADER_INDEX_INSTANCE_CLASS`), including defaults.

ruff + ty + format clean (Astral toolchain, line length 110).

## Cost outcome

The 192-vCPU box lives only for `build_indexes` wall-clock. `provision` / `create_schema`
/ `copy` run on 64 vCPU (~1/3 the hourly rate of the 48xlarge). During `build_indexes` the
box runs full instead of ~65% utilized, so the tail is shorter — itself fewer expensive
instance-hours. Since `resize` swaps to Serverless immediately after, shrinking the
build_indexes wall-clock maps directly to dollars.

## Out of scope / follow-ups

- The currently running job cannot be retuned: `max_parallel_workers` and the
  maintenance-worker count latch at each `CREATE INDEX` start, so this change lands for the
  next run. The live tail is left to finish.
- Auto-deriving `_DEFAULT_BUILDERS` and the worker pool from the instance's vCPU count is a
  real improvement but stays a noted follow-up (matches the existing todo), to keep this
  change focused.
