# dbt pre-unload data-quality gate — DATA-1906

**Status:** approved design, pre-implementation
**Date:** 2026-06-22
**Relates to:** DATA-1640, DATA-1906, DATA-1908 (unload — gated by this), DATA-1913 (DAG sequences it
ahead of unload).
**Implementation location:** `dbt/project/` (not the loader). Spec co-located with the epic's other
design docs for cohesion.

## Problem

`unload` exports the people_api marts to prod. A truncated, deduped-wrong, or mis-stated mart would
propagate silently into the served voter DB. DATA-1906 adds a data-quality gate on the voter + district
marts that must pass (severity `error`) before unload runs, so bad data is caught in Databricks rather
than after a multi-hour load.

## Decisions (settled during brainstorming)

Gate the marts on all four check classes, severity `error`:
1. **not_null** on key columns.
2. **uniqueness** of the natural/primary keys.
3. **accepted_values** on `State` (the 51 valid codes).
4. **row-count thresholds** (total within a sane band; every state non-empty).

## Scope

- `m_people_api__voter` (~218M rows, tagged `monthly`, ~350 cols) and `m_people_api__district` (small).
  These are the two the ticket names. `m_people_api__district{voter,stats}` already have some tests in
  `m_people_api.yaml` and are out of this ticket's scope except where a check naturally extends.

## Checks (extend `dbt/project/models/marts/people_api/m_people_api.yaml`)

**`m_people_api__voter`** (add a model block if absent):
- `not_null`: `id`, `LALVOTERID`, `State`.
- **uniqueness — sampled, not exact.** At 218M rows an exact `unique` test is prohibitively expensive,
  so use the repo's existing `unique_combination_sampled` pattern (already used for
  `m_people_api__districtvoter`) on `LALVOTERID` (the dedup key) with a large `sample_size`. Document
  the cost rationale inline.
- `accepted_values` on `State`: the 51 codes (50 states + DC), matching the loader's `schema/states.py`
  `STATES`. Severity `error`.

**`m_people_api__district`** (small — exact tests are cheap):
- Confirm/keep `not_null` + `unique` on `id`, and `not_null` on `name`/`type` (most already present).

**Row-count + state-coverage gate** — a custom singular test (the repo already keeps singular tests under
`dbt/project/tests/`, e.g. `assert_*`):
- `tests/assert_people_api_voter_rowcount_and_state_coverage.sql`: fails if the voter mart total is below
  a floor (a conservative band around the inspected ~218M — e.g. `< 200_000_000`, tunable) **or** any of
  the 51 `State` codes is missing / has zero rows. One cheap `count(*)` + `group by State`. Severity `error`.

## Gate wiring

- Tag every check above `people_api_unload_gate` so the orchestrator runs exactly the gate:
  `dbt test --select tag:people_api_unload_gate`. A non-zero exit (any `error`-severity failure) blocks
  `unload`. The DAG (DATA-1913) and the runbook (DATA-1914) sequence this test selection immediately
  before the unload task; this spec only defines the tests + tag, not the DAG.
- The gate is independent of the loader CLI — it's dbt, run before the loader's `unload` step.

## Error handling / cost

- All gate tests are severity `error` (block), not `warn`.
- Cost: not_null + the row-count/state-coverage test are single full scans (acceptable once per monthly
  refresh); uniqueness is **sampled** to avoid a full self-join over 218M rows. District tests are exact
  (small table).

## Testing

- dbt tests are themselves the verification; validate by running `dbt build`/`dbt test --select
  tag:people_api_unload_gate` against the marts (CI runs the dbt project's own workflow). Add a
  deliberately-failing local check (e.g. a lowered floor) once to confirm the singular test actually
  fails closed, then revert.

## Non-goals

- No changes to the mart models' logic — tests only.
- Wiring the gate into the Airflow DAG is DATA-1913; documenting it in the runbook is DATA-1914.
