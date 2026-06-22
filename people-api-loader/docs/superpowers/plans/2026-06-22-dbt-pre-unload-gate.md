# dbt Pre-Unload Gate Implementation Plan — DATA-1906

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the pre-unload data-quality gate on the people_api voter/district marts — most column tests already exist; add a row-count + state-coverage singular test and define the gate selection.

**Architecture:** dbt tests in `dbt/project`. The voter mart already has `not_null`+`unique` on `id`/`LALVOTERID`, `not_null`+`accepted_values` on `State`. The only new check is a singular test asserting the voter mart's total is above a floor and every state is present. The "gate" is `dbt test --select m_people_api__voter m_people_api__district` (all their tests, severity error), run before unload.

**Tech Stack:** dbt (dbt Cloud CLI — do NOT invoke via uv), the project's existing macros (`get_us_states_list`) and singular-test convention under `dbt/project/tests/`.

**Branch:** `feat/DATA-1906-dbt-unload-gate` (off latest `main`). This touches only `dbt/`. CI is the dbt project's own workflow.

**Spec:** `docs/superpowers/specs/2026-06-22-dbt-pre-unload-gate-design.md`

---

## Scope discovery (important)

`dbt/project/models/marts/people_api/m_people_api.yaml` ALREADY has, on `m_people_api__voter`:
`id` (not_null, unique), `LALVOTERID` (not_null, unique), `State` (not_null, accepted_values via
`get_us_states_list(include_DC=true, include_US=true)`), `Voter_Status` (not_null, accepted_values).
And `m_people_api__district` has `id`/`name`/`type` not_null + `id` unique. dbt's `unique` is a cheap
`GROUP BY … HAVING count>1` aggregation (not a self-join), so the existing exact tests are fine at 218M —
**do not change them to sampled.** The only genuinely new work is the row-count/state-coverage gate.

---

## Task 1: Row-count + state-coverage singular test

**Files:** Create `dbt/project/tests/assert_people_api_voter_rowcount_and_state_coverage.sql`.

A dbt singular test passes when it returns **zero rows**. We return rows describing any violation:
(a) total below a conservative floor, or (b) any of the 51 expected state codes missing from the mart.

- [ ] **Step 1: Write the test SQL** — `dbt/project/tests/assert_people_api_voter_rowcount_and_state_coverage.sql`:

```sql
-- DATA-1906 pre-unload gate: the voter mart must have a plausible row count and cover every state.
-- Returns one row per violation (singular test fails iff any rows returned).
-- Floor is a conservative band under the inspected ~218M; tune if the source grows materially.
{% set row_floor = 200000000 %}

with
    summary as (
        select count(*) as total_rows, count(distinct `State`) as distinct_states
        from {{ ref('m_people_api__voter') }}
    ),
    expected_states as (
        select explode(array({{ "'" ~ get_us_states_list(include_DC=true, include_US=false) | join("','") ~ "'" }})) as state
    ),
    present_states as (select distinct `State` as state from {{ ref('m_people_api__voter') }}),
    missing_states as (
        select e.state from expected_states e
        left join present_states p on p.state = e.state
        where p.state is null
    ),
    violations as (
        select 'row_count_below_floor' as violation,
               cast(total_rows as string) as detail
        from summary where total_rows < {{ row_floor }}
        union all
        select 'missing_state' as violation, state as detail from missing_states
    )
select * from violations
```

Notes:
- `get_us_states_list(include_DC=true, include_US=false)` → the 51 codes (50 + DC), the partition set the
  unload produces. (The `State` accepted_values test uses `include_US=true`; coverage here is the 51.)
- If the `get_us_states_list` macro's return shape doesn't render cleanly into a SQL array literal,
  fall back to a hardcoded `array('AL','AK',...,'DC')` of the 51 codes (must match
  `people-api-loader/src/loader/people_api/schema/states.py`). Confirm the macro output first.
- Spark SQL `explode(array(...))` produces one row per expected state; adjust to the project's
  Databricks SQL dialect if `explode` isn't the convention used elsewhere in `dbt/project/tests/`.

- [ ] **Step 2: Verify the test parses + fails closed.** Run the project's dbt test for this model
  (dbt Cloud CLI, per `dbt/CLAUDE.md` — `dbt test --select m_people_api__voter`, NOT via uv). Temporarily
  set `row_floor` absurdly high (e.g. `999999999999`) and confirm the test FAILS (returns the
  `row_count_below_floor` row); then restore the floor and confirm it PASSES. This proves the gate
  fails closed.

- [ ] **Step 3: Commit**
```bash
git add dbt/project/tests/assert_people_api_voter_rowcount_and_state_coverage.sql
git commit -m "test(dbt): pre-unload row-count + state-coverage gate on the voter mart"
```

---

## Task 2: Document the gate selection

**Files:** Modify `dbt/project/models/marts/people_api/m_people_api.yaml` (a short comment) and confirm no
model changes are needed.

- [ ] **Step 1:** Add a comment at the top of the `m_people_api.yaml` models list documenting the gate:
```yaml
# DATA-1906 pre-unload gate: run `dbt test --select m_people_api__voter m_people_api__district`
# (all severity=error) before the loader `unload` step. Covers the column tests below plus the
# singular row-count/state-coverage test in tests/assert_people_api_voter_rowcount_and_state_coverage.sql.
# The Airflow DAG (DATA-1913) and runbook (DATA-1914) sequence this selection ahead of unload.
```

- [ ] **Step 2:** Confirm the existing voter/district column tests are severity `error` (dbt's default
  for schema tests is `error` unless `config: {severity: warn}` is set — verify none are downgraded).
  No change if already default.

- [ ] **Step 3: Commit**
```bash
git add dbt/project/models/marts/people_api/m_people_api.yaml
git commit -m "docs(dbt): document the people_api pre-unload gate selection"
```

---

## Task 3: Final check + push

- [ ] **Step 1:** From `dbt/`, run the gate selection end to end (dbt Cloud CLI):
  `dbt test --select m_people_api__voter m_people_api__district` → all pass against current marts.
- [ ] **Step 2:** `pre-commit run --all-files` from repo root (sqlfmt will format the singular test SQL;
  re-add if it reformats).
- [ ] **Step 3: Push + PR**
```bash
git push -u origin feat/DATA-1906-dbt-unload-gate
```
  Open the PR (touches only `dbt/`); the dbt Cloud check runs the project tests.

---

## Notes for the implementer

- **dbt invocation:** use the system dbt Cloud CLI directly (`dbt test ...`), never `uv run dbt` — see
  `dbt/CLAUDE.md` and the repo root `CLAUDE.md` "Never invoke dbt via uv".
- **Don't touch mart model logic** — tests only.
- **Don't change the existing exact `unique` tests to sampled** — they're cheap aggregations and already ship.
- The floor (200M) is intentionally conservative against the inspected ~218M; the singular test comment
  says to tune it if the source grows materially.
