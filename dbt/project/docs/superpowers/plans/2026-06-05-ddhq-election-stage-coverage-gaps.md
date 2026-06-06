# DDHQ election_stage coverage gaps — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dbt diagnostic model listing every BallotReady local 2026+ election_stage that has no matched DDHQ counterpart.

**Architecture:** A single `view` model in `models/intermediate/civics/`. It scopes BR election_stages (2026+, non-state/non-federal), determines DDHQ/TS matches from the `int__civics_er_canonical_election_stages` crosswalk via null-safe left-joins (anti-join for the DDHQ gap), and enriches with a TS-match flag and BR candidacy count.

**Tech Stack:** dbt (Databricks/Spark SQL), dbt Cloud CLI, dev schema `dbt_hugh`. Commit via `cd dbt && poetry run git ...` (pre-commit pytest/sqlfmt hooks). Branch: `data-1582/ddhq-election-stage-coverage-gaps`.

Spec: `docs/superpowers/specs/2026-06-05-ddhq-election-stage-coverage-gaps-design.md`

---

### Task 1: Create the coverage-gaps model

**Files:**
- Create: `dbt/project/models/intermediate/civics/int__civics_ddhq_election_stage_coverage_gaps.sql`

- [ ] **Step 1: Write the model**

Create `int__civics_ddhq_election_stage_coverage_gaps.sql` with exactly:

```sql
{{ config(materialized="view", tags=["civics", "entity_resolution"]) }}

-- Diagnostic: BallotReady local election_stages (election_date >= 2026-01-01)
-- with NO matched DDHQ counterpart, per the entity-resolution crosswalk
-- int__civics_er_canonical_election_stages. Used to triage gaps in DDHQ
-- election coverage. Grain: one row per BR gp_election_stage_id with no DDHQ
-- match.
--
-- NOTE: in dev the crosswalk reflects the OR-baseline matcher (no anchors), so
-- dev counts overstate the gap; self-corrects once prod's crosswalk is rebuilt
-- from the final-config matcher. See the design doc.
with
    br_position as (
        select database_id as br_position_id, state
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    -- In-scope BR races: local (non-state/non-federal) stages on/after 2026-01-01.
    br_local as (
        select
            es.gp_election_stage_id,
            es.br_race_id,
            bp.state,
            {{ strip_office_name_state_prefix("es.race_name") }}
            as official_office_name,
            es.candidate_office,
            es.office_level,
            es.office_type,
            try_cast(
                regexp_extract(es.district, '([0-9]+)') as int
            ) as district_identifier,
            es.seat_name,
            es.election_date,
            es.stage_type
        from {{ ref("int__civics_election_stage_ballotready") }} as es
        left join br_position as bp on es.br_position_id = bp.br_position_id
        where
            es.election_date >= '2026-01-01'
            and es.office_level not in ('State', 'Federal')
    ),

    -- BR races that DID get a DDHQ / TS match: their gp_election_stage_id is the
    -- canonical of a DDHQ / TS crosswalk row (BR-anchored clusters).
    ddhq_matched as (
        select distinct canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_election_stages") }}
        where source_name = 'ddhq' and canonical_gp_election_stage_id is not null
    ),

    ts_matched as (
        select distinct canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_election_stages") }}
        where source_name = 'techspeed' and canonical_gp_election_stage_id is not null
    ),

    -- BR candidacies per race (the candidate count on the BR side).
    br_candidacy_counts as (
        select br_race_id, count(distinct gp_candidacy_id) as br_candidate_count
        from {{ ref("int__civics_candidacy_ballotready") }}
        where br_race_id is not null
        group by br_race_id
    )

select
    b.gp_election_stage_id,
    b.br_race_id,
    b.state,
    b.official_office_name,
    b.candidate_office,
    b.office_level,
    b.office_type,
    b.district_identifier,
    b.seat_name,
    b.election_date,
    b.stage_type,
    ts.canonical_gp_election_stage_id is not null as has_techspeed_match,
    coalesce(cc.br_candidate_count, 0) as br_candidate_count
from br_local as b
left join ddhq_matched as dq on b.gp_election_stage_id = dq.canonical_gp_election_stage_id
left join ts_matched as ts on b.gp_election_stage_id = ts.canonical_gp_election_stage_id
left join br_candidacy_counts as cc on b.br_race_id = cc.br_race_id
-- Null-safe anti-join: keep only BR races with NO DDHQ match.
where dq.canonical_gp_election_stage_id is null
```

- [ ] **Step 2: Build the model**

Run: `cd dbt/project && dbt run --select int__civics_ddhq_election_stage_coverage_gaps`
Expected: `Completed successfully` / `PASS=1`.

- [ ] **Step 3: Inspect grain + counts**

Run:
```
dbt show --inline "select count(*) rows, count(distinct gp_election_stage_id) distinct_pk, count_if(office_level in ('State','Federal')) bad_level, count_if(election_date < '2026-01-01') bad_date, count_if(has_techspeed_match) ts_covered from {{ ref('int__civics_ddhq_election_stage_coverage_gaps') }}" --limit 5
```
Expected: `rows == distinct_pk` (1:1 grain), `bad_level = 0`, `bad_date = 0`. (In dev, `rows` will be inflated vs prod because the crosswalk is OR-baseline — that's expected per the spec.)

- [ ] **Step 4: Validate the gap logic with a spot check**

Run (a BR race that IS DDHQ-matched must NOT appear in the output):
```
dbt show --inline "with m as (select distinct canonical_gp_election_stage_id id from {{ ref('int__civics_er_canonical_election_stages') }} where source_name='ddhq' and canonical_gp_election_stage_id is not null) select count(*) leaked_matched_rows from {{ ref('int__civics_ddhq_election_stage_coverage_gaps') }} g join m on g.gp_election_stage_id = m.id" --limit 3
```
Expected: `leaked_matched_rows = 0` (no DDHQ-matched race leaks into the gap list).

- [ ] **Step 5: Commit**

```bash
cd dbt && poetry run git add project/models/intermediate/civics/int__civics_ddhq_election_stage_coverage_gaps.sql && poetry run git commit -m "feat(data-1582): DDHQ election_stage coverage-gaps diagnostic model"
```
(If sqlfmt reformats and aborts, re-`add` the file and re-run the commit.)

---

### Task 2: Add schema docs + tests

**Files:**
- Modify: `dbt/project/models/intermediate/civics/int__civics.yaml` (the civics intermediate schema file; append a new `- name:` entry under `models:`)

- [ ] **Step 1: Add the model entry**

Append this entry to the `models:` list in `int__civics.yaml`:

```yaml
  - name: int__civics_ddhq_election_stage_coverage_gaps
    description: >
      Diagnostic: BallotReady local election_stages (election_date >= 2026-01-01,
      non-state/non-federal) that have no matched DDHQ counterpart per the
      int__civics_er_canonical_election_stages crosswalk. One row per BR
      gp_election_stage_id with no DDHQ match. NOTE: dev counts overstate the
      gap (crosswalk is OR-baseline in dev); self-corrects in prod.
    columns:
      - name: gp_election_stage_id
        description: BallotReady race/stage id (primary key).
        tests:
          - unique
          - not_null
      - name: br_race_id
        description: BallotReady race id.
        tests:
          - not_null
      - name: has_techspeed_match
        description: True if TechSpeed covered this race even though DDHQ did not.
      - name: br_candidate_count
        description: Number of BR candidacies on the race.
```

- [ ] **Step 2: Run the tests**

Run: `cd dbt/project && dbt test --select int__civics_ddhq_election_stage_coverage_gaps`
Expected: all tests `PASS` (unique + not_null on `gp_election_stage_id`, not_null on `br_race_id`).

- [ ] **Step 3: Commit**

```bash
cd dbt && poetry run git add project/models/intermediate/civics/int__civics.yaml && poetry run git commit -m "test(data-1582): schema docs + tests for DDHQ coverage-gaps model"
```

---

### Task 3: Open the PR

- [ ] **Step 1: Push and open the PR**

```bash
cd dbt && poetry run git push -u origin data-1582/ddhq-election-stage-coverage-gaps
gh pr create --repo thegoodparty/gp-data-platform --base main \
  --title "[DATA-1582] DDHQ election_stage coverage-gaps diagnostic" \
  --body "Adds int__civics_ddhq_election_stage_coverage_gaps: BR local 2026+ election_stages with no matched DDHQ counterpart (per the ER crosswalk). Diagnostic model for triaging DDHQ coverage gaps. Spec: docs/superpowers/specs/2026-06-05-ddhq-election-stage-coverage-gaps-design.md"
```
Expected: PR URL printed.

---

## Notes / gotchas

- The BR election_stage model has `race_name`/`district`, not `official_office_name`/`district_identifier` — derived here via `strip_office_name_state_prefix` + `regexp_extract`, matching `int__er_prematch_election_stages`.
- `state` is not native on the BR stage model; joined from `stg_airbyte_source__ballotready_api_position` on `br_position_id`.
- Null-safe anti-join (left join + `IS NULL`) avoids the `NOT IN` null trap called out in `dbt/project/CLAUDE.md`.
- Commits must go through `cd dbt && poetry run git ...` so the pre-commit pytest/sqlfmt hooks find their deps.
