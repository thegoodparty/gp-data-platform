# DATA-1894 Archive Position Null-Outs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `br_position_database_id` trustworthy and unique in the 2025 election archive by nulling it on mis-tagged and duplicate-variant rows via a committed, human-reviewed seed.

**Architecture:** A one-time classification of the frozen 2025-archive collision set produces a CSV seed (`seed_civics_election_2025_position_nullouts`) listing every `gp_election_id` whose `br_position_database_id` should be nulled, with a reason. `int__civics_election_2025` applies it with a single `case` expression at the point the position id is selected. A `unique_combination_of_columns` test on that model guarantees the archive stays clean.

**Tech Stack:** dbt Cloud CLI (Databricks), dbt seeds, `dbt_utils`, `dbt_expectations`.

## Global Constraints

- dbt Cloud CLI only. Do **not** invoke `dbt` via `uv`. Run dbt from `dbt/project/`.
- Do **not** use the `+` upstream selector during development. List only modified models.
- Commit from the `dbt/` directory (`cd dbt && git commit ...`) so pre-commit hooks find their envs.
- Branch: `data-1894/archive-position-nullouts` (already created). PR title prefix `[DATA-1894]`.
- Generic test args go under `arguments:`. Custom config keys go under `config.meta`.
- Booleans referenced explicitly (`where is_x`, not `is_x = 'true'`).
- Do not use real people's names in any committed artifact (code, CSV audit columns, commit messages). Office names and jurisdiction names are fine; candidate names are not — the seed's audit columns use office/position strings only.
- Seeds live in `dbt/project/seeds/` and are documented in `dbt/project/seeds/seeds_schema.yaml`.
- Databricks supports lateral column references; referencing the `gp_election_id` alias inside the same SELECT is allowed.
- All work is against the 2025 archive only. The 2026 BR/TS transient-staleness split is explicitly out of scope.

---

### Task 1: Verify plan-time assumptions (go/no-go gate)

No code. This task confirms the two assumptions the whole seed approach rests on. If assumption 1 fails, stop and revisit the design.

**Files:** none (findings recorded in the PR description).

- [ ] **Step 1: Confirm the archive `gp_election_id` is stable (frozen source)**

Read the lineage of `int__civics_election_2025`'s contest input:

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform/dbt/project
grep -n "ref(\|source(" models/intermediate/civics/int__hubspot_contest_2025.sql
```

Confirm it reads a static/archived snapshot (model header says "2026-01-22 snapshot"), not a daily-refreshed source. Expected: the upstream is a frozen archive, so `generate_gp_election_id("tbl_contest")` output does not drift between runs.

- [ ] **Step 2: Confirm the null is harmless downstream**

Verify the mart's enrichment joins on `br_position_database_id` are LEFT joins (so nulled rows simply get null enrichment, not dropped):

```bash
grep -n "br_position_database_id" models/marts/civics/election.sql
```

Expected: `int__icp_offices` and `int__civics_position_office_type` are joined with `left join ... on deduplicated.br_position_database_id = ...`. Nulled rows get null ICP / office-type. Confirm no `inner join` on that key anywhere downstream:

```bash
grep -rln "br_position_database_id" models/marts | while read f; do grep -q "inner join" "$f" && echo "CHECK: $f"; done
```

- [ ] **Step 3: Record findings**

Write both confirmations (or a blocker) into the PR description. Proceed only if both hold.

---

### Task 2: Build and commit the seed

Produces the classified null-out seed and registers it in dbt. The seed is inert until Task 3 consumes it, so it is a clean standalone commit.

**Files:**
- Create: `dbt/project/seeds/seed_civics_election_2025_position_nullouts.csv`
- Modify: `dbt/project/seeds/seeds_schema.yaml` (append seed docs + tests)
- Scratch (NOT committed): extraction/classification artifacts under the scratchpad dir

**Interfaces:**
- Produces: seed `seed_civics_election_2025_position_nullouts` with columns
  `gp_election_id` (string), `br_position_database_id` (bigint), `election_date` (date),
  `reason` (string: `office_mismatch` | `duplicate_variant`), `contest_office` (string),
  `br_position_name` (string). Task 3 selects `gp_election_id` from it.

- [ ] **Step 1: Extract the collision set to JSON (scratch)**

Run from `dbt/project/`:

```bash
dbt show --output json --limit 5000 --inline "
with dupes as (
  select br_position_database_id, election_date
  from {{ ref('int__civics_election_2025') }}
  where br_position_database_id is not null
  group by 1,2 having count(*) > 1
)
select
  e.br_position_database_id, e.election_date, e.gp_election_id,
  e.official_office_name, e.candidate_office,
  e.office_type  as contest_office_type,
  e.office_level as contest_office_level,
  e.district, e.seat_name,
  e.br_normalized_position_type as br_truth_position_type,
  e.has_ddhq_match, e.updated_at
from {{ ref('int__civics_election_2025') }} e
join dupes using (br_position_database_id, election_date)
order by e.br_position_database_id, e.election_date, e.gp_election_id
" > /private/tmp/claude-501/-Users-hugh-Documents-repos-2/590e2cd6-4680-4874-9ab2-1e93f7b23eb8/scratchpad/collisions.json
```

Expected: ~2,533 rows across ~1,156 `(br_position_database_id, election_date)` groups. If `--output json` is unsupported by the installed CLI version, materialize a temporary view instead and export from Databricks; do not commit the temp view.

- [ ] **Step 2: Classify each row as match / no-match against its BR position**

For every collision group, decide for each row whether the contest's real office matches
`br_truth_position_type` (constant within the group — it is the BR position's true office).
Use this rubric, in order:

1. If `contest_office_type` is present and not `'Other'`: it matches when it denotes the same
   office as `br_truth_position_type` (e.g. contest `State Senate` ~ BR `State Senator` = match;
   contest `State House` vs BR `State Senator` = no-match).
2. If `contest_office_type` is `'Other'` or null: compare `official_office_name` (fall back to
   `candidate_office`) against `br_truth_position_type` (e.g. official `Rochester City Select
   Board` vs BR `Local School Board` = no-match; `Rochester School Board` = match).
3. Ignore `candidate_office` free text when it conflicts with a clear `contest_office_type`
   (it is noisy: e.g. `U.S. Senate - Mississippi` free text on a row whose
   `contest_office_type` is `State Senate` is a match to a State Senate position).

Emit a JSON/CSV with `gp_election_id, is_match (bool), confidence ('high'|'low')`. Batch the
~1,156 groups (e.g. 50 groups per call) to keep each classification prompt small. Mark
`confidence='low'` whenever the rubric relied on step 2 (name text) or the group has no
matching row.

- [ ] **Step 3: Human review of low-confidence and conflict groups**

Review every `confidence='low'` row, plus any group where zero rows matched or two or more
*distinct* offices matched (a BR position is a single office, so multi-office matches signal a
classification error). Correct `is_match` in place. This is the authoritative human gate.

- [ ] **Step 4: Assemble the seed deterministically**

Given the reviewed `is_match` labels, pick the winner per group and assign reasons. The winner
is the single matched row that keeps its tag; everyone else is nulled:

- Winner = the `is_match = true` row ranked first by:
  `official_office_name is not null desc`, then `has_ddhq_match desc`, then `updated_at desc`.
- Every `is_match = true` non-winner → `reason = 'duplicate_variant'`.
- Every `is_match = false` row → `reason = 'office_mismatch'`.
- Groups with no matched row → all rows `office_mismatch` (no winner; the tag was wrong for all).

Write the CSV with exactly these columns and no candidate names in `contest_office`
(use `official_office_name`, or `contest_office_type` when the name is null):

```
gp_election_id,br_position_database_id,election_date,reason,contest_office,br_position_name
```

Expected row count ≈ 1,377 (2,533 collision rows minus ~1,156 winners). Save as
`dbt/project/seeds/seed_civics_election_2025_position_nullouts.csv`.

- [ ] **Step 5: Document the seed in `seeds_schema.yaml`**

Append to `dbt/project/seeds/seeds_schema.yaml`:

```yaml
  - name: seed_civics_election_2025_position_nullouts
    description: >
      One-time resolution of 2025-archive br_position_database_id collisions
      (DATA-1894). Each row is a gp_election_id whose br_position_database_id
      must be nulled in int__civics_election_2025 because the contest's office
      does not match the tagged BallotReady position (office_mismatch) or because
      it is a same-office name-variant duplicate of the winning row
      (duplicate_variant). The 2025 archive is a frozen 2026-01-22 snapshot, so
      these gp_election_ids are stable.
    columns:
      - name: gp_election_id
        description: Archive election id whose position tag is nulled.
        data_tests:
          - unique
          - not_null
          - relationships:
              arguments:
                to: ref('int__civics_election_2025')
                field: gp_election_id
      - name: br_position_database_id
        description: BR position the row was tagged to (audit only).
        data_tests:
          - not_null
      - name: reason
        description: Why the tag is nulled.
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ["office_mismatch", "duplicate_variant"]
      - name: br_position_name
        description: Human-readable BR position name (documentation only).
```

- [ ] **Step 6: Build the seed and run its tests**

```bash
dbt build --select seed_civics_election_2025_position_nullouts
```

Expected: seed loads; `unique`, `not_null`, `accepted_values`, and the `relationships` test to
`int__civics_election_2025` all PASS (the relationships test passes because every seed id was
extracted from that model). If relationships fails, a seed id has rotted — recheck Task 1 Step 1.

- [ ] **Step 7: Commit**

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform/dbt
git add project/seeds/seed_civics_election_2025_position_nullouts.csv project/seeds/seeds_schema.yaml
git commit -m "$(cat <<'EOF'
[DATA-1894] Add 2025-archive position null-out seed

Human-reviewed resolution of br_position_database_id collisions in the
frozen 2025 election archive: office_mismatch + duplicate_variant rows.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

> **Revision 2026-07-09 (hybrid).** Implementation found the archive `gp_election_id` drifts
> ~1.7% across builds (live SCD snapshot), so a pure `gp_election_id` seed left residual
> collisions. Tasks 2-3 were revised: the seed carries **only** the ~78 `office_mismatch` rows
> (not `duplicate_variant`), and `int__civics_election_2025` gains a deterministic in-model
> `collapsed` CTE that keeps `br_position_database_id` on one completeness-winner per
> `(br_position_database_id, election_date)` and nulls the rest — making uniqueness a drift-proof
> build-time invariant. The seed `relationships` test is `warn`-level. Where the task text below
> conflicts (e.g. the two-value `accepted_values`, the ~1,377-row estimate), the revision governs.
> See the same-dated spec's "Revision" banner.

### Task 3: Apply the seed in the model and enforce uniqueness

Consumes the seed to null the tags, adds the guarding test, and proves the collisions are gone.

**Files:**
- Modify: `dbt/project/models/intermediate/civics/int__civics_election_2025.sql:36`
- Modify: `dbt/project/models/intermediate/civics/int__civics.yaml:35-38` (add test to the existing `tests:` block for `int__civics_election_2025`)

**Interfaces:**
- Consumes: `seed_civics_election_2025_position_nullouts.gp_election_id` from Task 2.

- [ ] **Step 1: Observe the failing invariant (red)**

Add the uniqueness test to the `tests:` block of `int__civics_election_2025` in
`models/intermediate/civics/int__civics.yaml` (currently lines 35-38):

```yaml
    tests:
      - dbt_utils.expression_is_true:
          arguments:
            expression: "updated_at >= created_at"
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [br_position_database_id, election_date]
          config:
            where: "br_position_database_id is not null"
```

Run it against the current (unfixed) model:

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform/dbt/project
dbt test --select int__civics_election_2025
```

Expected: the `unique_combination_of_columns` test FAILS (~1,156 violating groups). This is the
red state that proves the test detects the problem.

- [ ] **Step 2: Apply the seed in the model (green)**

In `int__civics_election_2025.sql`, replace line 36:

```sql
            tbl_candidacy.br_position_database_id,
```

with:

```sql
            case
                when gp_election_id in (
                    select gp_election_id
                    from {{ ref("seed_civics_election_2025_position_nullouts") }}
                )
                then null
                else tbl_candidacy.br_position_database_id
            end as br_position_database_id,
```

- [ ] **Step 3: Rebuild the model**

```bash
dbt run --select int__civics_election_2025
```

Expected: model builds successfully.

- [ ] **Step 4: Re-run the tests (verify green)**

```bash
dbt test --select int__civics_election_2025
```

Expected: `unique_combination_of_columns` now PASSES, and the existing `unique` / `not_null` /
`is_uuid` / `expression_is_true` tests still PASS (nulling a non-key column cannot affect them).

- [ ] **Step 5: Commit**

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform/dbt
git add project/models/intermediate/civics/int__civics_election_2025.sql project/models/intermediate/civics/int__civics.yaml
git commit -m "$(cat <<'EOF'
[DATA-1894] Null mis-tagged/duplicate archive position ids + uniqueness test

Apply seed_civics_election_2025_position_nullouts in int__civics_election_2025
and add unique_combination_of_columns on (br_position_database_id, election_date).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: End-to-end verification through the mart

Confirms the fix reaches `mart_civics.election` and that spot-check positions behave correctly.

**Files:** none (verification only; any doc tweak committed here).

- [ ] **Step 1: Rebuild the mart election model**

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform/dbt/project
dbt run --select election
```

- [ ] **Step 2: Spot-check a resolved mis-tag (58211 / 47891)**

```bash
dbt show --inline "
select br_position_database_id, election_date,
       count(*) as rows,
       count(br_position_database_id) as rows_with_tag
from {{ ref('election') }}
where br_position_database_id in (47891, 58211)
group by 1,2 order by 1,2
" --limit 20
```

Expected: for each position+date, `rows_with_tag = 1` (only the winning office keeps the tag;
the mismatched offices now have null and drop out of the group count on the non-null key).

- [ ] **Step 3: Confirm the 2026 flagship case is untouched**

```bash
dbt show --inline "
select gp_election_id, source_systems
from {{ ref('election') }}
where br_position_database_id = 190845 and election_date = '2026-05-19'
" --limit 10
```

Expected: still a single row with `['ballotready','techspeed']`. The seed only touches 2025
archive ids, so 2026 is unaffected.

- [ ] **Step 4: Confirm no 2025 collisions remain in the mart**

```bash
dbt show --inline "
select count(*) as remaining_2025_split_pairs from (
  select br_position_database_id, election_date
  from {{ ref('election') }}
  where br_position_database_id is not null and year(election_date) = 2025
  group by 1,2 having count(*) > 1
)
" --limit 5
```

Expected: `0`.

- [ ] **Step 5: Build downstream consumers to confirm nothing breaks**

```bash
dbt build --select "candidacy_hubspot candidacy_techspeed m_election_api__zip_to_position"
```

Expected: all build and test PASS. (They join `election` on `gp_election_id` and filter to
future elections, so nulled 2025 tags cannot affect them — this confirms it.)

- [ ] **Step 6: Open the PR**

```bash
cd /Users/hugh/Documents/repos_2/gp-data-platform
git push -u origin data-1894/archive-position-nullouts
gh pr create --title "[DATA-1894] Clean 2025-archive br_position_database_id collisions" --body "$(cat <<'EOF'
## What

Seed-driven null-out of mis-tagged (`office_mismatch`) and duplicate name-variant
(`duplicate_variant`) rows in the frozen 2025 election archive, so
`br_position_database_id` is trustworthy and unique. Adds a
`unique_combination_of_columns` guard on `int__civics_election_2025`.

## Why

`mart_civics.election` had 1,156 split `(br_position_database_id, election_date)` pairs, all
in the 2025 archive, caused by HubSpot's unreliable contact-to-BR-position tag. The 2026 BR/TS
symptom in the original ticket was transient staleness and is out of scope here.

## Verification

- `int__civics_election_2025` uniqueness test passes.
- Spot-checks: 47891 / 58211 resolved to one tagged row; 190845 (2026) untouched.
- 0 remaining 2025 split pairs in the mart.
- Downstream `candidacy_hubspot`, `candidacy_techspeed`, `m_election_api__zip_to_position` build green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- Null-not-drop → Task 3 Step 2 (case expression, no row removal). ✓
- Seed grain + columns → Task 2 Steps 4-5. ✓
- Seed application (single point) → Task 3 Step 2. ✓
- Generation (extract → classify → review → winner) → Task 2 Steps 1-4. ✓
- Winner tiebreak (official_office_name → has_ddhq_match → updated_at) → Task 2 Step 4. ✓
- Hard uniqueness test on the intermediate → Task 3 Step 1. ✓
- Seed-integrity relationships test → Task 2 Step 5. ✓
- Mart NOT hard-tested (2026 flake risk) → no mart uniqueness test added; verified by query only (Task 4 Step 4). ✓
- Plan-time assumptions (frozen snapshot; LEFT-join harmlessness) → Task 1. ✓
- Out of scope (2026 staleness; re-deriving correct positions) → honored; 190845 spot-check confirms 2026 untouched. ✓

**Placeholder scan:** No TBD/TODO. Classification (Task 2 Step 2) is a judgment step but the rubric, batching, and confidence flag are specified concretely; the deterministic winner/reason assembly (Step 4) is fully specified.

**Type consistency:** `gp_election_id` (string) is the seed key and the join key in Task 3; `reason` values `office_mismatch`/`duplicate_variant` match between Task 2 Step 4, the `accepted_values` test in Step 5, and the spec. Test arg wrapping under `arguments:` and `config.where` follow repo conventions.
