# DATA-1896: Filter spurious ZIP↔district overlaps in office picker

**Date:** 2026-05-07
**ClickUp:** https://goodparty.clickup.com/t/90132012119/DATA-1896
**Sprint:** Beaver 9 (4/27 - 5/10)
**Driver:** Hugh Karimi

## Problem

The OfficePicker onboarding flow surfaces offices whose districts only marginally overlap the user's ZIP, leading to ineligible options appearing in the list. Example: ZIP 90210 (Beverly Hills) is showing West Hollywood city council seats because a small number of L2 voter records on imprecise district boundaries fall into both areas.

The existing `int__zip_code_to_l2_district.py` model applies a 0.5%-of-zip-voters threshold (`MIN_VOTER_PCT = 0.005`) to filter out tiny overlaps. That threshold is too lenient for the OfficePicker use case (WeHo voters in 90210 still clear it), and even when tuned correctly, the threshold value is baked into the dbt pipeline — every retune requires a model rebuild.

## Goal

Move the threshold logic out of the dbt pipeline and into the election-api layer:

- Pipeline produces the **facts** (voter counts and the precomputed pct).
- election-api enforces the **policy** (the cutoff value).
- Tuning the cutoff is a config change in election-api, not a dbt rebuild.
- Below-threshold rows are preserved in `ZipToPosition` so the product can build features like "See more" / "District not listed?" without another data change.

## Design

### Data flow

```
int__l2_nationwide_uniform
  └─> int__zip_code_to_l2_district           (CHANGED: flat grain, +counts)
        └─> int__zip_code_to_br_office       (CHANGED: counts pass through at L2 grain)
              └─> m_election_api__zip_to_position  (CHANGED: BR-grain aggregation, +3 columns; no row filter)
                    └─> sync_election_api.py (CHANGED: 3 new columns, new index DDL)
                          └─> Postgres ZipToPosition (CHANGED: schema, index, more rows)
                                └─> election-api: WHERE pct_districtzip_to_zip >= 0.005
```

### Model: `int__zip_code_to_l2_district.py`

**Grain change:** from one row per `(zip_code, state_postal_code, district_type)` with an array of district names, to one row per `(zip_code, state_postal_code, district_type, district_name)` with flat columns.

**Schema:**

| column | type | notes |
|---|---|---|
| `zip_code` | string | not-null |
| `state_postal_code` | string | not-null |
| `district_type` | string | not-null |
| `district_name` | string | not-null (NEW grain key) |
| `voters_in_zip_district` | bigint | NEW; count of L2 voters in (zip × district) |
| `voters_in_zip` | bigint | NEW; count of L2 voters in zip (denominator) |
| `loaded_at` | timestamp | not-null |

**Logic changes:**

- Remove the `MIN_VOTER_PCT = 0.005` constant and the `.filter(col("voter_count") / col("total_voters") >= MIN_VOTER_PCT)` step.
- Stop dropping `voter_count` and `total_voters` — rename them `voters_in_zip_district` and `voters_in_zip` and keep them in the output.
- Drop the final `.groupBy(...).agg(collect_list("district_name"))` step. Output stays flat.
- Update the model's `unique_key` config to include `district_name`.
- Keep the existing incremental "affected zips" recompute logic (lines 68-92) — `voters_in_zip` is still a full-zip aggregate and must be recomputed against the full source on incremental runs to stay accurate.

### Model: `int__zip_code_to_br_office.sql`

**Grain unchanged:** `(zip_code, district_type, district_name, br_database_id)`. Other teams consume this model at this grain.

**Schema additions:** `voters_in_zip`, `voters_in_zip_district` — pass through 1:1 from upstream at the L2 grain. No aggregation here.

**Code changes:**

- Drop the `explode(district_names)` step (was line 23). Upstream is now flat.
- Add the two new columns to the SELECT list.
- Final qualify/dedup logic stays the same.

### Model: `m_election_api__zip_to_position.sql`

**No row filtering.** All `(zip_code, br_database_id)` combinations from `int__zip_code_to_br_office` flow through (modulo the existing `future_elections` and `district_id is not null` joins).

**The `zip_to_position` CTE changes** to aggregate L2-grain counts up to BR-position grain:

```sql
zip_to_position as (
    select
        zip_code,
        br_database_id,
        max(voters_in_zip) as voters_in_zip,
        sum(voters_in_zip_district) as voters_in_zip_district,
        sum(voters_in_zip_district) * 1.0 / max(voters_in_zip)
            as pct_districtzip_to_zip
    from {{ ref("int__zip_code_to_br_office") }}
    group by zip_code, br_database_id
),
```

When multiple L2 district names map to the same BR position (e.g., LA Council Districts 4 & 5 → "LA City Council"), `voters_in_zip_district` for the BR position is the **sum** across L2 names. `voters_in_zip` is invariant per zip, so `max()` is equivalent to `any_value()`.

**Bonus fix:** this aggregation eliminates the duplicate `(zip_code, position_id, election_date)` triples flagged by the temp `QUALIFY` workaround at `sync_election_api.py:252-264` (DATA-1867). Drop that QUALIFY in the same PR.

**New mart columns:** `voters_in_zip` (bigint), `voters_in_zip_district` (bigint), `pct_districtzip_to_zip` (double).

### Airflow: `sync_election_api.py` & Postgres `ZipToPosition`

- Add to `ZTP_SOURCE_COLUMNS` (line 94): `voters_in_zip`, `voters_in_zip_district`, `pct_districtzip_to_zip`.
- Add to `_ztp_transform_row` signature and tuple (line 112).
- Update `_ztp_constraint_ddl()` (line 191) to add a composite index: `(zip_code, pct_districtzip_to_zip)`. Composite is preferred over a single-column index on `pct_districtzip_to_zip` because the API filters by zip first.
- Drop the temp `QUALIFY row_number() ...` from `load_staging()` (line 252-264). The mart no longer produces dupes.

### election-api repo (out of scope here, but coordinated)

Repo: https://github.com/thegoodparty/election-api

1. **Prisma migration on `ZipToPosition`** — add three nullable columns: `voters_in_zip BigInt?`, `voters_in_zip_district BigInt?`, `pct_districtzip_to_zip Float?` (or match the repo's convention). Note: the composite index `(zip_code, pct_districtzip_to_zip)` is created by the sync DAG via `_ztp_constraint_ddl` (the DAG is the source of truth for indexes on `ZipToPosition`, per the existing pattern for the other `ZipToPosition_*_idx` indexes). Prisma should declare the columns but not the index.
2. **API filter** — the offices-by-zip endpoint adds a filter on `pct_districtzip_to_zip`. The filter must be **null-safe**: e.g., `WHERE pct_districtzip_to_zip IS NULL OR pct_districtzip_to_zip >= 0.005`. This protects against the deploy window where Prisma has added nullable columns but the next DAG sync hasn't yet populated them — without the null-safe branch, the filter would empty results and break the office picker. After the first post-migration sync completes, all rows have non-null values and the `IS NULL` branch becomes defense in depth.
3. **Threshold value** — `0.005` should be a config value (env var or a single named constant), not embedded in the query, so tuning doesn't require a deploy.

## Tests

### `int__zip_code_to_l2_district` (in `int__l2.yaml:168`)

- Update unique-combination test from `(zip_code, state_postal_code, district_type)` → `(zip_code, state_postal_code, district_type, district_name)`.
- Add `not_null` on `district_name`, `voters_in_zip_district`, `voters_in_zip`.
- Add `dbt_utils.accepted_range` on both count columns: `min_value: 1`, `inclusive: true`.
- Add `dbt_utils.expression_is_true`: `voters_in_zip_district <= voters_in_zip`.

### `int__zip_code_to_br_office` (in `int__l2.yaml:108`)

- Existing unique-combination test stays as-is.
- Add `not_null` + `dbt_utils.accepted_range >= 1` on the two new count columns.
- Add `dbt_utils.expression_is_true`: `voters_in_zip_district <= voters_in_zip`.

### `m_election_api__zip_to_position` (in `m_election_api.yaml:205`)

- Existing unique-combination test should now pass cleanly without the QUALIFY workaround.
- Add `dbt_utils.accepted_range` on `pct_districtzip_to_zip`: `min_value: 0`, `max_value: 1`, scoped `where zip_code is not null`.
- Add `dbt_utils.expression_is_true`: `voters_in_zip_district <= voters_in_zip`, scoped `where zip_code is not null`.
- The new count columns will be null on rows where the `left join` to zips produced no match (positions without zip coverage). Don't add `not_null` on them.

## Incremental / refresh strategy

Both `int__zip_code_to_l2_district` and `int__zip_code_to_br_office` are `materialized="incremental"` with `incremental_strategy="merge"` and `on_schema_change="fail"`. The schema changes break the existing tables on incremental runs.

**Required deploy step:** run `dbt build --select "int__zip_code_to_l2_district int__zip_code_to_br_office m_election_api__zip_to_position" --full-refresh` once on prod (dbt cloud cli) after the dbt PR merges. Subsequent runs go back to incremental.

The `sync_election_api` DAG handles schema changes natively — it rebuilds the staging table each run and swaps, so the new columns flow into Postgres on the next sync after the dbt full-refresh.

## Deployment & rollback

### Deploy order

The DAG must stay paused throughout the deploy. If it's currently unpaused, pause it before any code ships — that "freezes" Postgres in its last good state while we coordinate the two repos.

1. **Pause the `sync_election_api` DAG** in Airflow.
2. **PR 1 (this repo)** — dbt model + yaml + Airflow DAG changes — merges.
3. **dbt full-refresh** — run `dbt build --select "int__zip_code_to_l2_district int__zip_code_to_br_office m_election_api__zip_to_position" --full-refresh` once on prod. The Databricks mart now has the new columns; Postgres is unchanged because the DAG is paused.
4. **PR 2 (election-api repo)** — Prisma migration adds nullable columns, API filter is **null-safe** — merges. Existing `ZipToPosition` rows in Postgres now have new columns set to null, but the null-safe filter (`pct_districtzip_to_zip IS NULL OR pct_districtzip_to_zip >= 0.005`) keeps the office picker working with the existing data.
5. **Unpause the DAG.** Next run builds staging with new columns, applies the new index in `_ztp_constraint_ddl`, and swaps. Postgres now has new columns populated; the API filter applies cleanly to non-null values.

The DAG's swap is `RENAME` — it replaces the entire table. So the swap doesn't conflict with Prisma's column additions in step 4: the DAG creates the staging table fresh from the Databricks mart's column list, which now includes the three new columns. Prisma's schema description matches what the DAG produces.

### Risk window

While the DAG is paused (between steps 1 and 5), Postgres data is stale (it's not getting daily refreshes). For a multi-hour to ~1-day pause, this is acceptable — election data doesn't change minute-to-minute. If the pause stretches longer, run the DAG manually with the OLD code as a one-off to refresh data, then re-pause.

There is **no window where the office picker shows MORE garbage than today**, because the threshold filter goes live atomically with PR 2.

### Rollback

- **Roll back PR 1:** revert the merge and run `dbt build --full-refresh` with the reverted code. The Databricks mart returns to the prior schema. The next DAG sync (after unpausing) swaps Postgres back to the prior schema as well.
- **Roll back PR 2:** `ZipToPosition` keeps the (now-orphan) nullable columns and index — they're harmless. The API code reverts to the pre-filter query. No data loss.
- The Prisma migration is forward-compatible (extra nullable columns are ignored by older code) — no need to revert it even if PR 1 rolls back.

## Out of scope

- **Per-(zip, district_type) override seed.** Not needed for this iteration since all rows now flow to election-api and the threshold is tunable there. If a specific bad case slips through after launch, we can add a seed-based override layer in a follow-up.
- **Per-district-type or per-zip thresholds.** Single uniform 0.005 default; can be revisited based on post-launch QA data.
- **Absolute-voter-count floor (Nigel's suggestion).** Single percentage threshold for now; raw counts are exposed in the table so this can be added later in election-api without a schema change.
- **gp-api side.** OfficePicker's data path goes through election-api's `ZipToPosition`, not gp-api directly.
