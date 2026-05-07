# DATA-1896: Filter spurious ZIP↔district overlaps in office picker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the OfficePicker overlap-threshold logic out of dbt and into the election-api layer by exposing voter-count and percentage facts through `m_election_api__zip_to_position` and dropping the existing 0.5% L2 filter.

**Architecture:** Three dbt models change shape. `int__zip_code_to_l2_district` flattens its grain and adds `voters_in_zip_district` + `voters_in_zip` count columns. `int__zip_code_to_br_office` propagates those counts at L2-grain. `m_election_api__zip_to_position` aggregates counts up to BR-position grain via `sum(voters_in_zip_district)` + `max(voters_in_zip)` and exposes a derived `pct_districtzip_to_zip` column. No row filtering; below-threshold rows flow through to Postgres `ZipToPosition` for the election-api to filter at query time. The Airflow `sync_election_api.py` DAG gains 3 new columns in its source/target lists and a new composite index `(zip_code, pct_districtzip_to_zip)`. The temp `QUALIFY` dedup in `load_staging` (DATA-1867) is dropped since the new mart-level aggregation eliminates the duplicate triples.

**Tech Stack:** dbt cloud cli, PySpark (Databricks), Spark SQL, Airflow, psycopg2, Postgres.

**Spec:** `docs/superpowers/specs/2026-05-07-filter-spurious-zip-district-overlaps-design.md`

**Branch:** `DATA-1896-filter-spurious-zip-district-overlaps` (already created from `main`; spec doc already committed there)

---

## File map

| File | Change | Why |
|---|---|---|
| `dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py` | Modify (grain change, +counts, drop filter) | Core change — flatten output, expose voter counts |
| `dbt/project/models/intermediate/l2/int__l2.yaml` | Modify (tests for both intermediates) | Update unique-grain test for L2 model; add `not_null`/range tests for new cols on both intermediates |
| `dbt/project/models/intermediate/l2/int__zip_code_to_br_office.sql` | Modify (drop explode, +counts) | Pass counts through at L2 grain |
| `dbt/project/models/marts/election_api/m_election_api__zip_to_position.sql` | Modify (CTE aggregation, +3 cols, no filter) | Aggregate L2-grain counts to BR-position grain; expose pct |
| `dbt/project/models/marts/election_api/m_election_api.yaml` | Modify (tests for mart) | Add range / expression tests for new cols |
| `airflow/astro/dags/sync_election_api.py` | Modify (cols, transform, DDL, drop QUALIFY) | Sync new columns + index to Postgres; remove temp dedup hack |

---

## Prerequisites

- [ ] **P1: Confirm working directory and branch**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git branch --show-current
```

Expected: `DATA-1896-filter-spurious-zip-district-overlaps`

- [ ] **P2: Confirm spec doc is committed on this branch**

```bash
git log --oneline -1 -- docs/superpowers/specs/2026-05-07-filter-spurious-zip-district-overlaps-design.md
```

Expected: a commit referencing "design doc for filtering spurious ZIP-district overlaps".

---

## Task 1: Update `int__zip_code_to_l2_district.py` — flatten grain, expose counts

**Files:**
- Modify: `dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py`

**Goal:** Drop the `MIN_VOTER_PCT = 0.005` filter; stop wrapping output in arrays; expose `voters_in_zip_district` and `voters_in_zip` columns. New grain: one row per `(zip_code, state_postal_code, district_type, district_name)`.

- [ ] **Step 1.1: Replace the model file with the new version**

Rewrite `dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py` to the following content (this is a complete replacement, not a partial edit):

```python
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    count,
    length,
    lit,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    when,
)
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

THIS_TABLE_SCHEMA = StructType(
    [
        StructField(name="zip_code", dataType=StringType(), nullable=False),
        StructField(name="state_postal_code", dataType=StringType(), nullable=False),
        StructField(name="district_type", dataType=StringType(), nullable=False),
        StructField(name="district_name", dataType=StringType(), nullable=False),
        StructField(
            name="voters_in_zip_district", dataType=LongType(), nullable=False
        ),
        StructField(name="voters_in_zip", dataType=LongType(), nullable=False),
        StructField(name="loaded_at", dataType=TimestampType(), nullable=False),
    ]
)


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Maps each (zip_code, state, district_type, district_name) to L2 voter
    counts: voters in that intersection and voters in the zip overall.

    No threshold is applied — downstream consumers (election-api) decide
    the cutoff. Output is flat (one row per intersection); upstream callers
    that want an array of names should aggregate themselves.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[
            "zip_code",
            "state_postal_code",
            "district_type",
            "district_name",
        ],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "zip_code", "districts"],
    )

    l2_uniform_data: DataFrame = dbt.ref("int__l2_nationwide_uniform")

    # On incremental runs, identify zip codes with new voter records, then
    # recompute those zips against the full source population so voters_in_zip
    # is accurate (not skewed by batch size).
    if dbt.is_incremental:
        existing_table = session.table(f"{dbt.this}")
        max_loaded_at = existing_table.agg({"loaded_at": "max"}).collect()[0][0]

        if max_loaded_at is not None:
            new_records = l2_uniform_data.filter(col("loaded_at") > max_loaded_at)
        else:
            new_records = l2_uniform_data

        if new_records.count() == 0:
            return session.createDataFrame(
                data=[],
                schema=THIS_TABLE_SCHEMA,
            )

        affected_zips = (
            new_records.select("Residence_Addresses_Zip", "state_postal_code")
            .filter(col("Residence_Addresses_Zip").isNotNull())
            .distinct()
        )
        l2_uniform_data = l2_uniform_data.join(
            affected_zips,
            on=["Residence_Addresses_Zip", "state_postal_code"],
            how="inner",
        )

    district_type_from_columns = (
        dbt.ref("int__model_prediction_voter_turnout")
        .select(col("district_type"))
        .distinct()
        .collect()
    )
    district_type_from_columns = [
        district_type.district_type for district_type in district_type_from_columns
    ]

    # Country/State are too coarse to be useful for office picking
    district_type_from_columns = [
        district_type
        for district_type in district_type_from_columns
        if district_type not in ["Country", "State"]
    ]

    zip_window = Window.partitionBy("zip_code", "state_postal_code")

    district_dataframes = []
    for district_type in district_type_from_columns:
        district_df = (
            l2_uniform_data.select(
                col("state_postal_code"),
                col("Residence_Addresses_Zip").alias("zip_code"),
                col(district_type).alias("district_name"),
            )
            .filter(col("Residence_Addresses_Zip").isNotNull())
            .filter(col("district_name").isNotNull())
        )

        # if zipcode is 4 characters, pad with a 0
        district_df = district_df.withColumn(
            "zip_code",
            when(
                length(col("zip_code")) == 4, concat(lit("0"), col("zip_code"))
            ).otherwise(col("zip_code")),
        )

        district_df = (
            district_df.groupBy(
                "state_postal_code", "zip_code", "district_name"
            )
            .agg(count("*").alias("voters_in_zip_district"))
            .withColumn(
                "voters_in_zip",
                spark_sum("voters_in_zip_district").over(zip_window),
            )
            .withColumn("district_type", lit(district_type))
        )

        district_dataframes.append(district_df)

    if district_dataframes:
        zip_code_to_l2_district = district_dataframes[0]
        for df in district_dataframes[1:]:
            zip_code_to_l2_district = zip_code_to_l2_district.union(df)
    else:
        return session.createDataFrame(data=[], schema=THIS_TABLE_SCHEMA)

    final_result = zip_code_to_l2_district.select(
        col("zip_code"),
        col("state_postal_code"),
        col("district_type"),
        col("district_name"),
        col("voters_in_zip_district").cast(LongType()).alias("voters_in_zip_district"),
        col("voters_in_zip").cast(LongType()).alias("voters_in_zip"),
    ).withColumn("loaded_at", lit(datetime.now()))

    return final_result
```

Key differences vs. the previous version:
- No `MIN_VOTER_PCT` constant; no `.filter(... >= MIN_VOTER_PCT)`.
- `unique_key` includes `district_name`.
- Output schema has flat `district_name` + `voters_in_zip_district` + `voters_in_zip` (LongType / bigint), no `district_names` array.
- Removed `collect_list` aggregation and `size(...) > 0` filter (they were collapsing rows).

- [ ] **Step 1.2: Build the model on Databricks with --full-refresh**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt build --select int__zip_code_to_l2_district --full-refresh
```

Expected: model builds successfully. (yaml tests run too — those are still on the OLD `(zip_code, state_postal_code, district_type)` unique combination and will FAIL. That's expected; we'll fix the yaml in Task 2.)

- [ ] **Step 1.3: Inspect the data**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt run-operation inspect_data --args '{"model": "int__zip_code_to_l2_district"}'
```

Expected: ~tens of millions of rows, 6 columns + loaded_at, all 100% populated, sample rows showing realistic ZIPs and district names. `voters_in_zip_district` should be ≥ 1 on every row; `voters_in_zip` should match across rows for the same zip.

- [ ] **Step 1.4: Spot-check the WeHo/90210 case**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select district_type, district_name, voters_in_zip_district, voters_in_zip, voters_in_zip_district * 1.0 / voters_in_zip as pct from {{ ref('int__zip_code_to_l2_district') }} where zip_code = '90210' and state_postal_code = 'CA' order by district_type, voters_in_zip_district desc" --limit 50
```

Expected: visible "City" row with `district_name = West Hollywood` and a small pct (likely below 0.5%), plus the legitimate 90210 rows (Beverly Hills city; LA Council Districts 4 & 5; etc.) at higher pct values.

- [ ] **Step 1.5: Stage and commit**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git add dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py
cd dbt && poetry run git -C /Users/hkarimi/Documents/repos_4/gp-data-platform commit -m "$(cat <<'EOF'
DATA-1896: flatten int__zip_code_to_l2_district, expose voter counts

Changes the grain to one row per (zip, state, district_type, district_name)
and drops the in-model 0.5% threshold. Adds voters_in_zip_district and
voters_in_zip count columns so downstream consumers can apply their own
threshold without a dbt rebuild.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Update `int__l2.yaml` tests for `int__zip_code_to_l2_district`

**Files:**
- Modify: `dbt/project/models/intermediate/l2/int__l2.yaml` (lines 168-188)

- [ ] **Step 2.1: Update the yaml block for `int__zip_code_to_l2_district`**

Open `dbt/project/models/intermediate/l2/int__l2.yaml`. Find the existing block:

```yaml
  - name: int__zip_code_to_l2_district
    description: "Zip code to L2 district mapping"
    columns:
      - name: zip_code
        description: "Zip code"
        tests:
          - not_null
      - name: state_postal_code
        description: "State postal code"
        tests:
          - dbt_expectations.expect_column_distinct_values_to_equal_set:
              arguments:
                value_set: "{{ get_us_states_list(include_DC=true, include_US=false) }}"
    tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - zip_code
              - state_postal_code
              - district_type
```

Replace it with:

```yaml
  - name: int__zip_code_to_l2_district
    description: >
      Zip code to L2 district mapping. One row per
      (zip_code, state_postal_code, district_type, district_name) with the
      L2 voter count in that intersection and the total L2 voter count for
      the zip. No threshold is applied; downstream consumers decide cutoffs.
    columns:
      - name: zip_code
        description: "5-digit zip code"
        tests:
          - not_null
      - name: state_postal_code
        description: "State postal code"
        tests:
          - dbt_expectations.expect_column_distinct_values_to_equal_set:
              arguments:
                value_set: "{{ get_us_states_list(include_DC=true, include_US=false) }}"
      - name: district_type
        description: "L2 district column name (e.g. City, US_Congressional_District)"
        tests:
          - not_null
      - name: district_name
        description: "L2 district value within district_type for this zip"
        tests:
          - not_null
      - name: voters_in_zip_district
        description: >
          Count of L2 voters whose residence is in this zip AND in this
          (district_type, district_name).
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                inclusive: true
      - name: voters_in_zip
        description: >
          Count of L2 voters whose residence is in this zip (across all
          district_names of this district_type). Invariant per
          (zip_code, state_postal_code, district_type).
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                inclusive: true
    tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - zip_code
              - state_postal_code
              - district_type
              - district_name
      - dbt_utils.expression_is_true:
          arguments:
            expression: "voters_in_zip_district <= voters_in_zip"
```

- [ ] **Step 2.2: Run the tests**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt test --select int__zip_code_to_l2_district
```

Expected: all tests PASS. If `voters_in_zip_district <= voters_in_zip` fails, that signals a bug in Task 1's model — investigate.

- [ ] **Step 2.3: Commit**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git add dbt/project/models/intermediate/l2/int__l2.yaml
cd dbt && poetry run git -C /Users/hkarimi/Documents/repos_4/gp-data-platform commit -m "$(cat <<'EOF'
DATA-1896: update int__zip_code_to_l2_district yaml tests for new grain

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Update `int__zip_code_to_br_office.sql` — drop explode, propagate counts

**Files:**
- Modify: `dbt/project/models/intermediate/l2/int__zip_code_to_br_office.sql`

- [ ] **Step 3.1: Replace the model with the new version**

Rewrite `dbt/project/models/intermediate/l2/int__zip_code_to_br_office.sql` to:

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        auto_liquid_cluster=true,
        unique_key=[
            "zip_code",
            "district_type",
            "district_name",
            "br_database_id",
        ],
        tags=["intermediate", "l2", "ballotready", "zip_code"],
    )
}}

with
    zip_code_to_l2_district as (
        select
            zip_code,
            state_postal_code,
            district_type,
            district_name,
            voters_in_zip_district,
            voters_in_zip,
            loaded_at
        from {{ ref("int__zip_code_to_l2_district") }}
        {% if is_incremental() %}
            where loaded_at > (select max(loaded_at) from {{ this }})
        {% endif %}
    ),
    -- Some L2 voters have an out-of-state zip in the L2 file; filter those.
    zip_code_within_state_range as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            tbl_zip.voters_in_zip_district,
            tbl_zip.voters_in_zip,
            tbl_zip.loaded_at
        from zip_code_to_l2_district as tbl_zip
        inner join
            {{ ref("int__general_states_zip_code_range") }} as zip_range
            on tbl_zip.state_postal_code = zip_range.state_postal_code
            and tbl_zip.zip_code >= zip_range.zip_code_range[0]
            and tbl_zip.zip_code <= zip_range.zip_code_range[1]
    ),
    zip_code_to_br_office as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            tbl_zip.voters_in_zip_district,
            tbl_zip.voters_in_zip,
            tbl_zip.loaded_at,
            tbl_match.name,
            tbl_match.br_database_id,
            tbl_br_race.id as br_race_id,
            tbl_br_race.database_id as br_race_database_id,
            tbl_br_position.id as br_position_id,
            tbl_match.l2_district_name,
            tbl_match.l2_district_type,
            tbl_match.is_matched,
            tbl_match.llm_reason,
            tbl_match.confidence,
            tbl_match.embeddings,
            tbl_match.top_embedding_score
        from zip_code_within_state_range as tbl_zip
        left join
            {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as tbl_match
            on lower(tbl_zip.district_name) = lower(tbl_match.l2_district_name)
            and lower(tbl_zip.district_type) = lower(tbl_match.l2_district_type)
            and lower(tbl_zip.state_postal_code) = lower(tbl_match.state)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_match.br_database_id = tbl_br_position.database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_br_race
            on tbl_br_position.database_id = tbl_br_race.position.databaseid
        -- dedup to the latest race per BR position per zip+district
        qualify
            row_number() over (
                partition by
                    zip_code, district_type, district_name, tbl_match.br_database_id
                order by br_race_database_id desc
            )
            = 1
    )
select
    zip_code,
    state_postal_code,
    district_type,
    district_name,
    voters_in_zip_district,
    voters_in_zip,
    loaded_at,
    name,
    br_database_id,
    br_position_id,
    br_race_id,
    br_race_database_id,
    l2_district_name,
    l2_district_type,
    is_matched,
    llm_reason,
    confidence,
    embeddings,
    top_embedding_score
from zip_code_to_br_office
```

Key differences vs. the previous version:
- Removed `district_names` array reference; reads `district_name` directly from upstream.
- Removed the `explode(district_names) as exploded_district_name` and the `exploded_district_name` alias usage.
- Added `voters_in_zip_district` and `voters_in_zip` to all CTEs and the final SELECT.
- Removed the long header comment about "the results can be aggregated in multiple ways" — no longer relevant since the upstream is now flat.

- [ ] **Step 3.2: Build with --full-refresh**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt build --select int__zip_code_to_br_office --full-refresh
```

Expected: model builds, existing yaml tests still pass (we haven't added new tests yet — that's Step 3.4).

- [ ] **Step 3.3: Spot-check 90210**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select district_name, name, br_database_id, voters_in_zip_district, voters_in_zip, voters_in_zip_district * 1.0 / voters_in_zip as pct from {{ ref('int__zip_code_to_br_office') }} where zip_code = '90210' and district_type = 'City' order by voters_in_zip_district desc" --limit 50
```

Expected: rows for Beverly Hills (high pct) and West Hollywood (low pct, e.g., < 0.005). `name` and `br_database_id` populated for matched rows.

- [ ] **Step 3.4: Update yaml tests for `int__zip_code_to_br_office`**

In `dbt/project/models/intermediate/l2/int__l2.yaml`, find the existing `int__zip_code_to_br_office` block (lines 108-166). Add `voters_in_zip_district` and `voters_in_zip` column entries (insert them after the existing `district_name` column entry around line 128, before the existing `confidence` entry):

```yaml
      - name: voters_in_zip_district
        description: >
          L2 voters in this (zip, district_type, district_name). Passes
          through unchanged from int__zip_code_to_l2_district at L2 grain.
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                inclusive: true
      - name: voters_in_zip
        description: >
          L2 voters in this zip across all district_names of this
          district_type. Invariant per (zip, state, district_type).
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                inclusive: true
```

Also add this row-level test under the existing `tests:` block at the end of the model definition (after the `dbt_utils.unique_combination_of_columns` test):

```yaml
      - dbt_utils.expression_is_true:
          arguments:
            expression: "voters_in_zip_district <= voters_in_zip"
```

- [ ] **Step 3.5: Run the tests**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt test --select int__zip_code_to_br_office
```

Expected: all tests PASS.

- [ ] **Step 3.6: Commit**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git add dbt/project/models/intermediate/l2/int__zip_code_to_br_office.sql dbt/project/models/intermediate/l2/int__l2.yaml
cd dbt && poetry run git -C /Users/hkarimi/Documents/repos_4/gp-data-platform commit -m "$(cat <<'EOF'
DATA-1896: propagate voter counts through int__zip_code_to_br_office

Removes the explode(district_names) since upstream is now flat. Passes
voters_in_zip_district and voters_in_zip through at L2 grain. Adds tests
for the new columns plus a voters_in_zip_district <= voters_in_zip
invariant.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Update `m_election_api__zip_to_position.sql` — aggregate, expose pct, no row filter

**Files:**
- Modify: `dbt/project/models/marts/election_api/m_election_api__zip_to_position.sql`

- [ ] **Step 4.1: Replace the mart with the new version**

Rewrite `dbt/project/models/marts/election_api/m_election_api__zip_to_position.sql` to:

```sql
{{
    config(
        materialized="table",
        tags=["mart", "election_api", "officepicker"],
    )
}}

with
    future_elections as (
        select
            election_date,
            election_year,
            state,
            office_level,
            office_type,
            city,
            district,
            is_judicial,
            br_position_database_id
        from {{ ref("election") }}
        where
            election_date > current_date()
            and election_date <= current_date() + interval 2 years
    ),

    zip_to_position as (
        select
            zip_code,
            br_database_id,
            max(voters_in_zip) as voters_in_zip,
            sum(voters_in_zip_district) as voters_in_zip_district,
            sum(voters_in_zip_district) * 1.0 / max(voters_in_zip)
                as pct_districtzip_to_zip
        from {{ ref("int__zip_code_to_br_office") }}
        where br_database_id is not null
        group by zip_code, br_database_id
    ),

    positions as (
        select id as position_id, name, br_database_id
        from {{ ref("m_election_api__position") }}
        where district_id is not null
    ),

    officepicker as (
        select
            pos.position_id,
            pos.name,
            zips.zip_code,
            elec.election_year,
            case
                when elec.is_judicial then 'Judicial' else elec.office_level
            end as display_office_level,
            elec.office_type,
            elec.state,
            elec.city,
            elec.district,
            elec.election_date,
            elec.br_position_database_id as br_database_id,
            zips.voters_in_zip,
            zips.voters_in_zip_district,
            zips.pct_districtzip_to_zip
        from future_elections as elec
        left join
            zip_to_position as zips
            on zips.br_database_id = elec.br_position_database_id
        inner join positions as pos on pos.br_database_id = elec.br_position_database_id
    )

select *
from officepicker
```

Key differences vs. previous version:
- `zip_to_position` CTE now aggregates: `max(voters_in_zip)`, `sum(voters_in_zip_district)`, derived `pct_districtzip_to_zip`. Filters out rows where `br_database_id is null` (those are unmatched L2 districts and have no BR position to anchor on).
- `officepicker` CTE selects three new columns: `voters_in_zip`, `voters_in_zip_district`, `pct_districtzip_to_zip`.
- No `WHERE pct_districtzip_to_zip >= ...` row filter — election-api will filter at query time.

- [ ] **Step 4.2: Build the mart**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt build --select m_election_api__zip_to_position
```

Expected: builds, the existing `unique_combination_of_columns` test on `(zip_code, position_id, election_date)` should now PASS without the temp QUALIFY workaround. (If it fails, the BR-grain aggregation isn't actually deduping — investigate.)

- [ ] **Step 4.3: Inspect the data**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt run-operation inspect_data --args '{"model": "m_election_api__zip_to_position"}'
```

Expected: row count is somewhat HIGHER than today's prod (sub-threshold rows now flow through). Each new column populated for rows where `zip_code is not null`; null for positions without zip coverage. `pct_districtzip_to_zip` between 0 and 1.

- [ ] **Step 4.4: Spot-check 90210 / WeHo**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select name, voters_in_zip_district, voters_in_zip, pct_districtzip_to_zip from {{ ref('m_election_api__zip_to_position') }} where zip_code = '90210' and state = 'CA' order by pct_districtzip_to_zip desc" --limit 50
```

Expected: West Hollywood-related rows present with low `pct_districtzip_to_zip` (likely < 0.005). Beverly Hills / LA Council Districts 4 & 5 with higher pct values.

- [ ] **Step 4.5: Update mart yaml tests**

In `dbt/project/models/marts/election_api/m_election_api.yaml`, find the `m_election_api__zip_to_position` block (lines 205-271). Add column entries for the three new columns (insert before the existing `tests:` block at line 265):

```yaml
      - name: voters_in_zip
        description: >
          L2 voters in this zip (sum across district names that map to this
          BR position via the LLM L2-to-BR match). Null when this position
          has no zip coverage.
      - name: voters_in_zip_district
        description: >
          L2 voters in this (zip, BR position). Aggregated from upstream
          int__zip_code_to_br_office by sum() across L2 district names that
          map to the same BR position. Null when no zip coverage.
      - name: pct_districtzip_to_zip
        description: >
          voters_in_zip_district / voters_in_zip — fraction of zip's L2
          voters who live in any L2 district that maps to this BR position.
          Used by election-api to filter spurious overlaps. Null when no
          zip coverage.
        tests:
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
                max_value: 1
                inclusive: true
              config:
                where: "zip_code is not null"
```

Also add this row-level invariant to the model-level `tests:` block (alongside the existing `dbt_utils.unique_combination_of_columns` test):

```yaml
      - dbt_utils.expression_is_true:
          arguments:
            expression: "voters_in_zip_district <= voters_in_zip"
          config:
            where: "zip_code is not null"
```

- [ ] **Step 4.6: Run the tests**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt test --select m_election_api__zip_to_position
```

Expected: all tests PASS — including the unique-combination test on `(zip_code, position_id, election_date)` which previously had 9 dupes per the temp QUALIFY in `sync_election_api.py`.

- [ ] **Step 4.7: Commit**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git add dbt/project/models/marts/election_api/m_election_api__zip_to_position.sql dbt/project/models/marts/election_api/m_election_api.yaml
cd dbt && poetry run git -C /Users/hkarimi/Documents/repos_4/gp-data-platform commit -m "$(cat <<'EOF'
DATA-1896: aggregate voter counts in m_election_api__zip_to_position

Aggregates L2-grain counts up to BR-position grain via sum() and exposes
pct_districtzip_to_zip for election-api to filter on. No row-level filter
in the mart; below-threshold rows pass through to ZipToPosition so
election-api can apply (and tune) the cutoff. The aggregation also
eliminates the duplicate (zip_code, position_id, election_date) triples
that DATA-1867's temp QUALIFY worked around.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Update `sync_election_api.py` — new columns, drop QUALIFY hack, add index

**Files:**
- Modify: `airflow/astro/dags/sync_election_api.py`

- [ ] **Step 5.1: Update `ZTP_SOURCE_COLUMNS`**

In `airflow/astro/dags/sync_election_api.py`, find the existing list (around line 94):

```python
ZTP_SOURCE_COLUMNS = [
    "position_id",
    "name",
    "br_database_id",
    "zip_code",
    "election_year",
    "election_date",
    "display_office_level",
    "office_type",
    "state",
    "district",
]
```

Replace with:

```python
ZTP_SOURCE_COLUMNS = [
    "position_id",
    "name",
    "br_database_id",
    "zip_code",
    "election_year",
    "election_date",
    "display_office_level",
    "office_type",
    "state",
    "district",
    "voters_in_zip",
    "voters_in_zip_district",
    "pct_districtzip_to_zip",
]
```

- [ ] **Step 5.2: Update `_ztp_transform_row`**

Find the existing function (around line 112):

```python
def _ztp_transform_row(row: tuple) -> tuple:
    (
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
    ) = row
    row_id = uuid.uuid5(
        _UUID_NAMESPACE,
        f"{zip_code}|{position_id}|{election_date}",
    )
    return (
        str(row_id),
        datetime.now(timezone.utc),
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
    )
```

Replace with:

```python
def _ztp_transform_row(row: tuple) -> tuple:
    (
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
        voters_in_zip,
        voters_in_zip_district,
        pct_districtzip_to_zip,
    ) = row
    row_id = uuid.uuid5(
        _UUID_NAMESPACE,
        f"{zip_code}|{position_id}|{election_date}",
    )
    return (
        str(row_id),
        datetime.now(timezone.utc),
        position_id,
        name,
        br_database_id,
        zip_code,
        election_year,
        election_date,
        display_office_level,
        office_type,
        state,
        district,
        voters_in_zip,
        voters_in_zip_district,
        pct_districtzip_to_zip,
    )
```

- [ ] **Step 5.3: Add the new index to `ZTP` spec and `_ztp_constraint_ddl`**

Find the existing `ZTP` definition (around line 84):

```python
ZTP = TableSyncSpec(
    target_table="ZipToPosition",
    indexes=(
        "ZipToPosition_zip_code_idx",
        "ZipToPosition_position_id_idx",
        "ZipToPosition_zip_code_position_id_election_date_key",
    ),
    fkeys=("ZipToPosition_position_id_fkey",),
)
```

Replace with (add the new composite index name to the tuple):

```python
ZTP = TableSyncSpec(
    target_table="ZipToPosition",
    indexes=(
        "ZipToPosition_zip_code_idx",
        "ZipToPosition_position_id_idx",
        "ZipToPosition_zip_code_pct_districtzip_to_zip_idx",
        "ZipToPosition_zip_code_position_id_election_date_key",
    ),
    fkeys=("ZipToPosition_position_id_fkey",),
)
```

Then find `_ztp_constraint_ddl()` (around line 191) and add a new `CREATE INDEX` for the composite index. The existing function:

```python
def _ztp_constraint_ddl() -> list[str]:
    sn, nt = ZTP.staging_schema, ZTP.new_table
    target_schema = ZTP.target_schema
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name(ZTP.pk_name)}" PRIMARY KEY (id)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_zip_code_idx")}" '
            f'ON "{sn}"."{nt}" (zip_code)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_position_id_idx")}" '
            f'ON "{sn}"."{nt}" (position_id)'
        ),
        (
            f"CREATE UNIQUE INDEX "
            f'"{ZTP.stage_name("ZipToPosition_zip_code_position_id_election_date_key")}" '
            f'ON "{sn}"."{nt}" '
            f"(zip_code, position_id, election_date) NULLS NOT DISTINCT"
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name("ZipToPosition_position_id_fkey")}" '
            f"FOREIGN KEY (position_id) "
            f'REFERENCES "{target_schema}"."Position"(id) '
            f"ON UPDATE CASCADE ON DELETE RESTRICT"
        ),
    ]
```

Insert a new statement immediately after the `ZipToPosition_position_id_idx` CREATE INDEX, so the function becomes:

```python
def _ztp_constraint_ddl() -> list[str]:
    sn, nt = ZTP.staging_schema, ZTP.new_table
    target_schema = ZTP.target_schema
    return [
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name(ZTP.pk_name)}" PRIMARY KEY (id)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_zip_code_idx")}" '
            f'ON "{sn}"."{nt}" (zip_code)'
        ),
        (
            f'CREATE INDEX "{ZTP.stage_name("ZipToPosition_position_id_idx")}" '
            f'ON "{sn}"."{nt}" (position_id)'
        ),
        (
            f"CREATE INDEX "
            f'"{ZTP.stage_name("ZipToPosition_zip_code_pct_districtzip_to_zip_idx")}" '
            f'ON "{sn}"."{nt}" (zip_code, pct_districtzip_to_zip)'
        ),
        (
            f"CREATE UNIQUE INDEX "
            f'"{ZTP.stage_name("ZipToPosition_zip_code_position_id_election_date_key")}" '
            f'ON "{sn}"."{nt}" '
            f"(zip_code, position_id, election_date) NULLS NOT DISTINCT"
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{ZTP.stage_name("ZipToPosition_position_id_fkey")}" '
            f"FOREIGN KEY (position_id) "
            f'REFERENCES "{target_schema}"."Position"(id) '
            f"ON UPDATE CASCADE ON DELETE RESTRICT"
        ),
    ]
```

- [ ] **Step 5.4: Drop the temp QUALIFY workaround in `load_staging`**

Find the existing `load_staging` function (around line 248-272):

```python
        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(ZTP_SOURCE_COLUMNS)
            # TEMP (DATA-1867): prod mart has 9 dupe (zip_code, position_id,
            # election_date) triples pending a full-refresh rebuild. Dedupe
            # here so the unique index doesn't block the load. Drop the
            # QUALIFY once the dbt unique test passes on prod.
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__zip_to_position` "
                f"QUALIFY row_number() OVER ("
                f"  PARTITION BY zip_code, position_id, election_date "
                f"  ORDER BY display_office_level"
                f") = 1"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    ZTP,
                    source_query=query,
                    target_columns=ZTP_TARGET_COLUMNS,
                    transform_row=_ztp_transform_row,
                )
```

Replace with (remove the comment block and the QUALIFY):

```python
        @task
        def load_staging() -> int:
            catalog = Variable.get("databricks_catalog")
            col_list = ", ".join(ZTP_SOURCE_COLUMNS)
            query = (
                f"SELECT {col_list} "
                f"FROM `{catalog}`.`{DATABRICKS_SCHEMA}`."
                f"`m_election_api__zip_to_position`"
            )
            with _open_pg() as conn:
                return bulk_insert_from_databricks(
                    conn,
                    ZTP,
                    source_query=query,
                    target_columns=ZTP_TARGET_COLUMNS,
                    transform_row=_ztp_transform_row,
                )
```

- [ ] **Step 5.5: Run the Airflow DAG import test**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/airflow/astro
poetry run pytest tests/dags/test_dag_example.py -v 2>&1 | tail -40
```

Expected: `test_file_imports[dags/sync_election_api.py-None]` PASSes (no import error). Other DAG tests should also pass.

If the test infrastructure isn't set up locally, fall back to a Python syntax check:

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
python -c "import ast; ast.parse(open('airflow/astro/dags/sync_election_api.py').read()); print('OK')"
```

Expected: `OK`.

- [ ] **Step 5.6: Commit**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git add airflow/astro/dags/sync_election_api.py
cd dbt && poetry run git -C /Users/hkarimi/Documents/repos_4/gp-data-platform commit -m "$(cat <<'EOF'
DATA-1896: sync new ZipToPosition columns + composite index

Adds voters_in_zip, voters_in_zip_district, and pct_districtzip_to_zip
to ZTP_SOURCE_COLUMNS and the row transform. Adds a composite
(zip_code, pct_districtzip_to_zip) index to support the election-api
threshold filter. Drops the DATA-1867 temp QUALIFY dedup since the
mart-level aggregation now produces unique (zip_code, position_id,
election_date) triples.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: End-to-end verification

- [ ] **Step 6.1: Verify all three dbt models build clean together**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt build --select "int__zip_code_to_l2_district int__zip_code_to_br_office m_election_api__zip_to_position" --full-refresh
```

Expected: all three models build, all tests PASS.

- [ ] **Step 6.2: Verify the WeHo/90210 case has been "captured" in the mart**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select name, district, pct_districtzip_to_zip from {{ ref('m_election_api__zip_to_position') }} where zip_code = '90210' and state = 'CA' and lower(name) like '%west hollywood%' order by pct_districtzip_to_zip" --limit 10
```

Expected: at least one row with WeHo in the name and `pct_districtzip_to_zip < 0.005` — meaning when election-api applies its filter, this row will correctly be excluded.

- [ ] **Step 6.3: Verify legitimate 90210 offices are NOT excluded by a 0.5% filter**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select name, district, pct_districtzip_to_zip from {{ ref('m_election_api__zip_to_position') }} where zip_code = '90210' and state = 'CA' and pct_districtzip_to_zip >= 0.005 order by pct_districtzip_to_zip desc" --limit 30
```

Expected: Beverly Hills city offices, LA County offices, LA Council District 4 / 5 offices — the legitimate set the 90210 user should see.

- [ ] **Step 6.4: Verify `pct_districtzip_to_zip` distribution**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select width_bucket(pct_districtzip_to_zip, 0, 1, 20) as bucket, count(*) as n from {{ ref('m_election_api__zip_to_position') }} where zip_code is not null group by 1 order by 1" --limit 25
```

Expected: histogram with most rows at high pct (close to 1) and a tail near 0 — confirming Dan's prediction that the long tail is small.

- [ ] **Step 6.5: Confirm row count change is reasonable**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform/dbt/project
dbt show --inline "select count(*) as total_rows, count(*) filter (where pct_districtzip_to_zip < 0.005) as below_threshold, count(*) filter (where pct_districtzip_to_zip >= 0.005) as at_or_above_threshold, count(*) filter (where zip_code is null) as no_zip_coverage from {{ ref('m_election_api__zip_to_position') }}" --limit 5
```

Expected: `at_or_above_threshold` should be roughly comparable to today's prod row count (today's mart used a 0.5% L2 filter that's similar). `below_threshold` shows the new rows being preserved for "See more" features. `no_zip_coverage` matches today's count of positions without zip mapping.

---

## Task 7: Open the PR

- [ ] **Step 7.1: Push branch and open PR**

```bash
cd /Users/hkarimi/Documents/repos_4/gp-data-platform
git push -u origin DATA-1896-filter-spurious-zip-district-overlaps
gh pr create --title "DATA-1896: filter spurious ZIP↔district overlaps in office picker" --body "$(cat <<'EOF'
## Summary

- Flatten `int__zip_code_to_l2_district` grain and expose `voters_in_zip_district` + `voters_in_zip` count columns; remove the in-model 0.5% filter so all rows flow through.
- Propagate counts through `int__zip_code_to_br_office` at L2 grain; aggregate up to BR-position grain in `m_election_api__zip_to_position` via `sum()` + `max()` and expose `pct_districtzip_to_zip`.
- Add `voters_in_zip`, `voters_in_zip_district`, `pct_districtzip_to_zip` to the `sync_election_api` DAG's source/target columns and add a composite `(zip_code, pct_districtzip_to_zip)` index to support the election-api threshold filter.
- Drop the DATA-1867 temp `QUALIFY` dedup — the new mart-level aggregation eliminates the dupes.

Threshold logic moves to election-api (separate PR there) so the cutoff is tunable without a dbt rebuild and below-threshold rows are preserved for future "See more" UX.

Spec: `docs/superpowers/specs/2026-05-07-filter-spurious-zip-district-overlaps-design.md`

## Test plan

- [ ] dbt build/test of all three modified models passes (with `--full-refresh` for the two incremental ones)
- [ ] WeHo rows for 90210 are present in the mart with `pct_districtzip_to_zip < 0.005` (verifying our threshold filter at the API will catch them)
- [ ] Legitimate 90210 offices (Beverly Hills, LA Council, etc.) are present at higher pct values
- [ ] `unique_combination_of_columns` test on `(zip_code, position_id, election_date)` passes without the temp QUALIFY
- [ ] Airflow DAG imports without errors

## Coordinated work (election-api repo, separate PR)

The election-api repo needs:
1. Prisma migration adding nullable `voters_in_zip BigInt?`, `voters_in_zip_district BigInt?`, `pct_districtzip_to_zip Float?` to `ZipToPosition`. The composite index is created by this DAG (per the existing `ZipToPosition_*_idx` pattern), not Prisma.
2. Null-safe API filter on the offices endpoint: `pct_districtzip_to_zip IS NULL OR pct_districtzip_to_zip >= 0.005`. Threshold should be a config value, not embedded in the query.

## Deploy order

1. Pause the `sync_election_api` DAG.
2. Merge this PR.
3. Run `dbt build --select "int__zip_code_to_l2_district int__zip_code_to_br_office m_election_api__zip_to_position" --full-refresh` on prod (dbt cloud).
4. Merge the election-api PR (Prisma migration + null-safe API filter).
5. Unpause the `sync_election_api` DAG.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed. Verify it lists all the changed files.

- [ ] **Step 7.2: Mark the implementation task complete**

The dbt + Airflow side is now complete. Coordinate with Win team (Swain) on the election-api PR to start, following the deploy order above.

---

## Out of scope (per spec)

- Per-(zip, district_type) override seed.
- Per-district-type or per-zip thresholds.
- Absolute-voter-count floor.
- gp-api side changes (data path goes through election-api).
- election-api Prisma migration + API filter (separate repo, separate PR, coordinated with Win team).
