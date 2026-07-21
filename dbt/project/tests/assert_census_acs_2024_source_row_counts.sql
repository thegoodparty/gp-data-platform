{{ config(severity="error") }}

-- Pinned from the loader's committed manifest (dbt/scripts/acs5y2024_manifest.json):
-- the 2020-2024 extract is a one-time load, so exact per-table row counts are
-- stable contract values, the decennial fixed-count pattern. A failure means a
-- partial or out-of-band re-load; re-run the documented loader, then update these
-- from the regenerated manifest in the same change.
{% set expected_row_counts = {
    "acs5y2024_b01001": 616690,
    "acs5y2024_b03002": 543579,
    "acs5y2024_b15002": 543579,
    "acs5y2024_b19001": 543579,
    "acs5y2024_b19013": 543579,
    "acs5y2024_b19025": 537657,
    "acs5y2024_b23025": 543579,
    "acs5y2024_b25003": 616690,
    "acs5y2024_b25008": 543579,
    "acs5y2024_b25077": 543579,
    "acs5y2024_b28002": 543579,
    "acs5y2024_c17002": 543579,
    "census_ct_block_to_planning_region_2022": 49861,
} %}

with
    actual as (
        {% for t in expected_row_counts %}
            select '{{ t }}' as table_name, count(*) as actual_rows
            from {{ source("census_acs", t) }} {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    expected as (
        {% for t, n in expected_row_counts.items() %}
            select
                '{{ t }}' as table_name,
                {{ n }} as expected_rows
                {{ "union all" if not loop.last }}
        {% endfor %}
    )

select table_name, expected_rows, actual_rows
from expected
full outer join actual using (table_name)
where actual_rows is null or expected_rows is null or actual_rows != expected_rows
