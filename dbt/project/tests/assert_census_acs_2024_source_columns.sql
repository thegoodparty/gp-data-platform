{{ config(severity="error") }}

-- Exact raw-table column contract, generated from the published 2024 5-year
-- table shells: geo_id plus interleaved estimate/margin pairs 001..N per
-- table, every column string-typed (the raw layer is all-string by
-- convention; casting happens in staging). Flags missing columns, unexpected
-- columns, and non-string types by NAME, so a restructured source file or a
-- wrong load can never masquerade as suppression downstream.
{% set expected_e_columns = {
    "acs5y2024_b01001": 49,
    "acs5y2024_b03002": 21,
    "acs5y2024_b15002": 35,
    "acs5y2024_b19001": 17,
    "acs5y2024_b19013": 1,
    "acs5y2024_b19025": 1,
    "acs5y2024_b23025": 7,
    "acs5y2024_b25003": 3,
    "acs5y2024_b25008": 3,
    "acs5y2024_b25077": 1,
    "acs5y2024_b28002": 13,
    "acs5y2024_c17002": 8,
} %}

with
    expected as (
        {% for t, n in expected_e_columns.items() %}
            select '{{ t }}' as table_name, 'geo_id' as column_name
            {% for i in range(1, n + 1) %}
                union all
                select
                    '{{ t }}',
                    '{{ t | replace("acs5y2024_", "") }}_e{{ "%03d" | format(i) }}'
                union all
                select
                    '{{ t }}',
                    '{{ t | replace("acs5y2024_", "") }}_m{{ "%03d" | format(i) }}'
                {% endfor %} {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    actual as (
        select
            table_name, lower(column_name) as column_name, lower(data_type) as data_type
        from
            {{ source("census_acs", "acs5y2024_b01001").database }}.information_schema.columns
        where
            table_schema = '{{ source("census_acs", "acs5y2024_b01001").schema }}'
            and table_name in (
                {% for t in expected_e_columns %}
                    '{{ t }}'{{ ", " if not loop.last }}
                {% endfor %}
            )
    ),

    missing as (
        select 'missing_column' as violation, table_name, column_name
        from expected
        left anti join actual using (table_name, column_name)
    ),

    unexpected as (
        select 'unexpected_column' as violation, table_name, column_name
        from actual
        left anti join expected using (table_name, column_name)
    ),

    not_string as (
        select 'not_string_type' as violation, table_name, column_name
        from actual
        where data_type != 'string'
    )

select *
from missing
union all
select *
from unexpected
union all
select *
from not_string
