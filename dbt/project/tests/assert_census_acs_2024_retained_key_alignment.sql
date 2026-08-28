{{ config(severity="error") }}

-- b01001 is THE canonical key spine: the staging model selects its keys and
-- left-joins every other table to them, so this contract is what makes that
-- join safe. Ten strict tables must match b01001's retained-key set exactly
-- (both directions), and the aggregate-household-income table (b19025) must
-- be a SUBSET of it: that is the one source that expresses "no published
-- value" for some small geographies by omitting the row entirely instead of
-- carrying a jam-code sentinel (verified against the Census API, which
-- returns null for the same geographies). Staged aggregate-income columns are
-- null on the omitted rows with ordinary suppression semantics; b19025's
-- overall shape stays pinned by the manifest row-count test and the national
-- anchor.
{% set strict_acs_tables = [
    "acs5y2024_b03002",
    "acs5y2024_b15002",
    "acs5y2024_b19001",
    "acs5y2024_b19013",
    "acs5y2024_b23025",
    "acs5y2024_b25003",
    "acs5y2024_b25008",
    "acs5y2024_b25077",
    "acs5y2024_b28002",
    "acs5y2024_c17002",
] %}

with
    b01001_keys as (
        select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
        from {{ source("census_acs", "acs5y2024_b01001") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b19025_keys as (
        select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
        from {{ source("census_acs", "acs5y2024_b19025") }}
        where {{ census_acs_retained_geo_rows() }}
    )

{% for t in strict_acs_tables %}
        {% set table_keys %}
        select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
        from {{ source("census_acs", t) }}
        where {{ census_acs_retained_geo_rows() }}
        {% endset %}
    select
        'key_missing_from_table' as violation,
        '{{ t }}' as table_name,
        b01001_keys.summary_level,
        b01001_keys.geoid
    from b01001_keys
    left anti join ({{ table_keys }}) as table_keys using (summary_level, geoid)
    union all
    select
        'key_not_in_spine' as violation,
        '{{ t }}' as table_name,
        table_keys.summary_level,
        table_keys.geoid
    from ({{ table_keys }}) as table_keys
    left anti join b01001_keys using (summary_level, geoid)
    union all
{% endfor %}
select
    'key_not_in_spine' as violation,
    'acs5y2024_b19025' as table_name,
    b19025_keys.summary_level,
    b19025_keys.geoid
from b19025_keys
left anti join b01001_keys using (summary_level, geoid)
