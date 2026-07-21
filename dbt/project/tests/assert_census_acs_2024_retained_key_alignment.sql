{{ config(severity="error") }}

-- Every ACS raw table must publish the exact same retained-key set. The
-- staging model left-joins all twelve tables onto the union of their keys, so
-- a key missing from one table would surface as nulls indistinguishable from
-- jam-code suppression. The staged model's key set IS the union, so "staged
-- keys minus this table's keys" being empty for all twelve proves every key
-- set identical (symmetric by construction). If a real publication gap ever
-- appears, encode it as an explicit documented exception here -- never
-- silence.
{% set acs_tables = [
    "acs5y2024_b01001",
    "acs5y2024_b03002",
    "acs5y2024_b15002",
    "acs5y2024_b19001",
    "acs5y2024_b19013",
    "acs5y2024_b19025",
    "acs5y2024_b23025",
    "acs5y2024_b25003",
    "acs5y2024_b25008",
    "acs5y2024_b25077",
    "acs5y2024_b28002",
    "acs5y2024_c17002",
] %}

with
    staged_keys as (
        select summary_level, geoid from {{ ref("stg_census_acs__geo_estimates") }}
    )

{% for t in acs_tables %}
    select '{{ t }}' as table_name, staged_keys.summary_level, staged_keys.geoid
    from staged_keys
    left anti join
        (
            select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
            from {{ source("census_acs", t) }}
            where {{ census_acs_retained_geo_rows() }}
        ) as table_keys using (summary_level, geoid)
        {{ "union all" if not loop.last }}
{% endfor %}
