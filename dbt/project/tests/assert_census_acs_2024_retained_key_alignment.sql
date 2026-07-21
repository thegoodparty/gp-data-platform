{{ config(severity="error") }}

-- Eleven of the twelve ACS raw tables must publish the exact same retained-key
-- set: the staging model left-joins them onto the union of their keys, so a
-- key missing from one table would surface as nulls indistinguishable from
-- jam-code suppression. The staged model's key set IS the union, so "staged
-- keys minus this table's keys" must be empty for each strict table.
--
-- Documented exception, deliberately structural: the aggregate-household-income
-- table (b19025) is the one source that expresses "no published value" for
-- some small geographies by OMITTING the row entirely instead of carrying a
-- jam-code sentinel (verified against the Census API, which returns null for
-- the same geographies). Its keys are therefore allowed to be a subset: a key
-- absent from b19025 must still be present in all eleven other tables (their
-- strict arms below enforce exactly that), b19025 may contribute no key the
-- staged spine lacks (its own arm below), and the staged aggregate-income
-- columns are null on the omitted rows with ordinary suppression semantics.
-- Its overall shape stays pinned by the manifest row-count test, the national
-- anchor, and the staged-versus-raw oracle.
{% set strict_acs_tables = [
    "acs5y2024_b01001",
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
    staged_keys as (
        select summary_level, geoid from {{ ref("stg_census_acs__geo_estimates") }}
    )

{% for t in strict_acs_tables %}
    select '{{ t }}' as table_name, staged_keys.summary_level, staged_keys.geoid
    from staged_keys
    left anti join
        (
            select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
            from {{ source("census_acs", t) }}
            where {{ census_acs_retained_geo_rows() }}
        ) as table_keys using (summary_level, geoid)
    union all
{% endfor %}
select
    'acs5y2024_b19025_extra_key' as table_name,
    b19025_keys.summary_level,
    b19025_keys.geoid
from
    (
        select left(geo_id, 3) as summary_level, substring(geo_id, 10) as geoid
        from {{ source("census_acs", "acs5y2024_b19025") }}
        where {{ census_acs_retained_geo_rows() }}
    ) as b19025_keys
left anti join staged_keys using (summary_level, geoid)
