{{ config(severity="error") }}

-- Every staged estimate and margin pair must equal an independent
-- recomputation from the raw tables at two fixed rows: the national row and
-- state 06. Estimates compare null-safe and exact; margins compare within
-- 1e-6 (two floating-point paths). One-sided nulls are mismatches.
{% set clean_estimate = (
    "case when cast({c} as bigint) in (-999999999, -888888888, -666666666)"
    " then null else cast({c} as bigint) end"
) %}
{% set clean_margin = (
    "case when cast({c} as bigint) = -555555555 then cast(0 as double)"
    " when cast({c} as bigint) in (-999999999, -888888888, -333333333, -222222222)"
    " then null else cast({c} as double) end"
) %}
{% set fixture_filter = "geo_id in ('0100000US', '0400000US06')" %}
{% set fixture_keys = (
    "case geo_id when '0100000US' then '010' else '040' end as summary_level,"
    " case geo_id when '0100000US' then '' else '06' end as geoid"
) %}
{% set under_18_cells = [
    "b01001_e003",
    "b01001_e004",
    "b01001_e005",
    "b01001_e006",
    "b01001_e027",
    "b01001_e028",
    "b01001_e029",
    "b01001_e030",
] %}
{% set age_65_plus_cells = [
    "b01001_e020",
    "b01001_e021",
    "b01001_e022",
    "b01001_e023",
    "b01001_e024",
    "b01001_e025",
    "b01001_e044",
    "b01001_e045",
    "b01001_e046",
    "b01001_e047",
    "b01001_e048",
    "b01001_e049",
] %}
{% set other_nh_cells = ["b03002_e007", "b03002_e008", "b03002_e009"] %}
{% set hs_or_higher_cells = [
    "b15002_e011",
    "b15002_e012",
    "b15002_e013",
    "b15002_e014",
    "b15002_e015",
    "b15002_e016",
    "b15002_e017",
    "b15002_e018",
    "b15002_e028",
    "b15002_e029",
    "b15002_e030",
    "b15002_e031",
    "b15002_e032",
    "b15002_e033",
    "b15002_e034",
    "b15002_e035",
] %}
{% set bachelors_cells = [
    "b15002_e015",
    "b15002_e016",
    "b15002_e017",
    "b15002_e018",
    "b15002_e032",
    "b15002_e033",
    "b15002_e034",
    "b15002_e035",
] %}
{% set below_poverty_cells = ["c17002_e002", "c17002_e003"] %}

with
    staged as (
        select *
        from {{ ref("stg_census_acs__geo_estimates") }}
        where
            (summary_level = '010' and geoid = '')
            or (summary_level = '040' and geoid = '06')
    ),

    b01001 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b01001_e001") }} as total_population,
            {{ clean_margin.format(c="b01001_m001") }} as total_population_moe,
            (
                {% for c in under_18_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_under_18,
            sqrt(
                {% for c in under_18_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_under_18_moe,
            (
                {% for c in age_65_plus_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_65_plus,
            sqrt(
                {% for c in age_65_plus_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_65_plus_moe
        from {{ source("census_acs", "acs5y2024_b01001") }}
        where {{ fixture_filter }}
    ),

    b03002 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b03002_e001") }} as population_race_universe,
            {{ clean_margin.format(c="b03002_m001") }} as population_race_universe_moe,
            {{ clean_estimate.format(c="b03002_e003") }} as population_white_nh,
            {{ clean_margin.format(c="b03002_m003") }} as population_white_nh_moe,
            {{ clean_estimate.format(c="b03002_e004") }} as population_black_nh,
            {{ clean_margin.format(c="b03002_m004") }} as population_black_nh_moe,
            {{ clean_estimate.format(c="b03002_e005") }}
            as population_native_american_nh,
            {{ clean_margin.format(c="b03002_m005") }}
            as population_native_american_nh_moe,
            {{ clean_estimate.format(c="b03002_e006") }} as population_asian_nh,
            {{ clean_margin.format(c="b03002_m006") }} as population_asian_nh_moe,
            (
                {% for c in other_nh_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_other_nh,
            sqrt(
                {% for c in other_nh_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_other_nh_moe,
            {{ clean_estimate.format(c="b03002_e012") }} as population_hispanic,
            {{ clean_margin.format(c="b03002_m012") }} as population_hispanic_moe
        from {{ source("census_acs", "acs5y2024_b03002") }}
        where {{ fixture_filter }}
    ),

    b15002 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b15002_e001") }} as population_25_plus,
            {{ clean_margin.format(c="b15002_m001") }} as population_25_plus_moe,
            (
                {% for c in hs_or_higher_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_hs_or_higher,
            sqrt(
                {% for c in hs_or_higher_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_hs_or_higher_moe,
            (
                {% for c in bachelors_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_bachelors_or_higher,
            sqrt(
                {% for c in bachelors_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_bachelors_or_higher_moe
        from {{ source("census_acs", "acs5y2024_b15002") }}
        where {{ fixture_filter }}
    ),

    b19025 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b19025_e001") }} as aggregate_household_income,
            {{ clean_margin.format(c="b19025_m001") }} as aggregate_household_income_moe
        from {{ source("census_acs", "acs5y2024_b19025") }}
        where {{ fixture_filter }}
    ),

    b23025 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b23025_e003") }} as civilian_labor_force,
            {{ clean_margin.format(c="b23025_m003") }} as civilian_labor_force_moe,
            {{ clean_estimate.format(c="b23025_e005") }} as unemployed_population,
            {{ clean_margin.format(c="b23025_m005") }} as unemployed_population_moe
        from {{ source("census_acs", "acs5y2024_b23025") }}
        where {{ fixture_filter }}
    ),

    b25003 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b25003_e001") }} as households,
            {{ clean_margin.format(c="b25003_m001") }} as households_moe,
            {{ clean_estimate.format(c="b25003_e002") }} as owner_occupied_households,
            {{ clean_margin.format(c="b25003_m002") }} as owner_occupied_households_moe
        from {{ source("census_acs", "acs5y2024_b25003") }}
        where {{ fixture_filter }}
    ),

    b25008 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b25008_e001") }} as population_in_households,
            {{ clean_margin.format(c="b25008_m001") }} as population_in_households_moe
        from {{ source("census_acs", "acs5y2024_b25008") }}
        where {{ fixture_filter }}
    ),

    b28002 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b28002_e001") }}
            as households_broadband_universe,
            {{ clean_margin.format(c="b28002_m001") }}
            as households_broadband_universe_moe,
            {{ clean_estimate.format(c="b28002_e004") }} as households_with_broadband,
            {{ clean_margin.format(c="b28002_m004") }} as households_with_broadband_moe
        from {{ source("census_acs", "acs5y2024_b28002") }}
        where {{ fixture_filter }}
    ),

    c17002 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="c17002_e001") }} as population_poverty_universe,
            {{ clean_margin.format(c="c17002_m001") }}
            as population_poverty_universe_moe,
            (
                {% for c in below_poverty_cells %}
                    {{ clean_estimate.format(c=c) }} {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_below_poverty,
            sqrt(
                {% for c in below_poverty_cells %}
                    pow(
                        {{ clean_margin.format(c=c.replace("_e", "_m")) }}, 2
                    ) {{ "+" if not loop.last }}
                {% endfor %}
            ) as population_below_poverty_moe
        from {{ source("census_acs", "acs5y2024_c17002") }}
        where {{ fixture_filter }}
    ),

    b19013 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b19013_e001") }} as median_household_income,
            {{ clean_margin.format(c="b19013_m001") }} as median_household_income_moe
        from {{ source("census_acs", "acs5y2024_b19013") }}
        where {{ fixture_filter }}
    ),

    b25077 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b25077_e001") }} as median_home_value,
            {{ clean_margin.format(c="b25077_m001") }} as median_home_value_moe
        from {{ source("census_acs", "acs5y2024_b25077") }}
        where {{ fixture_filter }}
    ),

    b19001 as (
        select
            {{ fixture_keys }},
            {{ clean_estimate.format(c="b19001_e001") }} as households_income_universe,
            {{ clean_margin.format(c="b19001_m001") }}
            as households_income_universe_moe,
            {% set bracket_names = [
                "under_10k",
                "10k_to_15k",
                "15k_to_20k",
                "20k_to_25k",
                "25k_to_30k",
                "30k_to_35k",
                "35k_to_40k",
                "40k_to_45k",
                "45k_to_50k",
                "50k_to_60k",
                "60k_to_75k",
                "75k_to_100k",
                "100k_to_125k",
                "125k_to_150k",
                "150k_to_200k",
                "200k_plus",
            ] %}
            {% for name in bracket_names %}
                {% set cell = "b19001_e%03d" | format(loop.index + 1) %}
                {{ clean_estimate.format(c=cell) }} as households_income_{{ name }},
                {{ clean_margin.format(c=cell.replace("_e", "_m")) }}
                as households_income_{{ name }}_moe{{ "," if not loop.last }}
            {% endfor %}
        from {{ source("census_acs", "acs5y2024_b19001") }}
        where {{ fixture_filter }}
    ),

    recomputed as (
        select *
        from b01001
        inner join b03002 using (summary_level, geoid)
        inner join b15002 using (summary_level, geoid)
        inner join b19025 using (summary_level, geoid)
        inner join b23025 using (summary_level, geoid)
        inner join b25003 using (summary_level, geoid)
        inner join b25008 using (summary_level, geoid)
        inner join b28002 using (summary_level, geoid)
        inner join c17002 using (summary_level, geoid)
        inner join b19013 using (summary_level, geoid)
        inner join b25077 using (summary_level, geoid)
        inner join b19001 using (summary_level, geoid)
    ),

    {% set estimate_columns = [
        "total_population",
        "population_under_18",
        "population_65_plus",
        "population_race_universe",
        "population_white_nh",
        "population_black_nh",
        "population_native_american_nh",
        "population_asian_nh",
        "population_other_nh",
        "population_hispanic",
        "population_25_plus",
        "population_hs_or_higher",
        "population_bachelors_or_higher",
        "aggregate_household_income",
        "civilian_labor_force",
        "unemployed_population",
        "households",
        "owner_occupied_households",
        "population_in_households",
        "households_broadband_universe",
        "households_with_broadband",
        "population_poverty_universe",
        "population_below_poverty",
        "median_household_income",
        "median_home_value",
        "households_income_universe",
        "households_income_under_10k",
        "households_income_10k_to_15k",
        "households_income_15k_to_20k",
        "households_income_20k_to_25k",
        "households_income_25k_to_30k",
        "households_income_30k_to_35k",
        "households_income_35k_to_40k",
        "households_income_40k_to_45k",
        "households_income_45k_to_50k",
        "households_income_50k_to_60k",
        "households_income_60k_to_75k",
        "households_income_75k_to_100k",
        "households_income_100k_to_125k",
        "households_income_125k_to_150k",
        "households_income_150k_to_200k",
        "households_income_200k_plus",
    ] %}
    compared as (
        select
            staged.summary_level,
            staged.geoid,
            {% for col in estimate_columns %}
                (
                    staged.{{ col }} is not distinct from recomputed.{{ col }}
                ) as {{ col }}_matches,
                coalesce(
                    abs(staged.{{ col }}_moe - recomputed.{{ col }}_moe) <= 1e-6,
                    staged.{{ col }}_moe is null and recomputed.{{ col }}_moe is null
                ) as {{ col }}_moe_matches
                {{ "," if not loop.last }}
            {% endfor %}
        from staged
        inner join recomputed using (summary_level, geoid)
    )

select *
from compared
where
    not (
        {% for col in estimate_columns %}
            {{ col }}_matches and {{ col }}_moe_matches {{ "and" if not loop.last }}
        {% endfor %}
    )
