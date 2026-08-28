-- Materialized as a table (this directory's default is view) because twelve
-- pipe-delimited national raw tables are joined below and every downstream
-- build or validation query through a view would re-scan them all; the source
-- is a one-time load, so the table is small, cheap, and stable.
{{ config(materialized="table") }}

-- One row per (summary_level, geoid) from the ACS 2020-2024 5-year summary
-- file, semantic estimate + margin pairs only, counts-first: raw counts and
-- additive dollar aggregates (plus the two never-allocated published medians),
-- never pre-derived shares. GEO_ID anatomy is summary level (3) + geographic
-- component (4) + 'US' + geoid: only component 0000 (complete geography) rows
-- are kept, because the files also publish urban/rural component slices of
-- nation and state rows that would otherwise duplicate the key. Retained
-- levels: block group 150 and state 040 (the allocation inputs) plus county
-- 050, place 160, national 010, and school districts 950/960/970 (published
-- validation surfaces and later place-page work). The national geoid parses to
-- an empty string. Jam-code, margin, and row-filter semantics live in the
-- census_acs_* macros: margins are null when not computable and 0 exactly when
-- the estimate is controlled to an official total.
with
    b01001 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b01001_e001") }} as total_population,
            {{ census_acs_moe("b01001_m001") }} as total_population_moe,
            {{
                census_acs_estimate_sum(
                    [
                        "b01001_e003",
                        "b01001_e004",
                        "b01001_e005",
                        "b01001_e006",
                        "b01001_e027",
                        "b01001_e028",
                        "b01001_e029",
                        "b01001_e030",
                    ]
                )
            }} as population_under_18,
            {{
                census_acs_moe_rss(
                    [
                        "b01001_m003",
                        "b01001_m004",
                        "b01001_m005",
                        "b01001_m006",
                        "b01001_m027",
                        "b01001_m028",
                        "b01001_m029",
                        "b01001_m030",
                    ]
                )
            }} as population_under_18_moe,
            {{
                census_acs_estimate_sum(
                    [
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
                    ]
                )
            }} as population_65_plus,
            {{
                census_acs_moe_rss(
                    [
                        "b01001_m020",
                        "b01001_m021",
                        "b01001_m022",
                        "b01001_m023",
                        "b01001_m024",
                        "b01001_m025",
                        "b01001_m044",
                        "b01001_m045",
                        "b01001_m046",
                        "b01001_m047",
                        "b01001_m048",
                        "b01001_m049",
                    ]
                )
            }} as population_65_plus_moe
        from {{ source("census_acs", "acs5y2024_b01001") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b03002 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b03002_e001") }} as population_race_universe,
            {{ census_acs_moe("b03002_m001") }} as population_race_universe_moe,
            {{ census_acs_estimate("b03002_e003") }} as population_white_nh,
            {{ census_acs_moe("b03002_m003") }} as population_white_nh_moe,
            {{ census_acs_estimate("b03002_e004") }} as population_black_nh,
            {{ census_acs_moe("b03002_m004") }} as population_black_nh_moe,
            {{ census_acs_estimate("b03002_e005") }} as population_native_american_nh,
            {{ census_acs_moe("b03002_m005") }} as population_native_american_nh_moe,
            {{ census_acs_estimate("b03002_e006") }} as population_asian_nh,
            {{ census_acs_moe("b03002_m006") }} as population_asian_nh_moe,
            {{
                census_acs_estimate_sum(
                    [
                        "b03002_e007",
                        "b03002_e008",
                        "b03002_e009",
                    ]
                )
            }} as population_other_nh,
            {{
                census_acs_moe_rss(
                    [
                        "b03002_m007",
                        "b03002_m008",
                        "b03002_m009",
                    ]
                )
            }} as population_other_nh_moe,
            {{ census_acs_estimate("b03002_e012") }} as population_hispanic,
            {{ census_acs_moe("b03002_m012") }} as population_hispanic_moe
        from {{ source("census_acs", "acs5y2024_b03002") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b15002 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b15002_e001") }} as population_25_plus,
            {{ census_acs_moe("b15002_m001") }} as population_25_plus_moe,
            {{
                census_acs_estimate_sum(
                    [
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
                    ]
                )
            }} as population_hs_or_higher,
            {{
                census_acs_moe_rss(
                    [
                        "b15002_m011",
                        "b15002_m012",
                        "b15002_m013",
                        "b15002_m014",
                        "b15002_m015",
                        "b15002_m016",
                        "b15002_m017",
                        "b15002_m018",
                        "b15002_m028",
                        "b15002_m029",
                        "b15002_m030",
                        "b15002_m031",
                        "b15002_m032",
                        "b15002_m033",
                        "b15002_m034",
                        "b15002_m035",
                    ]
                )
            }} as population_hs_or_higher_moe,
            {{
                census_acs_estimate_sum(
                    [
                        "b15002_e015",
                        "b15002_e016",
                        "b15002_e017",
                        "b15002_e018",
                        "b15002_e032",
                        "b15002_e033",
                        "b15002_e034",
                        "b15002_e035",
                    ]
                )
            }} as population_bachelors_or_higher,
            {{
                census_acs_moe_rss(
                    [
                        "b15002_m015",
                        "b15002_m016",
                        "b15002_m017",
                        "b15002_m018",
                        "b15002_m032",
                        "b15002_m033",
                        "b15002_m034",
                        "b15002_m035",
                    ]
                )
            }} as population_bachelors_or_higher_moe
        from {{ source("census_acs", "acs5y2024_b15002") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b19025 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b19025_e001") }} as aggregate_household_income,
            {{ census_acs_moe("b19025_m001") }} as aggregate_household_income_moe
        from {{ source("census_acs", "acs5y2024_b19025") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b23025 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b23025_e003") }} as civilian_labor_force,
            {{ census_acs_moe("b23025_m003") }} as civilian_labor_force_moe,
            {{ census_acs_estimate("b23025_e005") }} as unemployed_population,
            {{ census_acs_moe("b23025_m005") }} as unemployed_population_moe
        from {{ source("census_acs", "acs5y2024_b23025") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b25003 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b25003_e001") }} as households,
            {{ census_acs_moe("b25003_m001") }} as households_moe,
            {{ census_acs_estimate("b25003_e002") }} as owner_occupied_households,
            {{ census_acs_moe("b25003_m002") }} as owner_occupied_households_moe
        from {{ source("census_acs", "acs5y2024_b25003") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b25008 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b25008_e001") }} as population_in_households,
            {{ census_acs_moe("b25008_m001") }} as population_in_households_moe
        from {{ source("census_acs", "acs5y2024_b25008") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b28002 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b28002_e001") }} as households_broadband_universe,
            {{ census_acs_moe("b28002_m001") }} as households_broadband_universe_moe,
            {{ census_acs_estimate("b28002_e004") }} as households_with_broadband,
            {{ census_acs_moe("b28002_m004") }} as households_with_broadband_moe
        from {{ source("census_acs", "acs5y2024_b28002") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    c17002 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("c17002_e001") }} as population_poverty_universe,
            {{ census_acs_moe("c17002_m001") }} as population_poverty_universe_moe,
            {{
                census_acs_estimate_sum(
                    [
                        "c17002_e002",
                        "c17002_e003",
                    ]
                )
            }} as population_below_poverty,
            {{
                census_acs_moe_rss(
                    [
                        "c17002_m002",
                        "c17002_m003",
                    ]
                )
            }} as population_below_poverty_moe
        from {{ source("census_acs", "acs5y2024_c17002") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b19013 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b19013_e001") }} as median_household_income,
            {{ census_acs_moe("b19013_m001") }} as median_household_income_moe
        from {{ source("census_acs", "acs5y2024_b19013") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b25077 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b25077_e001") }} as median_home_value,
            {{ census_acs_moe("b25077_m001") }} as median_home_value_moe
        from {{ source("census_acs", "acs5y2024_b25077") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    b19001 as (
        select
            left(geo_id, 3) as summary_level,
            substring(geo_id, 10) as geoid,
            {{ census_acs_estimate("b19001_e001") }} as households_income_universe,
            {{ census_acs_moe("b19001_m001") }} as households_income_universe_moe,
            {{ census_acs_estimate("b19001_e002") }} as households_income_under_10k,
            {{ census_acs_moe("b19001_m002") }} as households_income_under_10k_moe,
            {{ census_acs_estimate("b19001_e003") }} as households_income_10k_to_15k,
            {{ census_acs_moe("b19001_m003") }} as households_income_10k_to_15k_moe,
            {{ census_acs_estimate("b19001_e004") }} as households_income_15k_to_20k,
            {{ census_acs_moe("b19001_m004") }} as households_income_15k_to_20k_moe,
            {{ census_acs_estimate("b19001_e005") }} as households_income_20k_to_25k,
            {{ census_acs_moe("b19001_m005") }} as households_income_20k_to_25k_moe,
            {{ census_acs_estimate("b19001_e006") }} as households_income_25k_to_30k,
            {{ census_acs_moe("b19001_m006") }} as households_income_25k_to_30k_moe,
            {{ census_acs_estimate("b19001_e007") }} as households_income_30k_to_35k,
            {{ census_acs_moe("b19001_m007") }} as households_income_30k_to_35k_moe,
            {{ census_acs_estimate("b19001_e008") }} as households_income_35k_to_40k,
            {{ census_acs_moe("b19001_m008") }} as households_income_35k_to_40k_moe,
            {{ census_acs_estimate("b19001_e009") }} as households_income_40k_to_45k,
            {{ census_acs_moe("b19001_m009") }} as households_income_40k_to_45k_moe,
            {{ census_acs_estimate("b19001_e010") }} as households_income_45k_to_50k,
            {{ census_acs_moe("b19001_m010") }} as households_income_45k_to_50k_moe,
            {{ census_acs_estimate("b19001_e011") }} as households_income_50k_to_60k,
            {{ census_acs_moe("b19001_m011") }} as households_income_50k_to_60k_moe,
            {{ census_acs_estimate("b19001_e012") }} as households_income_60k_to_75k,
            {{ census_acs_moe("b19001_m012") }} as households_income_60k_to_75k_moe,
            {{ census_acs_estimate("b19001_e013") }} as households_income_75k_to_100k,
            {{ census_acs_moe("b19001_m013") }} as households_income_75k_to_100k_moe,
            {{ census_acs_estimate("b19001_e014") }} as households_income_100k_to_125k,
            {{ census_acs_moe("b19001_m014") }} as households_income_100k_to_125k_moe,
            {{ census_acs_estimate("b19001_e015") }} as households_income_125k_to_150k,
            {{ census_acs_moe("b19001_m015") }} as households_income_125k_to_150k_moe,
            {{ census_acs_estimate("b19001_e016") }} as households_income_150k_to_200k,
            {{ census_acs_moe("b19001_m016") }} as households_income_150k_to_200k_moe,
            {{ census_acs_estimate("b19001_e017") }} as households_income_200k_plus,
            {{ census_acs_moe("b19001_m017") }} as households_income_200k_plus_moe
        from {{ source("census_acs", "acs5y2024_b19001") }}
        where {{ census_acs_retained_geo_rows() }}
    ),

    -- b01001 keys are THE canonical spine: the retained-key alignment test
    -- pins the other strict tables' key sets identical to b01001's and b19025
    -- a subset of it, so the eleven left joins below can never invent or drop
    -- a geography row.
    final as (
        select
            b01001.summary_level,
            b01001.geoid,
            b01001.total_population,
            b01001.total_population_moe,
            b01001.population_under_18,
            b01001.population_under_18_moe,
            b01001.population_65_plus,
            b01001.population_65_plus_moe,
            b03002.population_race_universe,
            b03002.population_race_universe_moe,
            b03002.population_white_nh,
            b03002.population_white_nh_moe,
            b03002.population_black_nh,
            b03002.population_black_nh_moe,
            b03002.population_native_american_nh,
            b03002.population_native_american_nh_moe,
            b03002.population_asian_nh,
            b03002.population_asian_nh_moe,
            b03002.population_other_nh,
            b03002.population_other_nh_moe,
            b03002.population_hispanic,
            b03002.population_hispanic_moe,
            b15002.population_25_plus,
            b15002.population_25_plus_moe,
            b15002.population_hs_or_higher,
            b15002.population_hs_or_higher_moe,
            b15002.population_bachelors_or_higher,
            b15002.population_bachelors_or_higher_moe,
            b19025.aggregate_household_income,
            b19025.aggregate_household_income_moe,
            b23025.civilian_labor_force,
            b23025.civilian_labor_force_moe,
            b23025.unemployed_population,
            b23025.unemployed_population_moe,
            b25003.households,
            b25003.households_moe,
            b25003.owner_occupied_households,
            b25003.owner_occupied_households_moe,
            b25008.population_in_households,
            b25008.population_in_households_moe,
            b28002.households_broadband_universe,
            b28002.households_broadband_universe_moe,
            b28002.households_with_broadband,
            b28002.households_with_broadband_moe,
            c17002.population_poverty_universe,
            c17002.population_poverty_universe_moe,
            c17002.population_below_poverty,
            c17002.population_below_poverty_moe,
            b19013.median_household_income,
            b19013.median_household_income_moe,
            b25077.median_home_value,
            b25077.median_home_value_moe,
            b19001.households_income_universe,
            b19001.households_income_universe_moe,
            b19001.households_income_under_10k,
            b19001.households_income_under_10k_moe,
            b19001.households_income_10k_to_15k,
            b19001.households_income_10k_to_15k_moe,
            b19001.households_income_15k_to_20k,
            b19001.households_income_15k_to_20k_moe,
            b19001.households_income_20k_to_25k,
            b19001.households_income_20k_to_25k_moe,
            b19001.households_income_25k_to_30k,
            b19001.households_income_25k_to_30k_moe,
            b19001.households_income_30k_to_35k,
            b19001.households_income_30k_to_35k_moe,
            b19001.households_income_35k_to_40k,
            b19001.households_income_35k_to_40k_moe,
            b19001.households_income_40k_to_45k,
            b19001.households_income_40k_to_45k_moe,
            b19001.households_income_45k_to_50k,
            b19001.households_income_45k_to_50k_moe,
            b19001.households_income_50k_to_60k,
            b19001.households_income_50k_to_60k_moe,
            b19001.households_income_60k_to_75k,
            b19001.households_income_60k_to_75k_moe,
            b19001.households_income_75k_to_100k,
            b19001.households_income_75k_to_100k_moe,
            b19001.households_income_100k_to_125k,
            b19001.households_income_100k_to_125k_moe,
            b19001.households_income_125k_to_150k,
            b19001.households_income_125k_to_150k_moe,
            b19001.households_income_150k_to_200k,
            b19001.households_income_150k_to_200k_moe,
            b19001.households_income_200k_plus,
            b19001.households_income_200k_plus_moe
        from b01001
        left join b03002 using (summary_level, geoid)
        left join b15002 using (summary_level, geoid)
        left join b19025 using (summary_level, geoid)
        left join b23025 using (summary_level, geoid)
        left join b25003 using (summary_level, geoid)
        left join b25008 using (summary_level, geoid)
        left join b28002 using (summary_level, geoid)
        left join c17002 using (summary_level, geoid)
        left join b19013 using (summary_level, geoid)
        left join b25077 using (summary_level, geoid)
        left join b19001 using (summary_level, geoid)
    )

select *
from final
