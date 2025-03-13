{{ config(materialized="view") }}

with
    source as (

        select *
        from {{ source("airbyte_source", "ballotready_s3_uscounties_v1_73_short") }}

    ),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            lat,
            lng,
            male,
            zips,
            county,
            female,
            age_20s,
            age_30s,
            age_40s,
            age_50s,
            age_60s,
            age_70s,
            density,
            married,
            poverty,
            veteran,
            widowed,
            bls_date,
            disabled,
            divorced,
            hispanic,
            state_id,
            timezone,
            age_median,
            home_value,
            population,
            race_asian,
            race_black,
            race_other,
            race_white,
            state_name,
            age_over_80,
            county_fips,
            county_full,
            family_size,
            labor_force,
            race_native,
            rent_burden,
            rent_median,
            age_10_to_19,
            age_under_10,
            city_largest,
            commute_time,
            county_ascii,
            race_pacific,
            timezone_all,
            never_married,
            race_multiple,
            home_ownership,
            city_largest_id,
            limited_english,
            health_uninsured,
            unemployment_rate,
            education_graduate,
            family_dual_income,
            _ab_source_file_url,
            education_bachelors,
            labor_force_average,
            education_highschool,
            education_stem_degree,
            education_some_college,
            income_household_median,
            income_household_5_to_10,
            income_household_under_5,
            income_individual_median,
            education_less_highschool,
            income_household_10_to_15,
            income_household_150_over,
            income_household_15_to_20,
            income_household_20_to_25,
            income_household_25_to_35,
            income_household_35_to_50,
            income_household_50_to_75,
            labor_force_participation,
            unemployment_rate_average,
            education_college_or_above,
            income_household_75_to_100,
            income_household_100_to_150,
            income_household_six_figure,
            _ab_source_file_last_modified

        from source

    )

select *
from renamed
