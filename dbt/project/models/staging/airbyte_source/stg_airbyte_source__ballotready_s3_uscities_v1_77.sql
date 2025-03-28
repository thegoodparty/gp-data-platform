{{ config(materialized="view") }}

with
    source as (

        select * from {{ source("airbyte_source", "ballotready_s3_uscities_v1_77") }}

    ),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(id as int) as id,
            cast(cdp as boolean) as cdp,
            cast(lat as float) as lat,
            cast(lng as float) as lng,
            city,
            cast(male as decimal(3, 2)) as male,
            zips,
            cast(female as decimal(3, 2)) as female,
            source,
            cast(age_20s as decimal(3, 2)) as age_20s,
            cast(age_30s as decimal(3, 2)) as age_30s,
            cast(age_40s as decimal(3, 2)) as age_40s,
            cast(age_50s as decimal(3, 2)) as age_50s,
            cast(age_60s as decimal(3, 2)) as age_60s,
            cast(age_70s as decimal(3, 2)) as age_70s,
            cast(density as float) as density,
            cast(married as decimal(3, 2)) as married,
            cast(poverty as decimal(3, 2)) as poverty,
            cast(ranking as float) as ranking,
            cast(veteran as decimal(3, 2)) as veteran,
            cast(widowed as decimal(3, 2)) as widowed,
            city_alt,
            cast(csa_fips as int) as csa_fips,
            csa_name,
            cast(disabled as decimal(3, 2)) as disabled,
            cast(divorced as decimal(3, 2)) as divorced,
            cast(hispanic as decimal(3, 2)) as hispanic,
            cast(military as boolean) as military,
            state_id,
            timezone,
            cast(township as boolean) as township,
            cast(cbsa_fips as int) as cbsa_fips,
            cbsa_name,
            cast(age_median as decimal(3, 2)) as age_median,
            cast(cbsa_metro as boolean) as cbsa_metro,
            city_ascii,
            try_cast(home_value as float) as home_value,
            try_cast(population as int) as population,
            cast(race_asian as decimal(3, 2)) as race_asian,
            cast(race_black as decimal(3, 2)) as race_black,
            cast(race_other as decimal(3, 2)) as race_other,
            cast(race_white as decimal(3, 2)) as race_white,
            state_name,
            cast(age_over_18 as decimal(3, 2)) as age_over_18,
            cast(age_over_65 as decimal(3, 2)) as age_over_65,
            cast(age_over_80 as decimal(3, 2)) as age_over_80,
            county_fips,
            county_name,
            cast(family_size as float) as family_size,
            cast(race_native as decimal(3, 2)) as race_native,
            cast(rent_burden as decimal(3, 2)) as rent_burden,
            cast(rent_median as decimal(3, 2)) as rent_median,
            cast(age_10_to_19 as decimal(3, 2)) as age_10_to_19,
            cast(age_18_to_24 as decimal(3, 2)) as age_18_to_24,
            cast(age_under_10 as decimal(3, 2)) as age_under_10,
            cast(commute_time as float) as commute_time,
            cast(incorporated as boolean) as incorporated,
            cast(race_pacific as decimal(3, 2)) as race_pacific,
            cast(housing_units as int) as housing_units,
            cast(never_married as decimal(3, 2)) as never_married,
            cast(race_multiple as decimal(3, 2)) as race_multiple,
            cast(home_ownership as decimal(3, 2)) as home_ownership,
            county_fips_all,
            county_name_all,
            cast(limited_english as decimal(3, 2)) as limited_english,
            cast(health_uninsured as decimal(3, 2)) as health_uninsured,
            cast(population_proper as int) as population_proper,
            try_cast(unemployment_rate as decimal(3, 2)) as unemployment_rate,
            cast(education_graduate as decimal(3, 2)) as education_graduate,
            cast(family_dual_income as decimal(3, 2)) as family_dual_income,
            _ab_source_file_url,
            cast(education_bachelors as decimal(3, 2)) as education_bachelors,
            cast(education_highschool as decimal(3, 2)) as education_highschool,
            cast(education_stem_degree as decimal(3, 2)) as education_stem_degree,
            cast(education_some_college as decimal(3, 2)) as education_some_college,
            try_cast(income_household_median as float) as income_household_median,
            cast(income_household_5_to_10 as float) as income_household_5_to_10,
            cast(income_household_under_5 as float) as income_household_under_5,
            cast(income_individual_median as float) as income_individual_median,
            cast(education_less_highschool as float) as education_less_highschool,
            cast(income_household_10_to_15 as float) as income_household_10_to_15,
            cast(income_household_150_over as float) as income_household_150_over,
            cast(income_household_15_to_20 as float) as income_household_15_to_20,
            cast(income_household_20_to_25 as float) as income_household_20_to_25,
            cast(income_household_25_to_35 as float) as income_household_25_to_35,
            cast(income_household_35_to_50 as float) as income_household_35_to_50,
            cast(income_household_50_to_75 as float) as income_household_50_to_75,
            cast(labor_force_participation as float) as labor_force_participation,
            cast(education_college_or_above as float) as education_college_or_above,
            cast(income_household_75_to_100 as float) as income_household_75_to_100,
            cast(income_household_100_to_150 as float) as income_household_100_to_150,
            cast(income_household_six_figure as float) as income_household_six_figure,
            _ab_source_file_last_modified

        from source

    )

select *
from renamed
