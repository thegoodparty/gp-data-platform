{{ config(severity="error") }}

-- B01001's cells partition each sex by age, and the two sexes partition the
-- total. The Census publishes the table internally consistent, so these hold
-- exactly wherever every cell is present: a miswired or misnamed cell in
-- stg_census_acs__b01001 breaks one of them somewhere.
{%- set ages = [
    "under_5",
    "5_to_9",
    "10_to_14",
    "15_to_17",
    "18_to_19",
    "20",
    "21",
    "22_to_24",
    "25_to_29",
    "30_to_34",
    "35_to_39",
    "40_to_44",
    "45_to_49",
    "50_to_54",
    "55_to_59",
    "60_to_61",
    "62_to_64",
    "65_to_66",
    "67_to_69",
    "70_to_74",
    "75_to_79",
    "80_to_84",
    "85_plus",
] %}
with
    b01001 as (select * from {{ ref("stg_census_acs__b01001") }}),

    violations as (
        select
            summary_level,
            geoid,
            case
                when population_male + population_female != total_population
                then 'sexes_do_not_sum_to_total'
                {%- for sex in ["male", "female"] %}
                    when
                        (
                            {%- for age in ages %}
                                population_{{ sex }}_{{ age }}
                                {%- if not loop.last %} +{% endif %}
                            {%- endfor %}
                        )
                        != population_{{ sex }}
                    then '{{ sex }}_ages_do_not_sum_to_{{ sex }}'
                {%- endfor %}
            end as violation
        from b01001
    )

select *
from violations
where violation is not null
