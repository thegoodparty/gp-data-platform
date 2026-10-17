{{ config(severity="error") }}

-- The adult sex-by-age columns gp-api weights by must add up on every
-- district row, the way they do in each block group: each sex's bands sum to
-- its 18+ total, the two 18+ totals equal total population less under-18,
-- and the two 65+ bands equal 65+. Allocation is linear, so local rows hold
-- these identities up to floating-point error; the tolerance is a millionth
-- of the district's population, floored at 0.01 of a person.
{%- set bands = [
    "18_to_24",
    "25_to_34",
    "35_to_44",
    "45_to_54",
    "55_to_64",
    "65_plus",
] %}
with
    districts as (select * from {{ ref("int__district_acs_estimates") }}),

    differences as (
        select
            state_postal_code,
            district_type,
            district_name,
            greatest(0.01, total_population * 1e-6) as tolerance,
            {%- for sex in ["male", "female"] %}
                population_{{ sex }}_18_plus - (
                    {%- for band in bands %}
                        population_{{ sex }}_{{ band }}
                        {%- if not loop.last %} +{% endif %}
                    {%- endfor %}
                ) as {{ sex }}_bands_off_by,
            {%- endfor %}
            population_male_18_plus
            + population_female_18_plus
            - (total_population - population_under_18) as adults_off_by,
            population_male_65_plus
            + population_female_65_plus
            - population_65_plus as seniors_off_by
        from districts
    )

select *
from differences
where
    abs(male_bands_off_by) > tolerance
    or abs(female_bands_off_by) > tolerance
    or abs(adults_off_by) > tolerance
    or abs(seniors_off_by) > tolerance
