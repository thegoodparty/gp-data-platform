-- ACS 2020-2024 5-year table B01001 (sex by age), every cell, one row per
-- (summary_level, geoid) at the retained summary levels. This is the one
-- place B01001's cell numbers become names: stg_census_acs__geo_estimates
-- builds its age and sex-by-age brackets from these columns, and a later
-- bracket (children by sex, a different adult cut) can too, without
-- re-reading the raw table or restating cell numbers. Jam codes map through
-- the census_acs_* macros, so null means suppressed and a 0 margin means
-- controlled.
{%- set cells = [
    (1, "total_population"),
    (2, "population_male"),
    (3, "population_male_under_5"),
    (4, "population_male_5_to_9"),
    (5, "population_male_10_to_14"),
    (6, "population_male_15_to_17"),
    (7, "population_male_18_to_19"),
    (8, "population_male_20"),
    (9, "population_male_21"),
    (10, "population_male_22_to_24"),
    (11, "population_male_25_to_29"),
    (12, "population_male_30_to_34"),
    (13, "population_male_35_to_39"),
    (14, "population_male_40_to_44"),
    (15, "population_male_45_to_49"),
    (16, "population_male_50_to_54"),
    (17, "population_male_55_to_59"),
    (18, "population_male_60_to_61"),
    (19, "population_male_62_to_64"),
    (20, "population_male_65_to_66"),
    (21, "population_male_67_to_69"),
    (22, "population_male_70_to_74"),
    (23, "population_male_75_to_79"),
    (24, "population_male_80_to_84"),
    (25, "population_male_85_plus"),
    (26, "population_female"),
    (27, "population_female_under_5"),
    (28, "population_female_5_to_9"),
    (29, "population_female_10_to_14"),
    (30, "population_female_15_to_17"),
    (31, "population_female_18_to_19"),
    (32, "population_female_20"),
    (33, "population_female_21"),
    (34, "population_female_22_to_24"),
    (35, "population_female_25_to_29"),
    (36, "population_female_30_to_34"),
    (37, "population_female_35_to_39"),
    (38, "population_female_40_to_44"),
    (39, "population_female_45_to_49"),
    (40, "population_female_50_to_54"),
    (41, "population_female_55_to_59"),
    (42, "population_female_60_to_61"),
    (43, "population_female_62_to_64"),
    (44, "population_female_65_to_66"),
    (45, "population_female_67_to_69"),
    (46, "population_female_70_to_74"),
    (47, "population_female_75_to_79"),
    (48, "population_female_80_to_84"),
    (49, "population_female_85_plus"),
] %}

select
    left(geo_id, 3) as summary_level,
    substring(geo_id, 10) as geoid,
    {%- for cell, name in cells %}
        {{ census_acs_estimate("b01001_e%03d" % cell) }} as {{ name }},
        {{ census_acs_moe("b01001_m%03d" % cell) }} as {{ name }}_moe
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ source("census_acs", "acs5y2024_b01001") }}
where {{ census_acs_retained_geo_rows() }}
