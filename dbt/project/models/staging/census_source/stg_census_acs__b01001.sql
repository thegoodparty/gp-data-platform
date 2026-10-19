-- depends_on: {{ ref("census_acs_b01001_cells") }}
-- ACS 2020-2024 5-year table B01001 (sex by age), every cell, one row per
-- (summary_level, geoid) at the retained summary levels. The
-- census_acs_b01001_cells seed is where B01001's cell numbers become names,
-- each beside the Census Bureau's label for it: this model generates one
-- estimate and one margin column per seed row, and
-- stg_census_acs__geo_estimates builds its age and sex-by-age brackets from
-- these columns, so a later bracket (children by sex, a different adult cut)
-- can too, without re-reading the raw table or restating cell numbers. Jam
-- codes map through the census_acs_* macros, so null means suppressed and a 0
-- margin means controlled.
{%- set cells = census_acs_b01001_cells() %}

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
