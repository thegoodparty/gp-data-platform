-- depends_on: {{ ref("census_acs_b01001_cells") }}
{{ config(severity="error") }}

-- The range wall stg_census_acs__geo_estimates has, over every B01001 cell:
-- an unmapped jam sentinel would surface as a large negative estimate or
-- margin. Null (suppressed) passes. Columns come from the seed that
-- generates the model, so a new cell is covered without editing this test.
select summary_level, geoid
from {{ ref("stg_census_acs__b01001") }}
where
    {%- for cell, name in census_acs_b01001_cells() %}
        {{ name }} < 0 or {{ name }}_moe < 0{{ " or" if not loop.last }}
    {%- endfor %}
