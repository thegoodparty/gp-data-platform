{{ config(severity="error") }}

-- Guard for the controlled-margin encoding (a staged margin of 0 means
-- "controlled to an official total"): the encoding is lossless only if the
-- raw files never publish a literal zero margin, so this asserts no retained
-- row in any table carries a margin of exactly 0. If it ever fails, the
-- encoding premise breaks and the decision goes back to review before
-- anything consumes staged margins.
{% set expected_e_columns = {
    "acs5y2024_b01001": 49,
    "acs5y2024_b03002": 21,
    "acs5y2024_b15002": 35,
    "acs5y2024_b19001": 17,
    "acs5y2024_b19013": 1,
    "acs5y2024_b19025": 1,
    "acs5y2024_b23025": 7,
    "acs5y2024_b25003": 3,
    "acs5y2024_b25008": 3,
    "acs5y2024_b25077": 1,
    "acs5y2024_b28002": 13,
    "acs5y2024_c17002": 8,
} %}

{% for t, n in expected_e_columns.items() %}
    select '{{ t }}' as table_name, count(*) as rows_with_a_zero_margin
    from {{ source("census_acs", t) }}
    where
        ({{ census_acs_retained_geo_rows() }})
        and (
            {% for i in range(1, n + 1) %}
                cast(
                    {{ t | replace("acs5y2024_", "") }}_m{{ "%03d" | format(i) }}
                    as bigint
                )
                = 0
                {{ "or" if not loop.last }}
            {% endfor %}
        )
    having count(*) > 0 {{ "union all" if not loop.last }}
{% endfor %}
