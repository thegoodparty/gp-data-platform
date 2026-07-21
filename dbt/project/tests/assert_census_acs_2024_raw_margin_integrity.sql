{{ config(severity="error") }}

-- Raw-margin integrity wall, two guards in one sweep of every retained
-- margin cell. (1) Controlled-encoding guard: a staged margin of 0 means
-- "controlled to an official total", which is lossless only if the raw files
-- never publish a literal zero margin -- so a raw 0 fails here. (2) Sentinel
-- authority guard: the staging macros map exactly the five documented margin
-- sentinels, so ANY other negative margin value (an undocumented sentinel --
-- for example an estimate-side code leaking into a margin column) fails here
-- at the source, before root-sum-of-squares could ever square it into a
-- plausible-looking number. Deliberate fail-loud posture: no speculative
-- mappings for codes the authority does not document. If this ever fails, the
-- mapping decision goes back to review before anything consumes staged
-- margins.
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
    select '{{ t }}' as table_name, count(*) as offending_margin_rows
    from {{ source("census_acs", t) }}
    where
        ({{ census_acs_retained_geo_rows() }})
        and (
            {% for i in range(1, n + 1) %}
                {% set margin = t | replace("acs5y2024_", "") ~ "_m" ~ (
                    "%03d" | format(i)
                ) %}
                cast({{ margin }} as bigint) = 0
                or (
                    cast({{ margin }} as bigint) < 0
                    and cast({{ margin }} as bigint)
                    not in (-999999999, -888888888, -555555555, -333333333, -222222222)
                )
                {{ "or" if not loop.last }}
            {% endfor %}
        )
    having count(*) > 0 {{ "union all" if not loop.last }}
{% endfor %}
