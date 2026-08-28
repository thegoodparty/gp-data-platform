{{ config(severity="error") }}

-- Raw-margin integrity wall over exactly the margin columns staging consumes
-- (composite components plus single-cell margins), two guards in one sweep.
-- (1) Controlled-encoding guard: a staged margin of 0 means "controlled to an
-- official total", which is lossless only if the raw files never publish a
-- literal zero margin -- so a raw 0 fails here. (2) Sentinel authority guard:
-- the staging macros map exactly the five documented margin sentinels, so ANY
-- other negative margin value (an undocumented sentinel -- for example an
-- estimate-side code leaking into a margin column) fails here at the source,
-- before root-sum-of-squares could ever square it into a plausible-looking
-- number. Deliberate fail-loud posture: no speculative mappings for codes the
-- authority does not document. Unconsumed margin columns are deliberately out
-- of scope: the source is static and hash-locked, and staging never reads
-- them.
{% set consumed_margins = {
    "acs5y2024_b01001": [
        1,
        3,
        4,
        5,
        6,
        20,
        21,
        22,
        23,
        24,
        25,
        27,
        28,
        29,
        30,
        44,
        45,
        46,
        47,
        48,
        49,
    ],
    "acs5y2024_b03002": [1, 3, 4, 5, 6, 7, 8, 9, 12],
    "acs5y2024_b15002": [
        1,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
    ],
    "acs5y2024_b19001": [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
    ],
    "acs5y2024_b19013": [1],
    "acs5y2024_b19025": [1],
    "acs5y2024_b23025": [3, 5],
    "acs5y2024_b25003": [1, 2],
    "acs5y2024_b25008": [1],
    "acs5y2024_b25077": [1],
    "acs5y2024_b28002": [1, 4],
    "acs5y2024_c17002": [1, 2, 3],
} %}

{% for t, margins in consumed_margins.items() %}
    select '{{ t }}' as table_name, count(*) as offending_margin_rows
    from {{ source("census_acs", t) }}
    where
        ({{ census_acs_retained_geo_rows() }})
        and (
            {% for i in margins %}
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
