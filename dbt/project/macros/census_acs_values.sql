-- ACS summary-file value handling, used by census_acs staging and its
-- source-contract tests.
--
-- Jam codes are large negative sentinels the Census publishes in place of real
-- values. Exact-equality mapping, per the summary-file handbook: estimates
-- -999999999 / -888888888 / -666666666 -> null; margins -555555555 -> 0 (the
-- estimate is controlled to an official total, so zero sampling error is the
-- correct variance) and -999999999 / -888888888 / -333333333 / -222222222 ->
-- null. Nothing else becomes zero, so a 0 margin always MEANS controlled. Any
-- unmapped sentinel stays hugely negative and fails the staging range tests
-- loudly. Estimates cast bigint; margins cast double so single-cell and
-- root-sum-of-squares-combined margins share one type.
{% macro census_acs_estimate(column) %}
    case
        when cast({{ column }} as bigint) in (-999999999, -888888888, -666666666)
        then null
        else cast({{ column }} as bigint)
    end
{% endmacro %}

{% macro census_acs_moe(column) %}
    case
        when cast({{ column }} as bigint) = -555555555
        then cast(0 as double)
        when
            cast({{ column }} as bigint)
            in (-999999999, -888888888, -333333333, -222222222)
        then null
        else cast({{ column }} as double)
    end
{% endmacro %}

-- Bracket combinations: sum the component estimates; combine margins by plain
-- root sum of squares (the Census handbook aggregation rule; the sometimes-cited
-- zero-estimate refinement is not verifiable in the current handbook and is
-- deliberately not applied). A null component nulls the whole combination:
-- a partially suppressed bracket set is treated as suppressed.
{% macro census_acs_estimate_sum(columns) %}
    (
        {%- for column in columns %}
            {{ census_acs_estimate(column) }}{{ " +" if not loop.last }}
        {%- endfor %}
    )
{% endmacro %}

{% macro census_acs_moe_rss(columns) %}
    sqrt(
        {%- for column in columns %}
            pow({{ census_acs_moe(column) }}, 2){{ " +" if not loop.last }}
        {%- endfor %}
    )
{% endmacro %}

-- Row filter shared by the staging model and the source-contract tests:
-- complete-geography (component 0000) rows at the retained summary levels
-- only. Defined once so staging and its tests can never drift apart.
{% macro census_acs_retained_geo_rows() %}
    substring(geo_id, 4, 6) = '0000US'
    and left(geo_id, 3) in ('150', '040', '050', '160', '010', '950', '960', '970')
{% endmacro %}
