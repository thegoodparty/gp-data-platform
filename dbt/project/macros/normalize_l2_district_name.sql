-- Normalized join key for L2 district names. L2 names drift between
-- snapshots: the " (EST.)" suffix appears and disappears, and doubled
-- internal spaces show up in some vintages. Joining cohort districts to
-- L2-derived tables on the raw name silently zeroes those districts, so
-- every cross-snapshot district-name join must normalize BOTH sides with
-- this macro (collapse whitespace, strip a trailing " (EST.)", upper-case).
{% macro normalize_l2_district_name(column_expr) %}
    upper(
        regexp_replace(
            trim(regexp_replace({{ column_expr }}, '\\s+', ' ')),
            '\\s*\\(EST\\.\\)$',
            ''
        )
    )
{% endmacro %}
