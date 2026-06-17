{% macro get_l2_major_district_columns(use_backticks=true, cast_to_string=false) %}
    {#-
    The v1 major office-bearing district-type subset (DATA-1992): the L2
    district columns the district/census substrate UNPIVOTs. General-purpose
    government, municipal, county/city subdivisions, schools, and fire -- the
    TDD §4.5 buckets that hold essentially the whole serve cohort, plus the
    universal-governance types the comparison/Win consumers need. Excludes
    synthetic State/Country, non-offices (Designated_Market_Area_DMA,
    Hamlet_Community_Area, Other), at-large/sub-seat columns, and the sparse
    special-district long tail (a sized fast-follow).

    Deliberately a DEDICATED macro, not a `major_only` flag on
    get_l2_district_columns: that shared macro feeds the election-api mart and
    int__l2_district_aggregations, so editing it marks those (heavy) models as
    state:modified and pulls their full-refresh rebuild into every PR/merge
    build. Isolating the v1 scope policy here keeps that blast radius out. The
    subset names carry no aliases, so the formatting is simple.

    Args:
        use_backticks (bool): true wraps names in backticks (SELECT); false
                              emits bare names (UNPIVOT IN clause).
        cast_to_string (bool): when use_backticks, cast each column to STRING
                               (UNPIVOT needs uniform types). Ignored when
                               use_backticks is false.

    Usage:
        SELECT {{ get_l2_major_district_columns(cast_to_string=true) }} FROM ...
        ... UNPIVOT (v FOR c IN ({{ get_l2_major_district_columns(use_backticks=false) }}))
    -#}
    {%- set cols = [
        "County",
        "US_Congressional_District",
        "State_Senate_District",
        "State_House_District",
        "State_Legislative_District",
        "City",
        "Township",
        "Town_District",
        "Village",
        "Borough",
        "City_Council_Commissioner_District",
        "City_Ward",
        "County_Commissioner_District",
        "County_Supervisorial_District",
        "School_District",
        "Unified_School_District",
        "School_Board_District",
        "Board_of_Education_District",
        "City_School_District",
        "Elementary_School_District",
        "High_School_District",
        "Fire_District",
    ] -%}
    {%- set out = [] -%}
    {%- for c in cols -%}
        {%- if not use_backticks -%} {%- set _ = out.append(c) -%}
        {%- elif cast_to_string -%}
            {%- set _ = out.append("cast(`" ~ c ~ "` as string) as `" ~ c ~ "`") -%}
        {%- else -%} {%- set _ = out.append("`" ~ c ~ "`") -%}
        {%- endif -%}
    {%- endfor -%}
    {{ out | join(",\n            ") }}
{% endmacro %}
