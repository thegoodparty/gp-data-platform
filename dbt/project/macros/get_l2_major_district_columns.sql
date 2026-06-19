{% macro get_l2_major_district_columns(use_backticks=true, cast_to_string=false) %}
    {#-
    The curated district-type subset the District/Census substrate UNPIVOTs
    (DATA-1992; widened in DATA-2013). Selection rule: every office-bearing L2
    district type the SERVE COHORT occupies that has a clean L2 binding. That is
    the general-purpose / municipal / county-subdivision / school / fire core
    that holds the bulk of the cohort (the original v1 subset), PLUS the
    previously-deferred special-district and sub-jurisdiction types that bind
    real cohort officials: community colleges, county boards of education,
    judicial districts, park / library / water / sewer / fire-protection / port /
    conservation / soil-and-water / aquatic districts, town & township wards,
    community-council sub-districts, county legislative districts, and the
    educational-service / vocational / learning-community types. Excludes
    synthetic State/Country, non-offices (Designated_Market_Area_DMA,
    Hamlet_Community_Area, Other), and any type with no cohort official or no L2
    binding (e.g. Precinct has no L2 column). The name is kept as "major" for
    continuity; it now means "the curated substrate subset," not a size cut.

    Deliberately a DEDICATED macro, not a `major_only` flag on
    get_l2_district_columns: that shared macro feeds the election-api mart and
    int__l2_district_aggregations, so editing it marks those (heavy) models as
    state:modified and pulls their full-refresh rebuild into every PR/merge
    build. Isolating the substrate's type policy here keeps that blast radius
    out. The subset names carry no aliases, so the formatting is simple. The two
    accepted_values tests on district_type (int__l2_block_district_map and
    district_census_stats) mirror this list; their subset semantics make a
    forgotten update fail CI loudly, so this macro stays the source of truth.

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
        "Community_College",
        "Community_College_SubDistrict",
        "County_Board_of_Education_District",
        "School_Subdistrict",
        "School_District_Vocational",
        "Educational_Service_District",
        "Learning_Community_Coordinating_Council_District",
        "Town_Ward",
        "Township_Ward",
        "Community_Council_SubDistrict",
        "County_Legislative_District",
        "Judicial_Justice_of_the_Peace",
        "Judicial_Superior_Court_District",
        "Judicial_District_Court_District",
        "Park_District",
        "Library_District",
        "Water_District",
        "Water_SubDistrict",
        "Sewer_District",
        "Fire_Protection_District",
        "Port_District",
        "Conservation_District",
        "Conservation_SubDistrict",
        "Soil_and_Water_District",
        "Aquatic_District",
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
