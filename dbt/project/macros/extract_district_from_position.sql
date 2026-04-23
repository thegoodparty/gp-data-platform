{% macro extract_district_raw(position_column) %}
    -- Extract the full district string after a keyword prefix in position_name.
    -- Captures "- District 3", "- Ward 2", "- Precinct 4, Seat A", etc.
    coalesce(
        regexp_extract(
            {{ position_column }},
            '- (?:District|Ward|Place|Branch|Subdistrict|Zone|Precinct|Position|Area|Region|Circuit|Division|Post|Section|Subdivision|Seat) (.+)$'
        ),
        ''
    )
{% endmacro %}


{% macro extract_district_civics_hash(position_column) %}
    -- Narrower 6-keyword subset of extract_district_raw, load-bearing for
    -- civics mart gp_candidacy_id hashing. The salted UUID takes district
    -- as a hash input, so every civics intermediate that computes a candidacy
    -- id must use the exact same extraction or hashes diverge across
    -- sources and ER clustering breaks silently. Used by:
    -- int__civics_candidacy_ballotready
    -- int__civics_elected_official_ballotready
    -- int__civics_candidacy_gp_api
    -- int__civics_candidacy_stage_gp_api
    -- Do not widen the keyword list without synchronizing a rebuild of all
    -- civics_candidacy* and civics_elected_official* models.
    coalesce(
        regexp_extract(
            {{ position_column }},
            '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
        ),
        ''
    )
{% endmacro %}


{% macro extract_district_identifier(position_column) %}
    -- Extract the numeric district identifier from position_name.
    -- First tries keyword-prefixed patterns (District 3, Ward 2), then falls
    -- back to congressional district patterns (33rd Congressional District).
    coalesce(
        try_cast(
            regexp_extract(
                {{ position_column }},
                '- (?:District|Ward|Place|Branch|Subdistrict|Zone|Precinct|Position|Area|Region|Circuit|Division|Post|Section|Subdivision|Seat) ([0-9]+)'
            ) as int
        ),
        try_cast(
            regexp_extract(
                {{ position_column }}, '([0-9]+)(?:st|nd|rd|th) Congressional'
            ) as int
        )
    )
{% endmacro %}
