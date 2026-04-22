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


{% macro extract_district_geographic(position_column) %}
    -- Extract the geographic district string from position_name.
    --
    -- Unlike extract_district_raw (which captures ALL suffix keywords including
    -- seat/post labels for entity-resolution fuzzy matching), this macro is
    -- scoped to keywords that represent geographic or electoral districts:
    -- District, Ward, Place, Branch, Subdistrict, Zone, Precinct, Area,
    -- Region, Circuit, Division, Subdivision.
    --
    -- Seat-label keywords (Position, Seat, Post, Section) are intentionally
    -- excluded because they identify a seat within a district, not the district
    -- itself. Example: "Aberdeen School Board - Position 1" has district=""
    -- here (the seat label is not a district), while extract_district_raw
    -- would return "1" — correct for ER matching, wrong for district semantics.
    --
    -- Uses ([^,]+) instead of (.+)$ to stop at trailing comma-delimited seat
    -- labels. E.g., "Circuit 10, Place 1" captures "10" not "10, Place 1".
    coalesce(
        trim(
            regexp_extract(
                {{ position_column }},
                '- (?:District|Ward|Place|Branch|Subdistrict|Zone|Precinct|Area|Region|Circuit|Division|Subdivision) ([^,]+)'
            )
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
