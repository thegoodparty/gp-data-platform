{% macro slugify(column_name, keep_hyphens=true) %}
    {#-
        Matches npm (slugify package)[https://www.npmjs.com/package/slugify] behavior with lower=true and defaults:
        - replacement: '-'
        - lower: true
        - strict: false
        - trim: true

        Args:
            column_name: The column or string expression to slugify
            keep_hyphens: Boolean to keep existing hyphens (default: true)

        Example:
            {{ slugify('title') }}
            {{ slugify('title', keep_hyphens=false) }}
    -#}
    {% if keep_hyphens %}
        trim(
            both '-/'
            from  -- trim leading/trailing hyphens and forward slashes
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                lower(trim({{ column_name }})), '[^a-z0-9\\s-/]', ''  -- remove special chars except hyphens and forward slashes
                            ),
                            '\\s+',
                            '-'  -- replace spaces with single hyphen
                        ),
                        '-{2,}',
                        '-'  -- collapse multiple hyphens
                    ),
                    '/{2,}',
                    '-'  -- collapse multiple forward slashes (can use '/' instead)
                )
        )
    {% else %}
        trim(
            both '-/'
            from  -- trim leading/trailing hyphens and forward slashes
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                lower(trim({{ column_name }})), '[^a-z0-9\\s-/]', ''  -- remove special chars except hyphens and forward slashes
                            ),
                            '\\s+',
                            '-'  -- replace spaces with single hyphen
                        ),
                        '-{2,}',
                        '-'  -- collapse multiple hyphens
                    ),
                    '/{2,}',
                    '-'  -- collapse multiple forward slashes (can use '/' instead)
                )
        )
    {% endif %}
{% endmacro %}

{% macro generate_candidate_slug(first_name_col, last_name_col, office_name_col=none) %}
    {#-
        Generates a candidate slug from first name, last name, and optional office name.
        This macro creates slugified identifiers for candidates that can be used across models.

        Args:
            first_name_col: The first name column or expression
            last_name_col: The last name column or expression
            office_name_col: Optional office name column for additional context

        Example:
            {{ generate_candidate_slug('first_name', 'last_name') }}
            {{ generate_candidate_slug('first_name', 'last_name', 'official_office_name') }}
    -#}
    {% if office_name_col %}
        case
            when {{ office_name_col }} is not null and {{ office_name_col }} != ''
            then
                concat(
                    {{
                        slugify(
                            "concat(coalesce("
                            + first_name_col
                            + ", ''), '-', coalesce("
                            + last_name_col
                            + ", ''))"
                        )
                    }},
                    '/',
                    {{ slugify(office_name_col) }}
                )
            else
                {{
                    slugify(
                        "concat(coalesce("
                        + first_name_col
                        + ", ''), '-', coalesce("
                        + last_name_col
                        + ", ''))"
                    )
                }}
        end
    {% else %}
        {{
            slugify(
                "concat(coalesce("
                + first_name_col
                + ", ''), '-', coalesce("
                + last_name_col
                + ", ''))"
            )
        }}
    {% endif %}
{% endmacro %}
