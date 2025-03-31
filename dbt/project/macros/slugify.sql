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
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        lower(trim({{ column_name }})), '[^a-z0-9\\s-]', ''  -- remove special chars except hyphens
                    ),
                    '\\s+',
                    '-'  -- replace spaces with single hyphen
                ),
                '-{2,}',
                '-'  -- collapse multiple hyphens
            ),
            '-'  -- trim leading/trailing hyphens
        )
    {% else %}
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        lower(trim({{ column_name }})), '[^a-z0-9\\s]', ''  -- remove all special chars
                    ),
                    '\\s+',
                    '-'  -- replace spaces with single hyphen
                ),
                '-{2,}',
                '-'  -- collapse multiple hyphens
            ),
            '-'  -- trim leading/trailing hyphens
        )
    {% endif %}
{% endmacro %}
