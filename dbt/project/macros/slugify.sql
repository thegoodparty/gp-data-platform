{% macro slugify(column_name) %}
    {#-
        Slugify one or more column/string expressions into a single lowercase,
        hyphen-joined slug. Extra positional args are joined with '-'; empty or
        null fields drop out, so there are no leading, trailing, or doubled
        hyphens. Matches npm (slugify)[https://www.npmjs.com/package/slugify]
        with lower=true, replacement='-', strict=false, trim=true.

        Example:
            {{ slugify('title') }}
            {{ slugify('first_name', 'last_name', "left(id, 8)") }}
    -#}
    {%- set cols = [column_name] + (varargs | list) -%}
    trim(
        both '-/'
        from  -- trim leading/trailing hyphens and forward slashes
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(trim(concat_ws('-', {{ cols | join(", ") }}))),
                            '[^a-z0-9\\s-/]',
                            ''  -- remove special chars except hyphens and forward slashes
                        ),
                        '\\s+',
                        '-'  -- replace spaces with single hyphen
                    ),
                    '-{2,}',
                    '-'  -- collapse multiple hyphens
                ),
                '/{2,}',
                '-'  -- collapse multiple forward slashes
            )
    )
{% endmacro %}
