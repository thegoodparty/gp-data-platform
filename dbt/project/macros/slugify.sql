{% macro slugify(column_name) %}
    {#-
        Slugify one or more column/string expressions into a single lowercase,
        hyphen-joined slug. Extra positional args are joined with '-'; empty or
        null fields drop out, so there are no leading, trailing, or doubled
        hyphens. Matches npm (slugify)[https://www.npmjs.com/package/slugify]
        with lower=true, replacement='-', strict=false, trim=true.

        transliterate=true romanizes non-ASCII before the strip ('josé' ->
        'jose', 'محمد' -> 'mhmd'). single_segment=true turns '/' into '-' for a
        one-segment slug ('n/a' -> 'n-a'). Both opt-in: they rewrite existing
        slugs, and place/race slugs are path-shaped routing keys
        ('ca/los-angeles/mayor') that int__geo_id_attributes re-slugifies.

        Flags come from kwargs, not declared params, which would swallow the
        second positional column. Transliteration precedes lower(): unidecode
        marks some letters by case (H for ح against h for ه).

        Example:
            {{ slugify('title') }}
            {{ slugify('first', 'last', "left(id, 8)", transliterate=true, single_segment=true) }}
    -#}
    {%- set transliterate = kwargs.get("transliterate", false) -%}
    {%- set single_segment = kwargs.get("single_segment", false) -%}
    {%- set cols = [column_name] + (varargs | list) -%}
    {%- set joined = "trim(concat_ws('-', " ~ (cols | join(", ")) ~ "))" -%}
    {%- if transliterate -%}
        {%- set joined = ref("transliterate_to_ascii") ~ "(" ~ joined ~ ")" -%}
    {%- endif -%}
    {#- After transliteration, which turns ½ into '1/2'. -#}
    {%- if single_segment -%}
        {%- set joined = "replace(" ~ joined ~ ", '/', '-')" -%}
    {%- endif -%}
    trim(
        both '-/'
        from  -- trim leading/trailing hyphens and forward slashes
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower({{ joined }}), '[^a-z0-9\\s-/]', ''  -- remove special chars except hyphens and forward slashes
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
