{% macro slugify(column_name) %}
    {#-
        Slugify one or more column/string expressions into a single lowercase,
        hyphen-joined slug. Extra positional args are joined with '-'; empty or
        null fields drop out, so there are no leading, trailing, or doubled
        hyphens. Matches npm (slugify)[https://www.npmjs.com/package/slugify]
        with lower=true, replacement='-', strict=false, trim=true.

        Pass transliterate=true to romanize non-ASCII text before the strip, so
        'josé' slugs to 'jose' and a non-Latin name keeps a readable stem.
        Opt-in because it rewrites existing slugs: a person slug carries an id
        suffix so the old URL still resolves and redirects, but a place or race
        slug is itself the routing key and needs a redirect plan first.

        Pass single_segment=true when the slug occupies one URL path segment,
        which turns '/' into '-' instead of keeping it. Slugs are path-shaped by
        default because place and race slugs nest ('ca/los-angeles/mayor'), and
        int__geo_id_attributes re-slugifies an already-joined parent path.

        Both flags are read from kwargs, not declared: a declared parameter
        would swallow the second positional column. Transliteration runs before
        lower() because unidecode marks some letters by case (H for ح against h
        for ه), which the a-z strip would otherwise drop.

        Example:
            {{ slugify('title') }}
            {{ slugify('first_name', 'last_name', "left(id, 8)", transliterate=true) }}
    -#}
    {%- set transliterate = kwargs.get("transliterate", false) -%}
    {%- set single_segment = kwargs.get("single_segment", false) -%}
    {%- set cols = [column_name] + (varargs | list) -%}
    {%- set joined = "trim(concat_ws('-', " ~ (cols | join(", ")) ~ "))" -%}
    {%- if transliterate -%}
        {%- set joined = ref("transliterate_to_ascii") ~ "(" ~ joined ~ ")" -%}
    {%- endif -%}
    {#- After transliteration, which turns fractions like ½ into '1/2'. -#}
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
