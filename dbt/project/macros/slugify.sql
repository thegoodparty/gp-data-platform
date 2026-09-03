{% macro slugify(column_name) %}
    {#-
        Slugify one or more column/string expressions into a single lowercase,
        hyphen-joined slug. Extra positional args are joined with '-'; empty or
        null fields drop out, so there are no leading, trailing, or doubled
        hyphens. Matches npm (slugify)[https://www.npmjs.com/package/slugify]
        with lower=true, replacement='-', strict=false, trim=true.

        Pass fold_accents=true to fold Latin diacritics onto their ASCII base
        before the strip, so 'josé' slugs to 'jose' rather than 'jos'. It is
        opt-in because turning it on rewrites existing slugs: a person slug
        carries an id suffix and so survives the change (the old URL still
        resolves and redirects to the new one), but a place or race slug is
        itself the routing key, so folding those needs a redirect plan first.
        It is read from kwargs rather than declared, because a declared
        parameter would swallow the second positional column instead.

        Example:
            {{ slugify('title') }}
            {{ slugify('first_name', 'last_name', "left(id, 8)", fold_accents=true) }}
    -#}
    {%- set fold_accents = kwargs.get("fold_accents", false) -%}
    {%- set cols = [column_name] + (varargs | list) -%}
    {%- set lowered = "lower(trim(concat_ws('-', " ~ (cols | join(", ")) ~ ")))" -%}
    trim(
        both '-/'
        from  -- trim leading/trailing hyphens and forward slashes
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            {% if fold_accents -%}{{ fold_latin_accents(lowered) }}
                            {%- else -%}{{ lowered }}
                            {%- endif %},
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
