{% macro first_name_tokens(col) %}
    /*
        Splits the first name into separate tokens (e.g. "james earl" ->
        "['james', 'earl']") so compound first names have a chance to match
        between sources if any of the first name tokens overlap.

        Single-character tokens are dropped to avoid initial-only overlaps.

        Mirrored in the matcha repo's first-name comparison; keep in sync.
    */
    filter(
        split(regexp_replace(lower({{ col }}), '[^a-z ]', ' '), ' '),
        t -> length(t) >= 2
    )
{% endmacro %}
