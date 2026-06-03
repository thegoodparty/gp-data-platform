{% macro first_name_normalized(col) %}
    /*
    Alpha-only normalized first name for Splink exact-match comparison.  Strips
    whitespace, periods, hyphens, and any other non-letter character so
    period/whitespace variants collapse to the same value (e.g. "r.j." and "rj"
    both -> "rj"; "mary-jane" and "mary jane" both -> "maryjane").

    Mirrored in the matcha repo's first-name comparison; keep in sync.
    */
    regexp_replace(lower({{ col }}), '[^a-z]', '')
{% endmacro %}
