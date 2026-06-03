{% macro first_name_normalized(col) %}
    -- Alpha-only normalized first name for Splink exact-match comparison.
    -- Strips whitespace, periods, hyphens, and any other non-letter character
    -- so period/whitespace variants collapse to the same value (e.g. "r.j."
    -- and "rj" both -> "rj"; "mary-jane" and "mary jane" both -> "maryjane").
    --
    -- Mirrors the first-name normalization in the matcha repo's
    -- scripts/configs comparisons -- keep both in sync when editing.
    regexp_replace(lower({{ col }}), '[^a-z]', '')
{% endmacro %}
