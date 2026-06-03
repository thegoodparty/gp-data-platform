{% macro first_name_tokens(col) %}
    -- Token array for Splink ArrayIntersectLevel first-name overlap. Splits the
    -- first name on non-letter characters and keeps tokens >= 2 chars, so a
    -- compound first name where one source carries an extra given name,
    -- honorific, or parenthetical still intersects on the shared name (e.g.
    -- "charles kirk" and "charles" overlap on "charles"; "dr. lori" and "lori"
    -- overlap on "lori"). Single-char tokens are dropped to avoid spurious
    -- initial-only overlaps; empty tokens from repeated separators are filtered
    -- out by the same length guard.
    --
    -- Mirrors the first-name tokenization in the matcha repo's scripts/configs
    -- comparisons -- keep both in sync when editing.
    filter(
        split(regexp_replace(lower({{ col }}), '[^a-z ]', ' '), ' '),
        t -> length(t) >= 2
    )
{% endmacro %}
