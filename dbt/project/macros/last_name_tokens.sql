{% macro last_name_tokens(col) %}
    /*
        Splits a surname into its parts on any run of non-letter characters
        (e.g. "garcia-santos" -> "['garcia', 'santos']") so records agreeing on
        only one part can still block together. Two patterns need this: a
        hyphenated surname that one source writes as a single part, and
        TechSpeed packing a middle name into the surname field ("nguyen" vs
        "quoc thai nguyen"). Blocking on the whole surname reaches neither.

        \p{L} rather than [a-z]: an ASCII-only class treats every accented
        letter as a separator and shatters the name ("garcía-lópez" would
        tokenize to garc/pez).

        Single-character tokens are dropped, so "o'hearn" yields ['hearn'].

        Mirrored in the matcha repo's person blocking rules; keep in sync.
    */
    filter(
        split(regexp_replace(lower({{ col }}), '[^\\p{L}]+', ' '), ' '),
        t -> length(t) >= 2
    )
{% endmacro %}
