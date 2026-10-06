{% macro last_name_variants(col) %}
    /*
        The surname plus its last word (e.g. "leann french" -> "['leann french',
        'french']"), so a middle or maiden name prepended by one source still
        overlaps the bare surname from another. A leading particle stays with the
        last word ("van doren" stays whole). The last word is always the final
        element, so it can serve as a blocking key.

        Mirrored in the matcha repo's last-name comparison; keep in sync.
    */
    case
        when nullif(trim({{ col }}), '') is not null
        then
            array_distinct(
                array(
                    lower(trim({{ col }})),
                    regexp_extract(
                        lower(trim({{ col }})),
                        '(?:^| )((?:(?:de|del|della|da|di|du|dos|das|la|le|van|von|der|den|ter|ten|st\\.?|san|santa|mac|bin|al|el|o) )*[^ ]+)$',
                        1
                    )
                )
            )
    end
{% endmacro %}
