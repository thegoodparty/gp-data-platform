-- Unit test for sanitize_l2_phone_number: a value survives only when it holds a
-- full 10-digit number once non-digits are stripped. One row per failing case,
-- so an empty result is a pass.
with
    cases as (
        select *
        from
        values
            ('5555555555', '5555555555'),
            ('(555) 555-5555', '(555) 555-5555'),
            ('555.555.5555', '555.555.5555'),
            ('+1 555 555 5555', null),
            ('555-5555', null),
            ('55555555555', null),
            ('', null),
            ('   ', null),
            ('not a phone', null),
            (cast(null as string), null) as t(input, expected)
    )

select input, expected, {{ sanitize_l2_phone_number("input") }} as actual
from cases
where not ({{ sanitize_l2_phone_number("input") }} <=> expected)
