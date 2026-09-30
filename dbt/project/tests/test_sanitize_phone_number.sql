-- Test the sanitize_phone_number macro
-- Passes when all rows return expected results (query returns 0 rows)
with
    test_data as (
        select *
        from
        values
            -- Standard 10-digit shapes are kept as-is
            ('2025550123', '2025550123'),
            ('202-555-0123', '202-555-0123'),
            ('(202) 555-0123', '(202) 555-0123'),
            ('202.555.0123', '202.555.0123'),
            -- 11 digits with a leading country code are not a bare 10-digit
            -- number; kept value must be exactly 10 digits
            ('1-202-555-0123', null),
            ('12025550123', null),
            -- Extension digits push the digit count past 10
            ('202-555-0123 x9', null),
            -- Too short
            ('555-0123', null),
            ('202-555-012', null),
            -- No digits at all
            ('not-a-phone', null),
            ('', null),
            (null, null) as t(phone, expected)
    ),

    results as (
        select phone, expected, {{ sanitize_phone_number("phone") }} as actual
        from test_data
    )

select *
from results
where
    actual != expected
    or (actual is null and expected is not null)
    or (actual is not null and expected is null)
