-- Pins slugify's output for each flag combination, so a change to the macro
-- cannot quietly alter slugs for the entities that did not ask for it.
--
-- The default cases are the contamination guard: place, race and candidacy
-- slugs are path-shaped routing keys, so slugify must keep '/' unless a caller
-- opts into single_segment. The varargs cases guard the column binding, which
-- silently drops a column if the flags are ever moved into the macro signature
-- (positional args fill declared parameters first).
--
-- Cases are grouped by flag combination rather than listed one per select,
-- because Databricks caps a query at 5 Python UDF references and each
-- transliterate=true call is one. Fixtures prefer 'ł' and 'ß', which have no
-- NFD decomposition, so an expected value cannot depend on how the literal
-- happens to be normalized in this file.
with
    default_inputs(case_name, raw, expected) as (
        values
            (
                'default keeps the path separator',
                'ca/los-angeles/mayor',
                'ca/los-angeles/mayor'
            ),
            ('default deletes a letter it cannot fold', 'Łukasz', 'ukasz'),
            ('default keeps a slash from the source text', 'N/A N/A', 'n/a-n/a')
    ),

    transliterate_literals(case_name, raw, expected) as (
        values
            ('transliterate folds a letter the strip would delete', 'Łukasz', 'lukasz'),
            ('transliterate expands a multi-character letter', 'Straße', 'strasse'),
            ('transliterate romanizes non-latin script', 'محمد', 'mhmd'),
            (
                'transliterate alone keeps the path separator',
                'ca/los-angeles/mayor',
                'ca/los-angeles/mayor'
            )
    ),

    -- Precomposed é (U+00E9) against decomposed é (e + U+0301), built from bytes
    -- rather than written out so an editor cannot normalize the two into one and
    -- make the comparison vacuous. Undecorated slugify treats them differently,
    -- deleting the first and keeping the base letter of the second.
    normalization_inputs as (
        select
            'transliterate folds precomposed e-acute' as case_name,
            decode(unhex('C3A9'), 'UTF-8') as raw,
            'e' as expected
        union all
        select
            'transliterate folds decomposed e-acute',
            concat('e', decode(unhex('CC81'), 'UTF-8')),
            'e'
    ),

    transliterate_inputs as (
        select case_name, raw, expected
        from transliterate_literals
        union all
        select case_name, raw, expected
        from normalization_inputs
    ),

    single_segment_inputs(case_name, raw, expected) as (
        values
            (
                'single_segment turns the path separator into a hyphen',
                'N/A N/A',
                'n-a-n-a'
            ),
            (
                'single_segment leaves a slash-free slug alone',
                'Plain Name',
                'plain-name'
            )
    ),

    both_inputs(case_name, raw, expected) as (
        values
            (
                'single_segment catches a separator introduced by transliteration',
                'Deanna MUA ½oz',
                'deanna-mua-1-2oz'
            )
    ),

    cases as (
        select case_name, {{ slugify("raw") }} as actual, expected
        from default_inputs

        union all
        select case_name, {{ slugify("raw", transliterate=true) }}, expected
        from transliterate_inputs

        union all
        select case_name, {{ slugify("raw", single_segment=true) }}, expected
        from single_segment_inputs

        union all
        select
            case_name,
            {{ slugify("raw", transliterate=true, single_segment=true) }},
            expected
        from both_inputs

        union all
        select
            'default joins varargs with a hyphen',
            {{ slugify("'first'", "'last'", "'a1b2c3d4'") }},
            'first-last-a1b2c3d4'

        union all
        select
            'default drops null and empty parts',
            {{ slugify("'first'", "cast(null as string)", "''", "'last'") }},
            'first-last'

        union all
        select
            'transliterate still joins varargs',
            {{ slugify("'Łukasz'", "'Straße'", "'a1b2c3d4'", transliterate=true) }},
            'lukasz-strasse-a1b2c3d4'
    )
select case_name, actual, expected
from cases
where actual is distinct from expected
