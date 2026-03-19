-- Test the remove_name_suffixes macro
-- Passes when all rows return expected results (query returns 0 rows)
with
    test_data as (
        select *
        from
        values
            -- Basic suffixes
            ('John Smith Jr', 'John Smith'),
            ('John Smith Jr.', 'John Smith'),
            ('John Smith Sr', 'John Smith'),
            ('John Smith Sr.', 'John Smith'),
            ('John Smith II', 'John Smith'),
            ('John Smith III', 'John Smith'),
            ('John Smith IV', 'John Smith'),
            ('John Smith V', 'John Smith'),
            -- Case insensitive
            ('John Smith jr', 'John Smith'),
            ('John Smith JR', 'John Smith'),
            ('John Smith sr.', 'John Smith'),
            ('John Smith iii', 'John Smith'),
            -- No suffix (should be unchanged)
            ('John Smith', 'John Smith'),
            ('Jane Doe', 'Jane Doe'),
            ('Mike', 'Mike'),
            -- Suffix-like substrings that should NOT be stripped
            ('Junior Smith', 'Junior Smith'),
            ('Ivan Senior', 'Ivan Senior'),
            -- Period is literal, not wildcard (regression test for \\.? escaping)
            -- If regex uses \.? instead of \\.?, the . matches any char and
            -- these would incorrectly strip "Jrx" / "Srx"
            ('John Smith Jrx', 'John Smith Jrx'),
            ('John Smith Srx', 'John Smith Srx'),
            ('John Smith Jr!', 'John Smith Jr!'),
            -- Period specifically should be stripped
            ('Mary Jones Sr.', 'Mary Jones'),
            ('Bob White Jr.', 'Bob White'),
            -- Double suffix (macro applies regex twice)
            ('John Smith Jr Jr', 'John Smith'),
            -- Empty / null
            ('', ''),
            (null, null) as t(name, expected)
    ),

    results as (
        select
            name as original_name,
            expected,
            {{ remove_name_suffixes("name") }} as actual
        from test_data
    )

select *
from results
where
    actual != expected
    or (actual is null and expected is not null)
    or (actual is not null and expected is null)
