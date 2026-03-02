-- Test the parse_human_name UDF with various name inputs
-- Returns rows where parsed output does not match expected values
with
    test_data as (
        select *
        from
        values
            (
                'Dr. Jannet "Jan the Man" Cynthia St. Jons Sr',
                'Dr.',
                'Jannet',
                'Cynthia',
                'St. Jons',
                'Sr',
                'Jan the Man'
            ),
            ('Jane Marie Smith', null, 'Jane', 'Marie', 'Smith', null, null),
            ('John O''Brien III', null, 'John', null, 'O''Brien', 'III', null),
            ('Ms. Maria de la Cruz', 'Ms.', 'Maria', null, 'de la Cruz', null, null),
            (null, null, null, null, null, null, null) as t(
                full_name,
                expected_title,
                expected_first,
                expected_middle,
                expected_last,
                expected_suffix,
                expected_nickname
            )
    ),
    completed_test_data as (
        select
            full_name,
            {{ ref("parse_human_name") }} (full_name) as parsed,
            expected_title,
            expected_first,
            expected_middle,
            expected_last,
            expected_suffix,
            expected_nickname
        from test_data
    ),
    invalid_test_data as (
        select *
        from completed_test_data
        where
            (parsed is null and full_name is not null)
            or (
                parsed is not null
                and (
                    coalesce(parsed.title, '') != coalesce(expected_title, '')
                    or coalesce(parsed.first, '') != coalesce(expected_first, '')
                    or coalesce(parsed.middle, '') != coalesce(expected_middle, '')
                    or coalesce(parsed.last, '') != coalesce(expected_last, '')
                    or coalesce(parsed.suffix, '') != coalesce(expected_suffix, '')
                    or coalesce(parsed.nickname, '') != coalesce(expected_nickname, '')
                )
            )
            or (parsed is not null and full_name is null)
    )

select *
from invalid_test_data
