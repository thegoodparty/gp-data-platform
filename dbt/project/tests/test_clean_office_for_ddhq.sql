-- Test the clean_office_for_ddhq macro with various test cases
-- This analysis file demonstrates the macro functionality
with
    test_data as (
        select *
        from
        values
            -- Basic office names
            ('Mayor', 'Mayor'),
            ('mayor', 'Mayor'),
            ('MAYOR', 'Mayor'),
            ('Board Member', 'Board Member'),
            ('board member', 'Board member'),
            ('Council Member', 'Council Member'),
            ('council member', 'Council member'),
            ('School Board', 'School Board'),
            ('school board', 'School Board'),
            ('School District', 'School District'),
            ('school district', 'School District'),

            -- Whitespace handling
            ('  Mayor  ', 'Mayor'),
            ('  board   member  ', 'Board member'),
            ('  school   district  ', 'School District'),
            ('  council   member  ', 'Council member'),

            -- Multiple spaces
            ('Mayor    of    City', 'Mayor of City'),
            ('board    member    at    large', 'Board member at large'),
            ('school    district    trustee', 'School District trustee'),

            -- Null and empty string handling
            ('', null),
            (null, null),
            ('nan', null),
            ('NAN', null),
            ('None', null),
            ('NONE', null),
            ('none', null),

            -- Length validation (too long)
            ('Mayor' || repeat('X', 150), null),
            ('Board Member' || repeat('X', 150), null),

            -- Mixed case standardization
            ('MAYOR of the CITY', 'Mayor of the CITY'),
            ('board OF DIRECTORS', 'Board OF DIRECTORS'),
            ('COUNCIL member', 'Council member'),
            ('SCHOOL board member', 'School Board member'),
            ('DISTRICT attorney', 'District attorney'),

            -- Complex office names
            ('Mayor of New York City', 'Mayor of New York City'),
            ('Board of Education Member', 'Board of Education Member'),
            ('City Council Member at Large', 'City Council Member at Large'),
            ('School District Superintendent', 'School District Superintendent'),
            ('Board of Supervisors', 'Board of Supervisors'),

            -- Edge cases
            ('Mayor-Mayor', 'Mayor-Mayor'),
            ('Board-Board', 'Board-Board'),
            ('Council-Council', 'Council-Council'),
            ('School-School', 'School-School'),
            ('District-District', 'District-District') as test_data(
                office, expected_cleaned_office
            )
    ),
    completed_test_data as (
        select
            office as original_office,
            expected_cleaned_office,
            {{ clean_office_for_ddhq("office") }} as cleaned_office
        from test_data
    ),
    invalid_test_data as (
        select *
        from completed_test_data
        where cleaned_office != expected_cleaned_office
    )

select *
from invalid_test_data
