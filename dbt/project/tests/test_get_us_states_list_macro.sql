-- Test the get_us_states_list macro functionality
-- This test validates that the macro returns the correct number of states
-- and includes/excludes DC and US as expected
with
    test_cases as (
        select *
        from
        values
            -- Test case 1: Default parameters (include_DC=true, include_US=false,
            -- include_territories=false)
            -- 50 states + DC = 51
            ('default', 51),

            -- Test case 2: Include US, include DC (no territories)
            -- 50 states + DC + US = 52
            ('include_us_dc', 52),

            -- Test case 3: Include US, exclude DC (no territories)
            -- 50 states + US = 51
            ('include_us_no_dc', 51),

            -- Test case 4: Exclude US, exclude DC (no territories)
            -- 50 states
            ('no_us_no_dc', 50),

            -- Test case 5: Include territories explicitly
            -- 50 states + DC + 5 territories = 56
            ('with_territories', 56) as test_cases(test_name, expected_count)
    ),

    macro_results as (
        select
            'default' as test_name, {{ get_us_states_list() | length }} as actual_count

        union all

        select
            'include_us_dc' as test_name,
            {{ get_us_states_list(include_DC=true, include_US=true) | length }}
            as actual_count

        union all

        select
            'include_us_no_dc' as test_name,
            {{ get_us_states_list(include_DC=false, include_US=true) | length }}
            as actual_count

        union all

        select
            'no_us_no_dc' as test_name,
            {{ get_us_states_list(include_DC=false, include_US=false) | length }}
            as actual_count

        union all

        select
            'with_territories' as test_name,
            {{
                get_us_states_list(
                    include_DC=true, include_US=false, include_territories=true
                ) | length
            }} as actual_count
    ),

    validation as (
        select
            tc.test_name,
            tc.expected_count,
            mr.actual_count,
            -- Check if count matches expected
            case
                when tc.expected_count = mr.actual_count then true else false
            end as count_check_passed
        from test_cases tc
        join macro_results mr on tc.test_name = mr.test_name
    )

select *
from validation
where not count_check_passed
