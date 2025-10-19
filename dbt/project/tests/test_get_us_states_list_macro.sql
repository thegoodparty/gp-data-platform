-- Test the get_us_states_list macro functionality
-- This analysis file demonstrates the macro functionality
with
    test_cases as (
        select *
        from
        values
            -- Test case 1: Default parameters (include_DC=true, include_US=false)
            ('default', '{{ get_us_states_list() }}', 51),

            -- Test case 2: Include US, include DC
            (
                'include_us_dc',
                '{{ get_us_states_list(include_DC=true, include_US=true) }}',
                52
            ),

            -- Test case 3: Include US, exclude DC
            (
                'include_us_no_dc',
                '{{ get_us_states_list(include_DC=false, include_US=true) }}',
                51
            ),

            -- Test case 4: Exclude US, exclude DC
            (
                'no_us_no_dc',
                '{{ get_us_states_list(include_DC=false, include_US=false) }}',
                50
            ) as test_cases(test_name, macro_result, expected_count)
    ),

    validation as (
        select
            test_name,
            macro_result,
            expected_count,
            -- Check if DC is included
            case
                when
                    test_name in ('default', 'include_us_dc')
                    and macro_result like '%DC%'
                then true
                when
                    test_name in ('include_us_no_dc', 'no_us_no_dc')
                    and macro_result not like '%DC%'
                then true
                else false
            end as dc_check_passed,
            -- Check if US is included
            case
                when
                    test_name in ('include_us_dc', 'include_us_no_dc')
                    and macro_result like '%US%'
                then true
                when
                    test_name in ('default', 'no_us_no_dc')
                    and macro_result not like '%US%'
                then true
                else false
            end as us_check_passed,
            -- Check if all 50 states are included
            case
                when macro_result like '%AK%' and macro_result like '%WY%'
                then true
                else false
            end as states_check_passed
        from test_cases
    )

select *
from validation
where not (dc_check_passed and us_check_passed and states_check_passed)
