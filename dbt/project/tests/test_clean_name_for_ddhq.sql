-- Test the clean_name_field macro with various test cases
-- This analysis file demonstrates the macro functionality
with
    test_data as (
        select *
        from
        values
            ('JOHN DOE', 'John Doe'),
            ('john doe', 'John Doe'),
            ('John Doe', 'John Doe'),
            ('  john   doe  ', 'John Doe'),
            ('john123', null),
            ('a', null),
            ('Jr', null),
            ('', null),
            (null, null),
            ('JOHN', 'John'),
            ('john', 'John'),
            ('John', 'John'),
            ('!!!JOHN!!!', null),
            ('JOHN' || repeat('X', 60), null),
            ('JOHN DOE JR', 'John Doe Jr') as test_data(name, expected_cleaned_name)
    ),
    completed_test_data as (
        select
            name as original_name,
            expected_cleaned_name,
            {{ clean_name_for_ddhq("name") }} as cleaned_name
        from test_data
    ),
    invalid_test_data as (
        select * from completed_test_data where cleaned_name != expected_cleaned_name
    )

select *
from invalid_test_data
