-- Test for ensuring district names are properly cleaned
-- This test identifies district names that may need standardization
{% test district_cleaning_quality(model, column_name) %}
    {{ config(severity="warn") }}

    with
        validation_errors as (
            select {{ column_name }}, count(*) as error_count
            from {{ model }}
            where
                {{ column_name }} is not null
                and (
                    {{ column_name }} like '%District %'
                    or {{ column_name }} like '%Dist. %'
                    or {{ column_name }} like '%Subdistrict %'
                    or {{ column_name }} like '%Ward %'
                )
            group by {{ column_name }}
            having count(*) > 0
        )

    select *
    from validation_errors

{% endtest %}
