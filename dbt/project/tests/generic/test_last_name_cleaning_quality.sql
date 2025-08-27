-- Test for ensuring last names don't contain common suffixes
-- This test identifies last names that may need suffix removal
{% test last_name_cleaning_quality(model, column_name) %}
    {{ config(severity="warn") }}

    with
        validation_errors as (
            select {{ column_name }}, count(*) as error_count
            from {{ model }}
            where
                {{ column_name }} is not null
                and (
                    {{ column_name }} like '% Jr%'
                    or {{ column_name }} like '% Sr%'
                    or {{ column_name }} like '% II%'
                    or {{ column_name }} like '% III%'
                    or {{ column_name }} like '% IV%'
                    or {{ column_name }} like '% V%'
                )
            group by {{ column_name }}
            having count(*) > 0
        )

    select *
    from validation_errors

{% endtest %}
