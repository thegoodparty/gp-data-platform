-- Highly optimized test for unique combination of columns
-- Uses sampling and early termination for better performance on large datasets
{% test unique_combination_sampled(model, combination_of_columns, sample_size=10000) %}

    {%- set columns_csv = combination_of_columns | join(", ") -%}

    with
        sampled_data as (
            select {{ columns_csv }}
            from {{ model }} tablesample ({{ sample_size }} rows)
        ),

        duplicate_check as (
            select {{ columns_csv }}, count(*) as duplicate_count
            from sampled_data
            group by {{ columns_csv }}
            having count(*) > 1
            limit 1
        )

    select {{ columns_csv }}, duplicate_count
    from duplicate_check

{% endtest %}
