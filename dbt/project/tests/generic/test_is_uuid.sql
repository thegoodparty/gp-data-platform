{% test is_uuid(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where
        not {{ column_name }}
        rlike '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

{% endtest %}
