{% macro generate_salted_uuid(fields, salt="salt_string") %}
    {% set salted_fields = [] %}
    {% for field in fields %}
        {% do salted_fields.append(
            "CONCAT(COALESCE(CAST("
            ~ field
            ~ " AS "
            ~ dbt.type_string()
            ~ "), ''), '"
            ~ salt
            ~ "')"
        ) %}
    {% endfor %}
    {{ format_as_uuid(dbt_utils.generate_surrogate_key(salted_fields)) }}
{% endmacro %}

{% macro format_as_uuid(hash_string) %}
    concat(
        substr({{ hash_string }}, 1, 8),
        '-',
        substr({{ hash_string }}, 9, 4),
        '-',
        substr({{ hash_string }}, 13, 4),
        '-',
        substr({{ hash_string }}, 17, 4),
        '-',
        substr({{ hash_string }}, 21, 12)
    )
{% endmacro %}
