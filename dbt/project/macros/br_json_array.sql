{% macro br_json_array(json_path, element_schema) %}
    {#- Parse a JSON array to a typed array, empty rather than null when the key is
        absent. The Python models this replaces normalised a missing list to [], and
        prod carries no null in any of these columns, so returning null here would
        both diverge from the old behaviour and fail the column's not_null test. -#}
    coalesce(
        from_json(
            get_json_object(payload, '{{ json_path }}'), 'array<{{ element_schema }}>'
        ),
        from_json('[]', 'array<{{ element_schema }}>')
    )
{% endmacro %}
