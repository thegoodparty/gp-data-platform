{% test is_state_abbreviation(model, column_name) %}

    {# Keep civics state validation territory-inclusive independent of macro defaults. #}
    {%- set valid_states = get_us_states_list(include_territories=true) -%}

    select {{ column_name }}
    from {{ model }}
    where
        {{ column_name }} not in (
            {%- for state in valid_states -%}
                '{{ state }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )

{% endtest %}
