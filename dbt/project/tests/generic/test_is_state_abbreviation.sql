{% test is_state_abbreviation(model, column_name) %}

    {%- set valid_states = get_us_states_list() -%}

    select {{ column_name }}
    from {{ model }}
    where
        {{ column_name }} not in (
            {%- for state in valid_states -%}
                '{{ state }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )

{% endtest %}
