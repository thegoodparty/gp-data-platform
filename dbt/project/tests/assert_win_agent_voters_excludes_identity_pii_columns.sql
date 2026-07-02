-- Fails if any column whose family = 'identity_pii' in the
-- l2_column_classification seed leaks into the win_agent_voters model.
-- win_agent_voters uses a family-based filter rather than the is_available
-- flag, so this guards the PII-exclusion contract directly: a seed edit that
-- re-classifies a direct identifier out of identity_pii would otherwise expose
-- it with no CI signal.
-- depends_on: {{ ref("win_agent_voters") }}
{% set model_columns = [] %}
{% if execute %}
    {% set model_columns = (
        adapter.get_columns_in_relation(ref("win_agent_voters"))
        | map(attribute="name")
        | map("lower")
        | list
    ) %}
{% endif %}

select column_name
from {{ ref("l2_column_classification") }}
where
    lower(cast(family as string)) = 'identity_pii'
    and lower(column_name) in (
        {%- for c in model_columns %}
            '{{ c }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
        {%- if not model_columns %} ''{% endif %}
    )
