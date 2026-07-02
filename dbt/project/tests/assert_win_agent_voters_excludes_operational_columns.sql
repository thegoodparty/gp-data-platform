-- Fails if any column whose family = 'operational' in the
-- l2_column_classification seed leaks into the win_agent_voters model.
-- win_agent_voters excludes both identity_pii and operational; this mirrors the
-- identity_pii guard for the ETL-metadata half (loaded_at, SEQUENCE, etc.), so a
-- seed edit that re-classifies an operational column would otherwise expose it
-- with no CI signal.
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
    lower(cast(family as string)) = 'operational'
    and lower(column_name) in (
        {%- for c in model_columns %}
            '{{ c }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
        {%- if not model_columns %} ''{% endif %}
    )
