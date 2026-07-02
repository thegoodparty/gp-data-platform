-- Fails if any column flagged is_available = false in the
-- l2_column_classification seed leaks into the serve_agent_voters model.
-- This enforces the de-identification / non-partisan contract: retracting a
-- field is done by flipping is_available in the seed, and this test guarantees
-- the retraction actually took effect in the exposed projection.
-- depends_on: {{ ref("serve_agent_voters") }}
{% set model_columns = [] %}
{% if execute %}
    {% set model_columns = (
        adapter.get_columns_in_relation(ref("serve_agent_voters"))
        | map(attribute="name")
        | map("lower")
        | list
    ) %}
{% endif %}

select column_name
from {{ ref("l2_column_classification") }}
where
    lower(cast(is_available as string)) = 'false'
    and lower(column_name) in (
        {%- for c in model_columns %}
            '{{ c }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
        {%- if not model_columns %} ''{% endif %}
    )
