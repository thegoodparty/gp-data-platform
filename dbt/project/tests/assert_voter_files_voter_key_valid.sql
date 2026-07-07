-- voter_key must be non-null and unique in each voter-file mart. Replaces the
-- per-column not_null + unique tests that could not live on the shared column
-- anchor (voter_key does not exist on the upstream w_haystaq model).
{% set voter_models = ["serve_agent_voters", "win_agent_voters"] %}
with
    unioned as (
        {% for m in voter_models %}
            select '{{ m }}' as model_name, voter_key
            from {{ ref(m) }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )
select model_name, voter_key, count(*) as n_rows
from unioned
group by model_name, voter_key
having voter_key is null or count(*) > 1
