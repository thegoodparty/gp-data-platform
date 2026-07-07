-- state_postal_code must be non-null and a valid US state (incl DC) in each
-- voter-file mart. Replaces the per-column not_null + in-set tests moved off the
-- shared column anchor.
{% set voter_models = ["serve_agent_voters", "win_agent_voters"] %}
{% set states = get_us_states_list(include_DC=true, include_US=false) %}
with
    unioned as (
        {% for m in voter_models %}
            select '{{ m }}' as model_name, state_postal_code
            from {{ ref(m) }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )
select model_name, state_postal_code
from unioned
where
    state_postal_code is null
    or state_postal_code not in ('{{ states | join("', '") }}')
group by model_name, state_postal_code
