-- Drift guard: every event_type the amplitude_event_is_recurrent allowlist admits
-- must resolve to a win_* family via amplitude_event_family. This is a static check
-- (does not depend on the events appearing in the stream), and so complements the
-- int__amplitude.yaml `is_win or not is_recurrent` invariant, which only fires
-- against observed rows in the materialized table.
--
-- Fails (returns rows) if a recurrent event classifies to a non-win family, which
-- would let int__amplitude_win_activity silently roll up non-Win events as Win
-- engagement. The allowlist is derived from the anchor declarations of the metrics
-- the Win rollups serve, so the event list is read from those declarations here too
-- rather than retyped: a hand-kept copy is what this guard exists to catch, and it
-- cannot catch a drift in itself.
--
-- The tags below deliberately do not left-trim their whitespace. Trimming pulls
-- the preamble up onto the last comment line above and takes the `with` with it,
-- so the query compiles commented out and fails on the first paren.
{% set anchored_metrics = ["win_active_candidates_30d", "win_activated_users"] %}
{% set names = [] %}
{% if execute %}
    {%- for metric_name in anchored_metrics -%}
        {%- for leg in metric_anchored_events(metric_name) -%}
            {#- Pathed legs are excluded from the allowlist, so they are out of scope
                here; their family is checked by the invariant test on observed rows. -#}
            {%- if not leg["path"] and leg["event"] not in names -%}
                {%- do names.append(leg["event"]) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
{% endif %}

with
    recurrent_events(event_type) as (
        values
            {%- if names | length == 0 %}
                -- Parse time only: the graph is empty, so emit a row the check passes
                -- on.
                ('Voter Outreach - Campaign Completed')
            {%- else %}
                {%- for event in names | sort %}
                    ('{{ event }}'){{ "," if not loop.last }}
                {%- endfor %}
            {%- endif %}
    )

select event_type, {{ amplitude_event_family("event_type") }} as family
from recurrent_events
where {{ amplitude_event_family("event_type") }} not like 'win_%'
