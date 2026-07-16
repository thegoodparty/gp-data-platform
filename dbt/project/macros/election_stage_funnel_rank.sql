{#
    Funnel-depth rank of an election stage: how far through the election cycle a
    stage sits. Higher = deeper. general runoff (4) > general (3) > primary
    runoff (2) > primary (1); special variants share their non-special rank;
    anything else is 0.

    Single source of truth for this ordering so the per-candidacy stage rank and
    the per-race deepest-stage rank are compared on the same scale (see
    candidacy.sql: decided_stage_rank >= race_max_stage_rank). Pass the stage
    column to rank, e.g. {{ election_stage_funnel_rank('cs.election_stage') }}.

    NOTE: this is the funnel-depth ordering. It is intentionally NOT the
    "representative stage" ranking in int__civics_election_ballotready.sql, which
    ranks the general highest for a different purpose; do not unify the two.
#}
{% macro election_stage_funnel_rank(stage_column) %}
    case
        {{ stage_column }}
        when 'general runoff'
        then 4
        when 'general special runoff'
        then 4
        when 'general'
        then 3
        when 'general special'
        then 3
        when 'primary runoff'
        then 2
        when 'primary special runoff'
        then 2
        when 'primary'
        then 1
        when 'primary special'
        then 1
        else 0
    end
{% endmacro %}
