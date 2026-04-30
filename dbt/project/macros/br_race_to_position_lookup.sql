{#
    Shared CTE body that maps br_race_id → BR position id + BR-side
    gp_election_id, sourced from int__civics_election_stage_ballotready.

    Used by the TS intermediates (candidacy / election / election_stage) to
    enrich TS-only rows with the BR-side gp_election_id when the row's
    br_race_id matches a BR election. This keeps candidacy/election aligned
    even when ER didn't cluster the candidacy.

    Returns one row per br_race_id (deterministic via the qualify on
    election_date desc).
#}
{% macro br_race_to_position_lookup() %}
    select
        br_race_id,
        cast(br_position_id as bigint) as br_position_database_id,
        gp_election_id as br_gp_election_id
    from {{ ref("int__civics_election_stage_ballotready") }}
    where br_position_id is not null
    qualify row_number() over (partition by br_race_id order by election_date desc) = 1
{% endmacro %}
