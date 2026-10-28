-- DDHQ election-stage natural key: the race id plus its stage. It is the ER
-- source_id for DDHQ election stages, so a DDHQ race keeps one key whatever
-- gp_election_stage_id it resolves to (the published id adopts the
-- election-stage mint, which keying on the id itself would feed back into).
-- Internal, not a published id. A race id can carry more than one stage,
-- hence the stage.
{% macro generate_ddhq_election_stage_key(race_id_col, stage_col) %}
    cast({{ race_id_col }} as string) || '_' || lower({{ stage_col }})
{% endmacro %}
