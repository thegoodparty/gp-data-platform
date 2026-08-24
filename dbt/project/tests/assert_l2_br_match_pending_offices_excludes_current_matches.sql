-- An office whose latest attempt is still a live match must not reappear on
-- the pending list.
with
    latest_attempt as (
        select br_database_id, l2_state, l2_district_type, l2_district_name
        from {{ source("model_predictions", "llm_l2_br_match_results") }}
        -- Must match int__l2_br_match_pending_offices' ordering exactly, or this
        -- test can disagree with the model about which attempt is latest.
        qualify
            row_number() over (
                partition by br_database_id order by attempted_at desc, l2_district_name
            )
            = 1
    ),
    still_valid_matches as (
        select latest_attempt.br_database_id
        from latest_attempt
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as office
            on latest_attempt.br_database_id = office.database_id
        inner join
            {{ ref("int__l2_district_universe") }} as universe
            on latest_attempt.l2_state = universe.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe.district_type
            and latest_attempt.l2_district_name = universe.district_name
        where latest_attempt.l2_district_name is not null
    )
select pending.br_database_id
from {{ ref("int__l2_br_match_pending_offices") }} as pending
inner join
    still_valid_matches on pending.br_database_id = still_valid_matches.br_database_id
