-- An office whose latest attempt still identifies a current district must not
-- reappear on the pending list: re-matching it can only lose information, since
-- an abstain would erase a link that serves today. "Identifies" tolerates L2's
-- cosmetic respellings: the stored tuple matches the universe exactly, or its
-- normalized spelling resolves to exactly one current universe row. Ambiguous
-- respellings (same-name twins) are not still-valid; those stay pending so a
-- re-match decides.
with
    latest_attempt as (
        select
            br_database_id,
            l2_state,
            l2_district_type,
            l2_district_name,
            {{ normalize_l2_district_name("l2_district_name") }}
            as normalized_district_name
        from {{ source("model_predictions", "llm_l2_br_match_results") }}
        -- Must match int__l2_br_match_pending_offices' ordering exactly, or this
        -- test can disagree with the model about which attempt is latest.
        qualify
            row_number() over (
                partition by br_database_id
                order by attempted_at desc, l2_district_name nulls first
            )
            = 1
    ),
    -- Mirrors the model's unambiguous-respelling arm: normalized keys carried by
    -- exactly one current universe row.
    universe_normalized as (
        select
            state_postal_code,
            district_type,
            {{ normalize_l2_district_name("district_name") }}
            as normalized_district_name
        from {{ ref("int__l2_district_universe") }}
        group by
            state_postal_code,
            district_type,
            {{ normalize_l2_district_name("district_name") }}
        having count(*) = 1
    ),
    still_valid_matches as (
        select latest_attempt.br_database_id
        from latest_attempt
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as office
            on latest_attempt.br_database_id = office.database_id
        left join
            {{ ref("int__l2_district_universe") }} as universe
            on latest_attempt.l2_state = universe.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe.district_type
            and latest_attempt.l2_district_name = universe.district_name
        left join
            universe_normalized
            on latest_attempt.l2_state = universe_normalized.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe_normalized.district_type
            and latest_attempt.normalized_district_name
            = universe_normalized.normalized_district_name
        where
            latest_attempt.l2_district_name is not null
            and (
                universe.district_name is not null
                or universe_normalized.normalized_district_name is not null
            )
    )
select pending.br_database_id
from {{ ref("int__l2_br_match_pending_offices") }} as pending
inner join
    still_valid_matches on pending.br_database_id = still_valid_matches.br_database_id
