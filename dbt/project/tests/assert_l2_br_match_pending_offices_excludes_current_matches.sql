-- Two failures, mirroring the model's aliveness arms (edits must keep the two in
-- step): (1) an office whose latest attempt still identifies a current district --
-- exactly, or as a respelling normalizing to exactly one universe row -- is on the
-- pending list, so a re-match could erase a working link; (2) an office whose
-- respelling is ambiguous (a same-name twin) is MISSING from the pending list,
-- though only a re-match may decide between twins. Branch 2 is dormant until L2
-- carries a twin colliding with a stored label.
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
    -- The same shared macro as the model, so the two cannot drift: spellings = 1
    -- is the unambiguous-respelling arm, > 1 its ambiguous complement.
    universe_keys as (
        {{ l2_normalized_district_keys(ref("int__l2_district_universe")) }}
    ),
    states_with_real_districts as (
        select distinct state_postal_code
        from {{ ref("int__l2_district_universe") }}
        where district_type <> 'State'
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
            universe_keys
            on latest_attempt.l2_state = universe_keys.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe_keys.district_type
            and latest_attempt.normalized_district_name
            = universe_keys.normalized_district_name
            and universe_keys.spellings = 1
        where
            latest_attempt.l2_district_name is not null
            and (
                universe.district_name is not null
                or universe_keys.normalized_district_name is not null
            )
    ),
    still_valid_but_pending as (
        select pending.br_database_id
        from {{ ref("int__l2_br_match_pending_offices") }} as pending
        inner join
            still_valid_matches
            on pending.br_database_id = still_valid_matches.br_database_id
    ),
    ambiguous_respelling_not_pending as (
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
        inner join
            universe_keys
            on latest_attempt.l2_state = universe_keys.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe_keys.district_type
            and latest_attempt.normalized_district_name
            = universe_keys.normalized_district_name
            and universe_keys.spellings > 1
        left join
            {{ ref("int__l2_br_match_pending_offices") }} as pending
            on latest_attempt.br_database_id = pending.br_database_id
        where
            latest_attempt.l2_district_name is not null
            and universe.district_name is null
            and office.state
            in (select state_postal_code from states_with_real_districts)
            and pending.br_database_id is null
    )
select br_database_id
from still_valid_but_pending
union all
select br_database_id
from ambiguous_respelling_not_pending
