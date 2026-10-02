-- BallotReady primary result for product campaigns: did this signup run in the
-- primary for the seat they picked, and how did it go?
--
-- Exists because a campaign's ER record carries only its own election date,
-- almost always the general, and the candidacy matcher keeps stages apart. A
-- signup who lost the primary therefore never clusters with the BallotReady
-- primary record, never clears the candidacy mart's corroboration gate, and
-- reads as an unmatched general candidate. On the 2026-11-03 cohort that is
-- 705 campaigns (DATA-2603).
--
-- Deterministic, not probabilistic: the seat is the campaign's own
-- ballotready_position_id (BR primary and general candidacies share it), and
-- the person is an exact email, or an exact last name plus first name or
-- BallotReady nickname.
with
    campaign_base as (
        select
            c.campaign_id,
            c.election_date,
            lower(trim(c.user_email)) as email,
            lower(trim(c.user_first_name)) as first_name,
            lower(trim(c.user_last_name)) as last_name,
            cast(c.ballotready_position_id as bigint) as br_position_id
        from {{ ref("campaigns") }} as c
        where
            c.is_latest_version
            and not coalesce(c.is_demo, false)
            and c.ballotready_position_id is not null
            and c.election_date is not null
    ),

    -- First-round primaries only. A primary runoff is its own stage in civics
    -- and would need its own column to land in.
    br_primary as (
        select
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_position_id as bigint) as br_position_id,
            election_day,
            election_result as raw_election_result,
            lower(trim(email)) as email,
            lower(trim(first_name)) as first_name,
            lower(trim(nickname)) as nickname,
            lower(trim(last_name)) as last_name
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where is_primary and not coalesce(is_runoff, false)
    ),

    matched as (
        select
            cb.campaign_id,
            cb.election_date,
            bp.br_candidacy_id,
            bp.election_day as br_primary_election_date,
            bp.raw_election_result,
            case when bp.email = cb.email then 'email' else 'name' end as match_key
        from campaign_base as cb
        inner join
            br_primary as bp
            on bp.br_position_id = cb.br_position_id
            and year(bp.election_day) = year(cb.election_date)
            and bp.election_day < cb.election_date
            and (
                bp.email = cb.email
                or (
                    bp.last_name = cb.last_name
                    and (bp.first_name = cb.first_name or bp.nickname = cb.first_name)
                )
            )
        -- Email beats name; then the latest primary, then a stable tiebreak.
        qualify
            row_number() over (
                partition by cb.campaign_id
                order by
                    case when bp.email = cb.email then 0 else 1 end,
                    bp.election_day desc,
                    bp.br_candidacy_id
            )
            = 1
    )

select
    campaign_id,
    election_date,
    br_candidacy_id,
    br_primary_election_date,
    -- Same mapping as int__civics_candidacy_stage_ballotready, so this can be
    -- coalesced with candidacy-sourced primary results.
    case
        when raw_election_result in ('WON', 'GENERAL_WIN', 'PRIMARY_WIN')
        then 'Won'
        when raw_election_result in ('LOST', 'LOSS')
        then 'Lost'
        when raw_election_result = 'RUNOFF'
        then 'Runoff'
    end as primary_election_result,
    match_key
from matched
