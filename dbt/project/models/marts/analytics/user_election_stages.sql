-- The stages of each user's current election (primary, runoffs, general) and
-- whether any candidate in each stage has a result yet: one row per user per
-- election stage. Internal only.
--
-- A stage has a result when any candidacy in it has one, ours or not, so the
-- same race counts the same for every user in it. Runoffs are listed as
-- scheduled, including "if needed" runoffs that never took place; filter on
-- is_runoff and has_result to leave those out.
with
    journey as (
        select
            user_id,
            candidacy_campaign_id,
            to_date(candidacy_election_date) as election_date
        from {{ ref("user_journey") }}
        where has_candidacy
    ),

    -- The same anchor match as the user's candidacy profile: the run's date
    -- must be one of the candidacy's stage dates, and a general-date match wins.
    anchor_candidacy as (
        select j.user_id, c.gp_candidacy_id, c.gp_election_id
        from journey as j
        inner join
            {{ ref("candidacy") }} as c
            on c.product_campaign_id = j.candidacy_campaign_id
            and j.election_date in (
                c.general_election_date,
                c.primary_election_date,
                c.primary_runoff_election_date,
                c.general_runoff_election_date
            )
        qualify
            row_number() over (
                partition by j.user_id
                order by
                    case
                        when c.general_election_date = j.election_date then 0 else 1
                    end,
                    c.gp_candidacy_id
            )
            = 1
    ),

    stage_results as (
        select gp_election_stage_id, bool_or(election_result is not null) as has_result
        from {{ ref("candidacy_stage") }}
        group by gp_election_stage_id
    )

select
    a.user_id,
    a.gp_candidacy_id,
    a.gp_election_id,
    e.gp_election_stage_id,
    e.stage_type,
    e.election_date as stage_date,
    coalesce(e.is_runoff, e.stage_type like '%runoff%') as is_runoff,
    coalesce(r.has_result, false) as has_result
from anchor_candidacy as a
inner join {{ ref("election_stage") }} as e on e.gp_election_id = a.gp_election_id
left join stage_results as r on r.gp_election_stage_id = e.gp_election_stage_id
