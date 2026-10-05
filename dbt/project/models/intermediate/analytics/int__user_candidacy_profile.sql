-- The run each user is on now, how it ended, and what they have run before.
-- One row per user.
--
-- Every candidacy_* column reads from the single anchor campaign version that
-- int__user_election_anchor picks. Taking the date from one campaign and the
-- office from another describes a race that does not exist.
--
-- A usable election date is what makes a candidacy. The anchor model also
-- picks campaigns with no date, but those are empty shells (about 21 of 18k
-- carry an office), so their candidacy_* columns stay null with the prospects.
--
-- Table, not view: read by the wide journey table and by its tests.
{{ config(materialized="table") }}

with
    anchor as (
        select
            user_id,
            anchor_campaign_id,
            anchor_campaign_version_id,
            election_date,
            election_date is not null as has_candidacy
        from {{ ref("int__user_election_anchor") }}
    ),

    keys as (select user_id, gp_person_id from {{ ref("int__user_resolved_keys") }}),

    anchor_campaign as (
        select
            campaign_version_id,
            campaign_office,
            normalized_position_name,
            campaign_state,
            campaign_party,
            partisan_type,
            election_level,
            ballotready_position_id,
            is_pledged,
            is_verified,
            is_pro
        from {{ ref("campaigns") }}
    ),

    -- Race-level civics facts at campaign-version grain. Incumbency here
    -- already falls back to the office-holder derivation when the candidacy
    -- corroboration gate kept the candidacy out.
    anchor_civics as (
        select
            campaign_version_id,
            icp_office_win,
            voter_count,
            is_incumbent,
            viability_score,
            win_number
        from {{ ref("users_win_candidacy") }}
    ),

    -- The civics candidacy for the anchor run. A product campaign can map to
    -- candidacies from different cycles, so the match requires the anchor date
    -- to be one of the candidacy's stage dates; a general-date match wins. A
    -- candidacy roughly a year off is another run and must not supply this
    -- run's outcome.
    anchor_candidacy as (
        select
            a.user_id,
            c.gp_candidacy_id,
            c.office_level,
            c.latest_stage_result,
            c.latest_stage_reached,
            -- A gp-api-only candidacy is our own campaign echoed back, not
            -- evidence the run reached a ballot.
            exists (c.source_systems, s -> s <> 'gp_api') as has_ballot_evidence,
            -- Narrower: a ballot data vendor. Vendor-sourced candidacies only
            -- exist from 2026; earlier runs reach the civics record through
            -- HubSpot, so this is false for them by construction. Taken over
            -- every candidacy matching the anchor, since the tiebreak below can
            -- pick one that lacks it.
            max(
                exists (
                    c.source_systems, s -> s in ('ballotready', 'ddhq', 'techspeed')
                )
            ) over (partition by a.user_id) as has_external_match
        from anchor as a
        inner join
            {{ ref("candidacy") }} as c
            on c.product_campaign_id = a.anchor_campaign_id
            and a.election_date in (
                c.general_election_date,
                c.primary_election_date,
                c.primary_runoff_election_date,
                c.general_runoff_election_date
            )
        qualify
            row_number() over (
                partition by a.user_id
                order by
                    case
                        when c.general_election_date = a.election_date then 0 else 1
                    end,
                    c.gp_candidacy_id
            )
            = 1
    ),

    -- Filing deadline through the position and the election date, the only
    -- route that reaches users whose candidacy never passed corroboration.
    -- Where one user matches two election rows they carry the same deadline.
    anchor_filing as (
        select a.user_id, max(e.filing_deadline) as filing_deadline
        from anchor as a
        inner join
            anchor_campaign as ac
            on ac.campaign_version_id = a.anchor_campaign_version_id
        inner join
            {{ ref("election") }} as e
            on e.br_position_database_id = ac.ballotready_position_id
            and a.election_date
            in (e.election_date, e.general_election_date, e.primary_election_date)
        group by a.user_id
    ),

    -- Every election year this person has run in, from both the product and
    -- the civics record. The product table alone is current-race-only, which
    -- undercounts re-running.
    run_years as (
        select user_id, year(election_date) as election_year
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and not coalesce(is_demo, false)
            and election_date between date '2000-01-01' and date '2100-01-01'
        union
        select
            k.user_id, year(coalesce(c.general_election_date, c.primary_election_date))
        from keys as k
        inner join {{ ref("candidacy") }} as c using (gp_person_id)
        where coalesce(c.general_election_date, c.primary_election_date) is not null
    ),

    prior_runs as (
        select
            a.user_id,
            count(distinct r.election_year) filter (
                where r.election_year < year(a.election_date)
            ) as prior_election_count,
            count(distinct r.election_year) as election_year_count
        from anchor as a
        inner join run_years as r on r.user_id = a.user_id
        where a.has_candidacy
        group by a.user_id
    ),

    -- Person grain on purpose: a win on another account or a civics-only
    -- candidacy is still this human having won.
    wins as (
        select distinct k.user_id
        from keys as k
        inner join {{ ref("candidacy") }} as c using (gp_person_id)
        where c.latest_stage_result = 'Won'
    )

select
    a.user_id,
    a.has_candidacy,

    case when a.has_candidacy then a.anchor_campaign_id end as candidacy_campaign_id,
    case when a.has_candidacy then ac.campaign_office end as candidacy_office,
    case
        when a.has_candidacy then ac.normalized_position_name
    end as candidacy_normalized_position_name,
    case when a.has_candidacy then ac.campaign_state end as candidacy_state,
    case when a.has_candidacy then ac.campaign_party end as candidacy_party,
    case when a.has_candidacy then ac.partisan_type end as candidacy_partisan_type,
    a.election_date as candidacy_election_date,
    case when a.has_candidacy then ac.election_level end as candidacy_election_level,
    f.filing_deadline as candidacy_filing_deadline,
    case
        when a.has_candidacy then ac.ballotready_position_id
    end as candidacy_position_id,
    cc.office_level as candidacy_office_level,
    case when a.has_candidacy then ac.is_pledged end as candidacy_is_pledged,
    case when a.has_candidacy then ac.is_verified end as candidacy_is_verified,
    -- Not candidacy_is_verified, which is the product's own flag.
    case
        when a.has_candidacy then coalesce(cc.has_external_match, false)
    end as candidacy_has_external_match,
    case when a.has_candidacy then ac.is_pro end as candidacy_is_pro,
    case when a.has_candidacy then civ.icp_office_win end as candidacy_is_win_icp,
    case when a.has_candidacy then civ.voter_count end as candidacy_voter_count,

    cc.latest_stage_result as candidacy_result,
    cc.latest_stage_reached as candidacy_latest_stage_reached,
    -- Never null. A user with no candidacy is neither running nor a
    -- non-filer, and a null would drop them out of a `where not` filter.
    coalesce(a.election_date >= current_date(), false) as is_still_running,
    coalesce(
        a.election_date < current_date()
        and not coalesce(cc.has_ballot_evidence, false),
        false
    ) as never_filed,
    case when a.has_candidacy then civ.is_incumbent end as candidacy_is_incumbent,
    case when a.has_candidacy then civ.viability_score end as candidacy_viability_score,
    case when a.has_candidacy then civ.win_number end as candidacy_win_number,

    case
        when a.has_candidacy then coalesce(p.prior_election_count, 0)
    end as prior_election_count,
    case
        when a.has_candidacy then coalesce(p.prior_election_count, 0) > 0
    end as has_run_before,
    -- Counts later runs too. The anchor is often a stale product campaign
    -- while the civics record shows the same person running again, so
    -- has_run_before alone misses most re-runners.
    p.election_year_count,
    w.user_id is not null as ever_won
from anchor as a
left join anchor_campaign as ac on ac.campaign_version_id = a.anchor_campaign_version_id
left join anchor_civics as civ on civ.campaign_version_id = a.anchor_campaign_version_id
left join anchor_candidacy as cc on cc.user_id = a.user_id
left join anchor_filing as f on f.user_id = a.user_id
left join prior_runs as p on p.user_id = a.user_id
left join wins as w on w.user_id = a.user_id
