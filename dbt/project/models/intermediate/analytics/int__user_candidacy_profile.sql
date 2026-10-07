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
            c.primary_election_date,
            c.general_election_date,
            c.general_election_result,
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

    -- The candidacy table carries a general result but no primary one. A few
    -- candidacies hold two primary stage rows; max keeps a non-null result.
    primary_results as (
        select gp_candidacy_id, max(election_result) as primary_result
        from {{ ref("candidacy_stage") }}
        where election_stage = 'primary'
        group by gp_candidacy_id
    ),

    -- The reporting grain for office, keyed on the anchor campaign's
    -- BallotReady position. Candidacies with no position stay null.
    office_category as (
        select br_position_database_id, office_category
        from {{ ref("int__civics_position_office_type") }}
    ),

    -- Scored per campaign, so it attaches to the anchor like every other
    -- candidacy column.
    legitimacy as (
        select
            campaign_id,
            legitimacy_score,
            legitimacy_bucket,
            legitimacy_label,
            scored_at
        from {{ ref("stg_model_predictions__win_candidate_scores_latest") }}
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
    -- The raw party varies in casing and suffix by signup flow, which the raw
    -- column keeps as a source tell. Grouped through the same macro the civics
    -- candidacy models apply to this field, so the two agree.
    case
        when a.has_candidacy then {{ parse_party_affiliation("ac.campaign_party") }}
    end as candidacy_party_normalized,
    case
        when a.has_candidacy then nullif(trim(ac.partisan_type), '')
    end as candidacy_partisan_type,
    a.election_date as candidacy_election_date,
    case when a.has_candidacy then ac.election_level end as candidacy_election_level,
    f.filing_deadline as candidacy_filing_deadline,
    case
        when a.has_candidacy then ac.ballotready_position_id
    end as candidacy_position_id,
    case when a.has_candidacy then oc.office_category end as candidacy_office_category,
    cc.office_level as candidacy_office_level,
    -- The civics value arrives in several casings plus a few labels that are
    -- not levels. BallotReady's level is the more specific one (it separates
    -- township and local districts), so it wins, and the product's own level
    -- fills the gaps.
    case
        when a.has_candidacy
        then
            coalesce(
                case
                    lower(trim(cc.office_level))
                    when 'federal'
                    then 'federal'
                    when 'presidential'
                    then 'federal'
                    when 'state'
                    then 'state'
                    when 'state legislative'
                    then 'state'
                    when 'statewide'
                    then 'state'
                    when 'county'
                    then 'county'
                    when 'city'
                    then 'city'
                    when 'township'
                    then 'township'
                    when 'town'
                    then 'township'
                    when 'local'
                    then 'local'
                    when 'regional'
                    then 'regional'
                end,
                -- election_level is free text upstream, so it is held to the
                -- same set rather than passed through.
                case
                    when
                        lower(trim(ac.election_level)) in (
                            'federal',
                            'state',
                            'county',
                            'city',
                            'township',
                            'local',
                            'regional'
                        )
                    then lower(trim(ac.election_level))
                end
            )
    end as candidacy_office_level_normalized,
    case when a.has_candidacy then ac.is_pledged end as candidacy_is_pledged,
    case when a.has_candidacy then ac.is_verified end as candidacy_is_verified,
    -- Not candidacy_is_verified, which is the product's own flag.
    case
        when a.has_candidacy then coalesce(cc.has_external_match, false)
    end as candidacy_has_external_match,
    case when a.has_candidacy then ac.is_pro end as candidacy_is_pro,
    case when a.has_candidacy then civ.icp_office_win end as candidacy_is_win_icp,
    case when a.has_candidacy then civ.voter_count end as candidacy_voter_count,
    -- Edges at the ICP cutoffs (Win 500 to 100,000, Serve from 1,000, both
    -- inclusive), so the bucket says whether the race passes the ICP's size
    -- rule. The office rules live in candidacy_is_win_icp.
    case
        when not a.has_candidacy or civ.voter_count is null
        then null
        when civ.voter_count < 500
        then '1. under 500'
        when civ.voter_count < 1000
        then '2. 500 to 999'
        when civ.voter_count < 5000
        then '3. 1K to 5K'
        when civ.voter_count < 10000
        then '4. 5K to 10K'
        when civ.voter_count < 25000
        then '5. 10K to 25K'
        when civ.voter_count < 50000
        then '6. 25K to 50K'
        when civ.voter_count <= 100000
        then '7. 50K to 100K'
        else '8. over 100K'
    end as candidacy_voter_count_bucket,

    cc.latest_stage_result as candidacy_result,
    cc.latest_stage_reached as candidacy_latest_stage_reached,
    cc.primary_election_date as candidacy_primary_election_date,
    pr.primary_result as candidacy_primary_result,
    cc.general_election_date as candidacy_general_election_date,
    cc.general_election_result as candidacy_general_result,
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
        when a.has_candidacy then lg.legitimacy_score
    end as candidacy_legitimacy_score,
    case
        when a.has_candidacy then lg.legitimacy_bucket
    end as candidacy_legitimacy_bucket,
    -- A candidacy already matched to a ballot data vendor needs no prediction,
    -- so it reads confirmed whether or not the model scored it.
    case
        when not a.has_candidacy
        then null
        when coalesce(cc.has_external_match, false)
        then '6. confirmed'
        else lg.legitimacy_label
    end as candidacy_legitimacy_label,
    case when a.has_candidacy then lg.scored_at end as candidacy_legitimacy_scored_at,

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
left join primary_results as pr on pr.gp_candidacy_id = cc.gp_candidacy_id
left join anchor_filing as f on f.user_id = a.user_id
left join prior_runs as p on p.user_id = a.user_id
left join wins as w on w.user_id = a.user_id
left join legitimacy as lg on lg.campaign_id = a.anchor_campaign_id
left join
    office_category as oc on oc.br_position_database_id = ac.ballotready_position_id
