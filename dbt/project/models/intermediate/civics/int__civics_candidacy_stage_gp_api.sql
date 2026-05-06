{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns clustered by ER -> Civics mart candidacy_stage rows.
-- Grain: one row per ER-clustered gp_api campaign (with a BR-anchored
-- canonical_gp_election_stage_id). Campaigns that didn't cluster with a
-- BR-anchored stage get no row here — PD has no native stage signal, so
-- without ER alignment we don't know which stage the user pledged to.
-- Stage assignment comes deterministically from the canonical
-- gp_election_stage_id resolved by Splink, not from a closest-date guess
-- against BR's race spine.
with
    latest_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where is_latest_version and ballotready_position_id is not null
    ),

    -- Source from the users mart (same as candidate/candidacy gp_api) to keep
    -- person hashes aligned across the three models.
    users as (select user_id, first_name, last_name from {{ ref("users") }}),

    -- ER crosswalk rows for gp_api campaigns. Only BR-anchored cluster
    -- branches set canonical_gp_election_stage_id; non-BR clusters
    -- (e.g., gp_api + DDHQ only) leave it null and don't emit a stage row
    -- here. canonical_gp_candidacy_stage_id is also null on those branches,
    -- so the filter below is equivalent to "BR-anchored clusters only."
    er_canonical_stage as (
        select
            gp_api_campaign_id,
            canonical_gp_candidacy_id,
            canonical_gp_candidacy_stage_id,
            canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where
            gp_api_campaign_id is not null
            and canonical_gp_election_stage_id is not null
    ),

    -- Look up the stage's election_date by canonical id (deterministic
    -- ID join, not a closest-date guess). The stage already exists in BR's
    -- spine because the canonical was resolved through it.
    br_stage_dates as (
        select gp_election_stage_id, election_date as stage_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    enriched as (
        select
            xw.gp_api_campaign_id as campaign_id,
            xw.canonical_gp_candidacy_id,
            xw.canonical_gp_candidacy_stage_id,
            xw.canonical_gp_election_stage_id,
            br.stage_election_date as election_stage_date,
            u.first_name as user_first_name,
            u.last_name as user_last_name,
            c.campaign_party,
            c.created_at,
            c.updated_at
        from er_canonical_stage as xw
        inner join latest_campaigns as c on xw.gp_api_campaign_id = c.campaign_id
        inner join users as u on c.user_id = u.user_id
        left join
            br_stage_dates as br
            on xw.canonical_gp_election_stage_id = br.gp_election_stage_id
    ),

    -- Only include stages whose candidacy resolves in int__civics_candidacy_gp_api
    valid_candidacies as (
        select gp_candidacy_id from {{ ref("int__civics_candidacy_gp_api") }}
    ),

    filtered as (
        select e.*
        from enriched as e
        inner join
            valid_candidacies as vc on e.canonical_gp_candidacy_id = vc.gp_candidacy_id
    ),

    deduplicated as (
        select *
        from filtered
        qualify
            row_number() over (
                partition by canonical_gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    )

select
    canonical_gp_candidacy_stage_id as gp_candidacy_stage_id,
    canonical_gp_candidacy_id as gp_candidacy_id,
    canonical_gp_election_stage_id as gp_election_stage_id,
    concat(user_first_name, ' ', user_last_name) as candidate_name,
    cast(campaign_id as string) as source_candidate_id,
    cast(null as string) as source_race_id,
    campaign_party as candidate_party,
    cast(null as boolean) as is_winner,
    cast(null as string) as election_result,
    cast(null as string) as election_result_source,
    cast(null as float) as match_confidence,
    cast(null as string) as match_reasoning,
    cast(null as string) as match_top_candidates,
    false as has_match,
    cast(null as string) as votes_received,
    election_stage_date,
    election_stage_date as br_election_stage_date,
    created_at,
    updated_at
from deduplicated
