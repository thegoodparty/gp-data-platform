{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns × BR election stages -> Civics mart candidacy_stage schema.
-- Grain: one row per (latest-version campaign × BR election_stage) for matching
-- (br_position_id, election_year). PD doesn't track stage grain natively, so
-- campaigns without a BR position are dropped and BR's stage inventory drives
-- the cross-join.
--
-- BR's stage inventory is 2026+ only, so pre-2026 PD campaigns produce zero
-- rows here. They're still present in int__civics_candidacy_gp_api at campaign
-- grain; historical stage-grain data lives in int__civics_candidacy_stage_2025.
with
    latest_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where is_latest_version and ballotready_position_id is not null
    ),

    -- Source from the users mart (same as candidate/candidacy gp_api) to keep
    -- person hashes aligned across the three models.
    users as (select user_id, first_name, last_name from {{ ref("users") }}),

    br_stages as (
        select
            gp_election_stage_id,
            br_race_id,
            br_position_id,
            stage_type,
            election_date as stage_election_date,
            year(election_date) as stage_election_year
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    -- Carry the canonical BR stage's date through as br_election_stage_date
    -- so consumers can compare against PD's stage_election_date without
    -- re-resolving the canonical stage downstream.
    er_canonical_stage as (
        select
            xw.gp_api_campaign_id,
            xw.gp_api_stage_election_date,
            xw.canonical_gp_candidacy_stage_id,
            xw.canonical_gp_election_stage_id,
            br.stage_election_date as br_election_stage_date
        from {{ ref("int__civics_er_canonical_ids") }} as xw
        left join
            br_stages as br
            on xw.canonical_gp_election_stage_id = br.gp_election_stage_id
        where xw.gp_api_campaign_id is not null
    ),

    -- Cascade canonical_gp_candidacy_id across all stages of a matched
    -- campaign so sibling stages align with candidacy_gp_api's gp_candidacy_id.
    -- max() not any_value(): deterministic, must match candidacy_gp_api.
    er_canonical_candidacy as (
        select
            gp_api_campaign_id,
            max(canonical_gp_candidacy_id) as canonical_gp_candidacy_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- Match each PD campaign to the specific BR race at the user's pledged
    -- election_date. PD's campaign.election_date identifies a single stage
    -- (the user pledged to one date), so we land on one BR race per campaign
    -- — no fan-out across stages, no orphan rows for stages PD didn't pledge
    -- to. Falls back to the BR stage closest in date when no exact match
    -- exists for the position+year (handles slightly stale PD dates).
    -- Mirrors the same logic applied in int__er_prematch_candidacy_stages.
    campaign_stages as (
        select
            c.campaign_id,
            u.first_name as user_first_name,
            u.last_name as user_last_name,
            c.campaign_party,
            c.campaign_state as state,
            c.campaign_office as candidate_office,
            c.election_date as general_election_date,
            c.created_at,
            c.updated_at,
            {{ parse_party_affiliation("c.campaign_party") }} as party_affiliation,
            {{ extract_district_geographic("c.normalized_position_name") }} as district,
            br.gp_election_stage_id as br_gp_election_stage_id,
            br.stage_election_date
        from latest_campaigns as c
        inner join users as u on c.user_id = u.user_id
        inner join
            br_stages as br
            on c.ballotready_position_id = br.br_position_id
            and year(c.election_date) = br.stage_election_year
        -- One row per campaign at the BR race matching c.election_date most
        -- closely. Exact-date hits get datediff = 0; if PD's date is slightly
        -- off, we still land on the nearest BR stage rather than fanning out.
        -- br_race_id desc breaks ties deterministically when a position has
        -- both a regular and a special race on the same day.
        qualify
            row_number() over (
                partition by c.campaign_id
                order by
                    abs(datediff(br.stage_election_date, c.election_date)) asc,
                    br.br_race_id desc nulls last
            )
            = 1
    ),

    with_er as (
        select
            cs.*,
            xw_c.canonical_gp_candidacy_id,
            xw_s.canonical_gp_candidacy_stage_id,
            xw_s.canonical_gp_election_stage_id,
            xw_s.br_election_stage_date
        from campaign_stages as cs
        left join
            er_canonical_candidacy as xw_c on cs.campaign_id = xw_c.gp_api_campaign_id
        left join
            er_canonical_stage as xw_s
            on cs.campaign_id = xw_s.gp_api_campaign_id
            and cs.stage_election_date = xw_s.gp_api_stage_election_date
    ),

    with_ids as (
        select
            -- Must match int__civics_candidacy_gp_api's hash.
            coalesce(
                canonical_gp_candidacy_id,
                {{
                    generate_gp_api_gp_candidacy_id(
                        first_name="user_first_name", last_name="user_last_name"
                    )
                }}
            ) as gp_candidacy_id,

            -- Either branch resolves to a valid gp_election_stage_id in
            -- int__civics_election_stage_ballotready.
            coalesce(
                canonical_gp_election_stage_id, br_gp_election_stage_id
            ) as gp_election_stage_id,

            canonical_gp_candidacy_stage_id,
            campaign_id,
            user_first_name,
            user_last_name,
            campaign_party,
            stage_election_date,
            br_election_stage_date,
            created_at,
            updated_at
        from with_er
    ),

    -- Only include stages whose candidacy resolves in int__civics_candidacy_gp_api
    valid_candidacies as (
        select gp_candidacy_id from {{ ref("int__civics_candidacy_gp_api") }}
    ),

    filtered as (
        select with_ids.*
        from with_ids
        inner join
            valid_candidacies
            on with_ids.gp_candidacy_id = valid_candidacies.gp_candidacy_id
    ),

    final_stages as (
        select
            coalesce(
                canonical_gp_candidacy_stage_id,
                {{
                    generate_salted_uuid(
                        fields=["gp_candidacy_id", "gp_election_stage_id"]
                    )
                }}
            ) as gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
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
            stage_election_date as election_stage_date,
            br_election_stage_date,
            created_at,
            updated_at
        from filtered
    ),

    deduplicated as (
        select *
        from final_stages
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    )

select
    gp_candidacy_stage_id,
    gp_candidacy_id,
    gp_election_stage_id,
    candidate_name,
    source_candidate_id,
    source_race_id,
    candidate_party,
    is_winner,
    election_result,
    election_result_source,
    match_confidence,
    match_reasoning,
    match_top_candidates,
    has_match,
    votes_received,
    election_stage_date,
    br_election_stage_date,
    created_at,
    updated_at
from deduplicated
