{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns × BR election stages -> Civics mart candidacy_stage schema.
--
-- Grain: One row per (latest-version campaign × BR election_stage) for
-- matching (br_position_id, election_year) pairs. A single campaign may emit
-- multiple rows (one per BR stage for its position+year). Campaigns without a
-- ballotready_position_id are dropped — Product DB doesn't track stage grain
-- natively, so we rely on BR's stage inventory.
--
-- Schema aligned with int__civics_candidacy_stage_techspeed. Stage-level
-- result fields (is_winner, election_result, votes_received) are null —
-- Product DB's `did_win` flag is candidacy-grain, not stage-grain.
with
    latest_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where is_latest_version and ballotready_position_id is not null
    ),

    -- Source user fields from the users mart (same as int__civics_candidate_gp_api
    -- and int__civics_candidacy_gp_api) to keep person hashes consistent across
    -- the three models even when the campaigns mart's denormalized user_*
    -- fields drift from the users mart's snapshot.
    users as (select user_id, first_name, last_name from {{ ref("users") }}),

    br_stages as (
        select
            gp_election_stage_id,
            br_position_id,
            election_date as stage_election_date,
            year(election_date) as stage_election_year
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    -- Per-user state for gp_candidacy_id consistency with candidacy_gp_api
    -- (uses the per-campaign campaign_state, not user_state — keep that clear).
    -- NOTE: gp_candidacy_id in candidacy_gp_api uses c.campaign_state (not
    -- user_state), so we do the same here.
    -- ER lookup at stage grain: canonical IDs that apply only to the specific
    -- (campaign, stage_date) pair.
    er_canonical_stage as (
        select
            gp_api_campaign_id,
            gp_api_stage_election_date,
            canonical_gp_candidacy_stage_id,
            canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
    ),

    -- ER lookup at candidacy grain: cascade canonical_gp_candidacy_id across
    -- all stages of a matched campaign (so the sibling stage of a clustered
    -- stage aligns with candidacy_gp_api's gp_candidacy_id).
    er_canonical_candidacy as (
        select
            gp_api_campaign_id,
            any_value(canonical_gp_candidacy_id) as canonical_gp_candidacy_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- Cross-join campaigns × BR stages for same position+year.
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
            nullif(c.campaign_party, '') as party_affiliation,
            coalesce(
                regexp_extract(
                    c.normalized_position_name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district,
            br.gp_election_stage_id as br_gp_election_stage_id,
            br.stage_election_date
        from latest_campaigns as c
        inner join users as u on c.user_id = u.user_id
        inner join
            br_stages as br
            on c.ballotready_position_id = br.br_position_id
            and year(c.election_date) = br.stage_election_year
    ),

    with_er as (
        select
            cs.*,
            xw_c.canonical_gp_candidacy_id,
            xw_s.canonical_gp_candidacy_stage_id,
            xw_s.canonical_gp_election_stage_id
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
            -- gp_candidacy_id: must match int__civics_candidacy_gp_api's hash.
            coalesce(
                canonical_gp_candidacy_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "user_first_name",
                            "user_last_name",
                            "state",
                            "party_affiliation",
                            "candidate_office",
                            "cast(general_election_date as string)",
                            "district",
                        ]
                    )
                }}
            ) as gp_candidacy_id,

            -- gp_election_stage_id: adopt BR canonical from ER or fall back to
            -- the BR stage we inner-joined on. Either way every row has a
            -- valid gp_election_stage_id in int__civics_election_stage_ballotready.
            coalesce(
                canonical_gp_election_stage_id, br_gp_election_stage_id
            ) as gp_election_stage_id,

            canonical_gp_candidacy_stage_id,
            campaign_id,
            user_first_name,
            user_last_name,
            campaign_party,
            stage_election_date,
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
    created_at,
    updated_at
from deduplicated
