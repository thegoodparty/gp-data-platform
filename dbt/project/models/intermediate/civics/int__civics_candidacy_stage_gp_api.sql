{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns -> Civics mart candidacy_stage rows.
-- Grain: one row per latest-version pledged gp_api campaign whose
-- ballotready_race_id resolves in BR's election_stage spine.
-- gp_candidacy_stage_id prefers the ER-resolved canonical when available;
-- otherwise self-derives from (gp_candidacy_id, gp_election_stage_id).
with
    latest_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and ballotready_position_id is not null
            and ballotready_race_id is not null
    ),

    -- Source from the users mart so person hashes align with candidacy_gp_api.
    users as (select user_id, first_name, last_name from {{ ref("users") }}),

    br_stages as (
        select br_race_id, gp_election_stage_id, election_date as stage_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    -- max() not any_value(): deterministic across runs, must align with
    -- int__civics_candidacy_gp_api.
    er_canonical as (
        select
            gp_api_campaign_id,
            max(canonical_gp_candidacy_id) as canonical_gp_candidacy_id,
            max(canonical_gp_candidacy_stage_id) as canonical_gp_candidacy_stage_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- Columns aliased to match what generate_gp_api_gp_candidacy_id expects
    -- in scope, so the unclustered fallback hash matches the corresponding
    -- hash in int__civics_candidacy_gp_api.
    enriched as (
        select
            c.campaign_id,
            c.campaign_party,
            c.campaign_state as state,
            c.campaign_office as candidate_office,
            c.election_date as general_election_date,
            {{ parse_party_affiliation("c.campaign_party") }} as party_affiliation,
            {{ extract_district_geographic("c.normalized_position_name") }} as district,
            c.created_at,
            c.updated_at,
            u.first_name as user_first_name,
            u.last_name as user_last_name,
            br.gp_election_stage_id,
            br.stage_election_date,
            xw.canonical_gp_candidacy_id,
            xw.canonical_gp_candidacy_stage_id
        from latest_campaigns as c
        inner join users as u on c.user_id = u.user_id
        inner join br_stages as br on c.ballotready_race_id = br.br_race_id
        left join er_canonical as xw on c.campaign_id = xw.gp_api_campaign_id
    ),

    with_ids as (
        select
            -- Must match int__civics_candidacy_gp_api's hash for unclustered.
            coalesce(
                canonical_gp_candidacy_id,
                {{
                    generate_gp_api_gp_candidacy_id(
                        first_name="user_first_name", last_name="user_last_name"
                    )
                }}
            ) as gp_candidacy_id,
            gp_election_stage_id,
            canonical_gp_candidacy_stage_id,
            campaign_id,
            user_first_name,
            user_last_name,
            campaign_party,
            stage_election_date,
            created_at,
            updated_at
        from enriched
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
            stage_election_date as br_election_stage_date,
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
