{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns -> Civics mart candidacy_stage rows.
-- Grain: one row per latest-version gp_api campaign whose pledge resolves
-- to a BR election stage. BR-stage resolution prefers PD's pledged race
-- id (details:raceid → ballotready_race_id), falling back to the closest
-- BR stage at the same (ballotready_position_id, year(election_date)) for
-- the residual without a race id.
-- gp_candidacy_stage_id prefers the ER-resolved canonical when available;
-- otherwise self-derives from (gp_candidacy_id, gp_election_stage_id).
with
    latest_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and (ballotready_race_id is not null or ballotready_position_id is not null)
    ),

    -- Source from the users mart so person hashes align with candidacy_gp_api.
    users as (select user_id, first_name, last_name from {{ ref("users") }}),

    br_stages as (
        select
            br_race_id,
            br_position_id,
            gp_election_stage_id,
            election_date as stage_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    campaign_br_stage as (
        select
            c.*,
            coalesce(
                br_by_race.gp_election_stage_id, br_by_pos.gp_election_stage_id
            ) as gp_election_stage_id,
            coalesce(
                br_by_race.stage_election_date, br_by_pos.stage_election_date
            ) as stage_election_date
        from latest_campaigns as c
        left join
            br_stages as br_by_race on c.ballotready_race_id = br_by_race.br_race_id
        left join
            br_stages as br_by_pos
            on br_by_race.br_race_id is null
            and c.ballotready_position_id = br_by_pos.br_position_id
            and year(br_by_pos.stage_election_date) = year(c.election_date)
        qualify
            row_number() over (
                partition by c.campaign_id
                order by
                    case when br_by_race.br_race_id is not null then 0 else 1 end,
                    abs(datediff(br_by_pos.stage_election_date, c.election_date)) asc,
                    br_by_pos.br_race_id desc nulls last
            )
            = 1
    ),

    -- canonical_gp_candidacy_id is candidacy-grain (same across all stages of
    -- a campaign), so max() across the campaign's er_canonical_ids rows is
    -- safe. max() not any_value(): deterministic across runs, must align with
    -- int__civics_candidacy_gp_api.
    er_canonical_candidacy as (
        select
            gp_api_campaign_id,
            max(canonical_gp_candidacy_id) as canonical_gp_candidacy_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- canonical_gp_candidacy_stage_id is stage-grain — different per stage row
    -- in er_canonical_ids. Carry canonical_gp_election_stage_id alongside it
    -- so the join below can match canonicals to the specific stage we
    -- resolved from PD's raceId; mismatches fall through to the self-derived
    -- salted UUID.
    er_canonical_stage as (
        select
            gp_api_campaign_id,
            canonical_gp_election_stage_id,
            canonical_gp_candidacy_stage_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where
            gp_api_campaign_id is not null
            and canonical_gp_election_stage_id is not null
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
            c.gp_election_stage_id,
            c.stage_election_date,
            xw_c.canonical_gp_candidacy_id,
            xw_s.canonical_gp_candidacy_stage_id
        from campaign_br_stage as c
        inner join users as u on c.user_id = u.user_id
        left join
            er_canonical_candidacy as xw_c on c.campaign_id = xw_c.gp_api_campaign_id
        left join
            er_canonical_stage as xw_s
            on c.campaign_id = xw_s.gp_api_campaign_id
            and c.gp_election_stage_id = xw_s.canonical_gp_election_stage_id
        where c.gp_election_stage_id is not null
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
