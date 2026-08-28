-- BallotReady candidacy stages → Civics mart candidacy_stage schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (all-time)
-- Grain: one row per candidacy stage. Each BR S3 row IS a candidacy stage.
-- <=2025 rows are emitted so they can enrich the HubSpot archive era in
-- candidacy_stage; they keep BR-derived ids that are not in the (still
-- 2026-gated) BR candidacy / election_stage marts, so the mart only surfaces
-- them by merging onto an archive person-stage.
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where first_name is not null and last_name is not null and state is not null
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    candidacies_with_fields as (
        select
            candidacies.*,
            {{
                generate_candidate_office_from_position(
                    "candidacies.position_name",
                    "candidacies.normalized_position_name",
                )
            }} as candidate_office,
            initcap(candidacies.level) as office_level,
            {{ extract_city_from_office_name("candidacies.position_name") }} as city,
            {{ extract_district_geographic("candidacies.position_name") }} as district,
            {{ parse_party_affiliation("candidacies.parties") }} as party_affiliation,
            br_position.partisan_type
        from candidacies
        left join br_position on candidacies.br_position_id = br_position.database_id
    ),

    -- Stage-grain cluster mints; unclustered rows self-mint (single-member
    -- semantics).
    candidacy_mint as (
        select source_id as br_candidacy_id, minted_gp_candidacy_id
        from {{ ref("int__civics_minted_candidacy_ids") }}
        where source_name = 'ballotready' and cluster_br_members = 1
    ),

    es_mint as (
        select source_id as br_race_id, minted_gp_election_stage_id
        from {{ ref("int__civics_minted_election_stage_ids") }}
        where source_name = 'ballotready' and cluster_br_members = 1
    ),

    candidacy_stages as (
        select
            -- Candidacy-grain rollup of the stage-grain mint. Must match
            -- int__civics_candidacy_ballotready: rows here are exactly its
            -- name/state-qualified subset, so the same min is reproduced.
            min(
                coalesce(
                    cm.minted_gp_candidacy_id,
                    {{
                        generate_salted_uuid(
                            fields=[
                                "'ballotready'",
                                "cast(s.br_candidacy_id as string)",
                            ],
                            salt="candidacy",
                        )
                    }}
                )
            ) over (
                partition by s.br_candidate_id, s.br_position_id, year(s.election_day)
            ) as computed_gp_candidacy_id,

            -- Must match int__civics_election_stage_ballotready.
            coalesce(
                em.minted_gp_election_stage_id,
                {{
                    generate_salted_uuid(
                        fields=["'ballotready'", "cast(s.br_race_id as string)"],
                        salt="election_stage",
                    )
                }}
            ) as gp_election_stage_id,

            cast(s.br_candidacy_id as string) as br_candidacy_id,
            concat(s.first_name, ' ', s.last_name) as candidate_name,
            cast(s.br_candidate_id as string) as source_candidate_id,
            cast(s.br_race_id as string) as source_race_id,
            s.party_affiliation as candidate_party,

            case
                when s.election_result in ('WON', 'GENERAL_WIN', 'PRIMARY_WIN')
                then true
                when s.election_result in ('LOST', 'LOSS')
                then false
                else null
            end as is_winner,

            case
                when s.election_result in ('WON', 'GENERAL_WIN')
                then 'Won'
                when s.election_result in ('LOST', 'LOSS')
                then 'Lost'
                when s.election_result = 'PRIMARY_WIN'
                then 'Won'
                when s.election_result = 'RUNOFF'
                then 'Runoff'
                else null
            end as election_result,

            'ballotready' as election_result_source,

            -- Native stage so <=2025 BR rows can merge at the archive
            -- person-stage grain (BR S3 has no 'special' variants).
            case
                when s.is_primary and s.is_runoff
                then 'primary runoff'
                when s.is_primary
                then 'primary'
                when s.is_runoff
                then 'general runoff'
                else 'general'
            end as election_stage,

            cast(null as float) as match_confidence,
            cast(null as string) as match_reasoning,
            cast(null as string) as match_top_candidates,
            s.election_result is not null as has_match,
            cast(null as string) as votes_received,
            s.election_day as election_stage_date,
            s._airbyte_extracted_at as created_at,
            s._airbyte_extracted_at as updated_at

        from candidacies_with_fields as s
        left join
            candidacy_mint as cm
            on cast(s.br_candidacy_id as string) = cm.br_candidacy_id
        left join es_mint as em on cast(s.br_race_id as string) = em.br_race_id
    ),

    -- Separate select: computed_gp_candidacy_id is a window result above, so
    -- the PK composition cannot lateral-reference it in the same block.
    with_stage_pk as (
        select
            *,
            {{
                generate_salted_uuid(
                    fields=["computed_gp_candidacy_id", "gp_election_stage_id"]
                )
            }} as gp_candidacy_stage_id
        from candidacy_stages
    ),

    -- Only include stages with valid candidacy and election_stage references
    valid_candidacies as (
        select gp_candidacy_id from {{ ref("int__civics_candidacy_ballotready") }}
    ),

    valid_election_stages as (
        select gp_election_stage_id
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    filtered as (
        select stage.*
        from with_stage_pk as stage
        left join
            valid_candidacies
            on stage.computed_gp_candidacy_id = valid_candidacies.gp_candidacy_id
        left join
            valid_election_stages
            on stage.gp_election_stage_id = valid_election_stages.gp_election_stage_id
        -- 2026+ keeps the historical FK gate (byte-stable); <=2025 rows pass
        -- through ungated to enrich the archive era in candidacy_stage.
        where
            stage.election_stage_date <= '2025-12-31'
            or (
                valid_candidacies.gp_candidacy_id is not null
                and valid_election_stages.gp_election_stage_id is not null
            )
    ),

    deduplicated as (
        select *
        from filtered
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    )

select
    gp_candidacy_stage_id,
    computed_gp_candidacy_id as gp_candidacy_id,
    gp_election_stage_id,
    br_candidacy_id,
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
    election_stage,
    created_at,
    updated_at
from deduplicated
