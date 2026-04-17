-- BallotReady candidacy stages → Civics mart candidacy_stage schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
--
-- Grain: One row per candidacy stage (candidacy + election stage)
--
-- Each row in the BR S3 data IS a candidacy stage (candidate in a specific race).
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            election_day >= '2026-01-01'
            and first_name is not null
            and last_name is not null
            and state is not null
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
            -- Parse party from parties JSON
            {{ parse_party_affiliation("candidacies.parties") }} as party_affiliation,
            br_position.partisan_type
        from candidacies
        left join br_position on candidacies.br_position_id = br_position.database_id
    ),

    -- Look up election dates to generate gp_candidacy_id
    -- Must use the same year-based grouping as int__civics_candidacy_ballotready
    -- to ensure IDs match across models (br_election_id differs between primary
    -- and general stages, so grouping by year rolls them up correctly)
    candidacy_election_dates as (
        select
            br_candidate_id,
            br_position_id,
            year(election_day) as election_year,
            max(
                case when not is_primary and not is_runoff then election_day end
            ) as general_election_date,
            max(
                case when is_primary and not is_runoff then election_day end
            ) as primary_election_date,
            max(
                case when not is_primary and is_runoff then election_day end
            ) as general_runoff_election_date,
            max(
                case when is_primary and is_runoff then election_day end
            ) as primary_runoff_election_date
        from candidacies
        group by br_candidate_id, br_position_id, year(election_day)
    ),

    candidacy_stages as (
        select
            -- gp_candidacy_stage_id = hash(gp_candidacy_id, br_race_id)
            -- First compute gp_candidacy_id inline
            {{
                generate_salted_uuid(
                    fields=[
                        "s.first_name",
                        "s.last_name",
                        "s.state",
                        "s.party_affiliation",
                        "s.candidate_office",
                        "cast(coalesce(ced.general_election_date, ced.primary_election_date, ced.general_runoff_election_date, ced.primary_runoff_election_date) as string)",
                        "s.district",
                    ]
                )
            }}
            as computed_gp_candidacy_id,

            {{
                generate_salted_uuid(
                    fields=[
                        generate_salted_uuid(
                            fields=[
                                "s.first_name",
                                "s.last_name",
                                "s.state",
                                "s.party_affiliation",
                                "s.candidate_office",
                                "cast(coalesce(ced.general_election_date, ced.primary_election_date, ced.general_runoff_election_date, ced.primary_runoff_election_date) as string)",
                                "s.district",
                            ]
                        ),
                        "s.br_race_id",
                    ]
                )
            }}
            as gp_candidacy_stage_id,

            -- gp_election_stage_id from br_race_id
            {{ generate_salted_uuid(fields=["s.br_race_id"]) }} as gp_election_stage_id,

            concat(s.first_name, ' ', s.last_name) as candidate_name,
            cast(s.br_candidate_id as string) as source_candidate_id,
            cast(s.br_race_id as string) as source_race_id,
            s.party_affiliation as candidate_party,

            -- is_winner
            case
                when s.election_result in ('WON', 'GENERAL_WIN', 'PRIMARY_WIN')
                then true
                when s.election_result in ('LOST', 'LOSS')
                then false
                else null
            end as is_winner,

            -- election_result mapped to standard values
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
            candidacy_election_dates as ced
            on s.br_candidate_id = ced.br_candidate_id
            and s.br_position_id = ced.br_position_id
            and year(s.election_day) = ced.election_year
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
        from candidacy_stages as stage
        inner join
            valid_candidacies
            on stage.computed_gp_candidacy_id = valid_candidacies.gp_candidacy_id
        inner join
            valid_election_stages
            on stage.gp_election_stage_id = valid_election_stages.gp_election_stage_id
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
