-- BallotReady candidacy stages → Civics mart candidacy_stage schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
--
-- Grain: One row per candidacy stage (candidacy + election stage)
--
-- Each row in the BR S3 data IS a candidacy stage (candidate in a specific race).
with
    source as (
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

    source_with_fields as (
        select
            source.*,
            {{
                generate_candidate_office_from_position(
                    "source.position_name", "source.normalized_position_name"
                )
            }} as candidate_office,
            case
                when source.level = 'local'
                then 'Local'
                when source.level = 'city'
                then 'City'
                when source.level = 'county'
                then 'County'
                when source.level = 'state'
                then 'State'
                when source.level = 'federal'
                then 'Federal'
                when source.level = 'regional'
                then 'Regional'
                when source.level = 'township'
                then 'Township'
                else source.level
            end as office_level,
            {{ extract_city_from_office_name("source.position_name") }} as city,
            case
                when source.position_name like '%- District %'
                then regexp_extract(source.position_name, '- District (.*)$')
                when source.position_name like '% - Ward %'
                then regexp_extract(source.position_name, ' - Ward (.*)$')
                when source.position_name like '% - Place %'
                then regexp_extract(source.position_name, ' - Place (.*)$')
                when source.position_name like '% - Branch %'
                then regexp_extract(source.position_name, ' - Branch (.*)$')
                when source.position_name like '% - Subdistrict %'
                then regexp_extract(source.position_name, ' - Subdistrict (.*)$')
                when source.position_name like '% - Zone %'
                then regexp_extract(source.position_name, ' - Zone (.*)$')
                else ''
            end as district,
            -- Parse party from parties JSON
            case
                when source.parties like '%Independent%'
                then 'Independent'
                when source.parties like '%Nonpartisan%'
                then 'Nonpartisan'
                when source.parties like '%Democrat%'
                then 'Democrat'
                when source.parties like '%Republican%'
                then 'Republican'
                when source.parties like '%Libertarian%'
                then 'Libertarian'
                when source.parties like '%Green%'
                then 'Green'
                else null
            end as party_affiliation,
            br_position.partisan_type
        from source
        left join
            br_position on cast(source.br_position_id as int) = br_position.database_id
    ),

    -- Look up the general election date to generate gp_candidacy_id
    -- (candidacy ID uses the general election date, not stage-specific date)
    general_election_dates as (
        select
            br_candidate_id,
            br_position_id,
            br_election_id,
            max(cast(election_day as date)) as general_election_date
        from source
        where is_primary = 'false' and is_runoff = 'false'
        group by br_candidate_id, br_position_id, br_election_id
    ),

    candidacy_stages as (
        select
            -- gp_candidacy_stage_id = hash(gp_candidacy_id, br_race_id)
            -- First compute gp_candidacy_id inline
            {{
                generate_salted_uuid(
                    fields=[
                        "coalesce(s.first_name, '')",
                        "coalesce(s.last_name, '')",
                        "coalesce(s.state, '')",
                        "coalesce(s.party_affiliation, '')",
                        "coalesce(s.candidate_office, '')",
                        "cast(coalesce(ged.general_election_date, cast(s.election_day as date)) as string)",
                        "coalesce(s.district, '')",
                    ]
                )
            }}
            as computed_gp_candidacy_id,

            {{
                generate_salted_uuid(
                    fields=[
                        generate_salted_uuid(
                            fields=[
                                "coalesce(s.first_name, '')",
                                "coalesce(s.last_name, '')",
                                "coalesce(s.state, '')",
                                "coalesce(s.party_affiliation, '')",
                                "coalesce(s.candidate_office, '')",
                                "cast(coalesce(ged.general_election_date, cast(s.election_day as date)) as string)",
                                "coalesce(s.district, '')",
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
                when s.election_result = ''
                then null
                else null
            end as election_result,

            'ballotready' as election_result_source,
            cast(null as float) as match_confidence,
            cast(null as string) as match_reasoning,
            cast(null as string) as match_top_candidates,
            cast(
                s.election_result is not null and s.election_result != '' as boolean
            ) as has_match,
            cast(null as string) as votes_received,
            cast(s.election_day as date) as election_stage_date,
            s._airbyte_extracted_at as created_at,
            s._airbyte_extracted_at as updated_at

        from source_with_fields as s
        left join
            general_election_dates as ged
            on s.br_candidate_id = ged.br_candidate_id
            and s.br_position_id = ged.br_position_id
            and s.br_election_id = ged.br_election_id
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
        where
            stage.gp_election_stage_id is null
            or stage.gp_election_stage_id
            in (select gp_election_stage_id from valid_election_stages)
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
