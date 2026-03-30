{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidacy_stage schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates
--
-- Grain: One row per candidacy stage (candidate + election stage)
--
-- Each candidate with both primary and general dates produces TWO rows.
-- Links to int__civics_candidacy_techspeed (gp_candidacy_id) and
-- int__civics_election_stage_techspeed (gp_election_stage_id).
with
    clean_states as (select * from {{ ref("clean_states") }}),

    source as (
        select
            ts.* except (state),
            coalesce(cs.state_cleaned_postal_code, ts.state) as state,
            cast(null as string) as seat_name,
            try_cast(number_of_seats_available as int) as seats_available,
            -- Generate candidate code inline (was provided by _clean)
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code,
            coalesce(
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM-dd-yy')
            ) as primary_election_date_parsed,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM-dd-yy')
            ) as general_election_date_parsed,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM-dd-yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM-dd-yy')
            ) as election_date
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
    ),

    -- Unpivot: primary stage rows
    primary_stages as (
        select
            *,
            'primary' as stage_type,
            primary_election_date_parsed as stage_election_date
        from source
        where
            primary_election_date_parsed is not null
            and year(primary_election_date_parsed) between 1900 and 2030
    ),

    -- Unpivot: general stage rows
    general_stages as (
        select
            *,
            'general' as stage_type,
            general_election_date_parsed as stage_election_date
        from source
        where
            general_election_date_parsed is not null
            and year(general_election_date_parsed) between 1900 and 2030
    ),

    unpivoted as (
        select *
        from primary_stages
        union all
        select *
        from general_stages
    ),

    candidacy_stages as (
        select
            -- gp_candidacy_id: must match int__civics_candidacy_techspeed generation
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "party",
                        "candidate_office",
                        "cast(coalesce(general_election_date_parsed, primary_election_date_parsed) as string)",
                        "district",
                    ]
                )
            }}
            as computed_gp_candidacy_id,

            -- gp_candidacy_stage_id = hash(gp_candidacy_id, gp_election_stage_id)
            {{
                generate_salted_uuid(
                    fields=[
                        generate_salted_uuid(
                            fields=[
                                "first_name",
                                "last_name",
                                "state",
                                "party",
                                "candidate_office",
                                "cast(coalesce(general_election_date_parsed, primary_election_date_parsed) as string)",
                                "district",
                            ]
                        ),
                        generate_salted_uuid(
                            fields=[
                                "'techspeed'",
                                "state",
                                "candidate_office",
                                "official_office_name",
                                "district",
                                "city",
                                "cast(stage_election_date as string)",
                                "stage_type",
                            ]
                        ),
                    ]
                )
            }}
            as gp_candidacy_stage_id,

            -- gp_election_stage_id: must match election_stage model's generation
            {{
                generate_salted_uuid(
                    fields=[
                        "'techspeed'",
                        "state",
                        "candidate_office",
                        "official_office_name",
                        "district",
                        "city",
                        "cast(stage_election_date as string)",
                        "stage_type",
                    ]
                )
            }} as gp_election_stage_id,

            concat(first_name, ' ', last_name) as candidate_name,
            techspeed_candidate_code as source_candidate_id,
            cast(br_race_id as string) as source_race_id,
            party as candidate_party,

            -- Only apply election_result to the general stage. TechSpeed results
            -- are candidate-level (not stage-specific) and refer to the final
            -- outcome, which is the general election. Applying to both stages
            -- would incorrectly mark the primary as Won too.
            case
                when
                    stage_type = 'general'
                    and election_result is not null
                    and trim(election_result) != ''
                then true
                else null
            end as is_winner,

            case
                when
                    stage_type = 'general'
                    and election_result is not null
                    and trim(election_result) != ''
                then 'Won'
                else null
            end as election_result,

            case
                when
                    stage_type = 'general'
                    and election_result is not null
                    and trim(election_result) != ''
                then 'techspeed'
                else null
            end as election_result_source,

            cast(null as float) as match_confidence,
            cast(null as string) as match_reasoning,
            cast(null as string) as match_top_candidates,
            -- No DDHQ matching for TechSpeed — has_match is always false
            false as has_match,
            cast(null as string) as votes_received,
            stage_election_date as election_stage_date,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from unpivoted
        where election_date is not null
    ),

    -- Only include stages with valid candidacy and election_stage references
    valid_candidacies as (
        select gp_candidacy_id from {{ ref("int__civics_candidacy_techspeed") }}
    ),

    valid_election_stages as (
        select gp_election_stage_id
        from {{ ref("int__civics_election_stage_techspeed") }}
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
