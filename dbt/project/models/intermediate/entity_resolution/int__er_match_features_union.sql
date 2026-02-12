{{
    config(
        materialized="table",
        tags=["intermediate", "entity_resolution"],
    )
}}

-- Entity Resolution Step 1: Canonicalized union of Techspeed and BallotReady
-- candidacy records.
-- All normalization happens here so downstream models compare clean values.
with
    -- Techspeed records from the clean model
    techspeed as (
        select
            techspeed_candidate_code as record_id,
            'techspeed' as source_system,

            -- Name cleaning
            lower(trim(first_name)) as first_name_clean,
            lower(trim(last_name)) as last_name_clean,
            lower(trim(concat_ws(' ', first_name, last_name))) as full_name_clean,
            first_name as first_name_raw,
            last_name as last_name_raw,

            -- State normalization (TS has full state names)
            {{ normalize_state_to_abbr("state") }} as state_abbr,

            -- Contact info
            lower(trim(email)) as email_clean,
            trim(regexp_replace(phone, '[^0-9]', '')) as phone_clean,

            -- Office standardization (already cleaned in source)
            lower(trim(candidate_office)) as candidate_office_clean,
            lower(trim(office_type)) as office_type_clean,
            case
                when lower(trim(office_level)) in ('local', 'city', 'county', 'state')
                then initcap(lower(trim(office_level)))
                else lower(trim(office_level))
            end as office_level_clean,
            lower(trim(city)) as city_clean,

            -- District cleaning
            lower(trim(district)) as district_clean,

            -- Election dates
            coalesce(
                try_cast(general_election_date as date),
                try_to_date(general_election_date, 'MM/dd/yyyy'),
                try_to_date(general_election_date, 'MM-dd-yyyy'),
                try_to_date(general_election_date, 'MM/dd/yy'),
                try_cast(primary_election_date as date),
                try_to_date(primary_election_date, 'MM/dd/yyyy'),
                try_to_date(primary_election_date, 'MM-dd-yyyy'),
                try_to_date(primary_election_date, 'MM/dd/yy')
            ) as election_date,

            -- Party
            lower(trim(party)) as party_clean,

            -- Cross-reference IDs
            cast(ballotready_race_id as string) as br_race_id,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as br_candidate_id,
            cast(null as int) as br_position_id,

            -- Candidate code (already generated in source)
            techspeed_candidate_code as candidate_code,

            -- Raw fields for embedding text
            official_office_name,

            -- Source timestamp
            _airbyte_extracted_at as _source_extracted_at

        from {{ ref("int__techspeed_candidates_clean") }}
        where
            techspeed_candidate_code is not null
            -- DEV FILTER: limit to single state for testing
            and upper(trim(state)) = 'OHIO'
    ),

    techspeed_with_year as (
        select *, year(election_date) as election_year from techspeed
    ),

    -- BallotReady records from staging (full scope, no filters)
    ballotready_raw as (
        select
            cast(candidacy_id as string) as record_id,
            'ballotready' as source_system,

            -- Name cleaning
            lower(trim(first_name)) as first_name_clean,
            lower(trim(last_name)) as last_name_clean,
            lower(trim(concat_ws(' ', first_name, last_name))) as full_name_clean,
            first_name as first_name_raw,
            last_name as last_name_raw,

            -- State normalization (BR staging has 2-letter abbreviations)
            {{ normalize_state_to_abbr("state") }} as state_abbr,

            -- Contact info
            lower(trim(email)) as email_clean,
            trim(regexp_replace(phone, '[^0-9]', '')) as phone_clean,

            -- Office standardization (apply same macros as
            -- int__ballotready_clean_candidacies)
            lower(
                trim(
                    {{
                        generate_candidate_office_from_position(
                            "position_name", "normalized_position_name"
                        )
                    }}
                )
            ) as candidate_office_clean,
            lower(
                trim(
                    {{
                        map_ballotready_office_type(
                            generate_candidate_office_from_position(
                                "position_name", "normalized_position_name"
                            )
                        )
                    }}
                )
            ) as office_type_clean,
            case
                when lower(trim(level)) in ('local', 'city', 'county', 'state')
                then initcap(lower(trim(level)))
                else lower(trim(level))
            end as office_level_clean,

            -- City extraction from position_name
            lower(
                trim({{ extract_city_from_office_name("position_name") }})
            ) as city_clean,

            -- District extraction (same logic as int__ballotready_clean_candidacies)
            lower(
                trim(
                    case
                        when position_name like '%- District %'
                        then regexp_extract(position_name, '- District (.*)$')
                        when position_name like '% - Ward %'
                        then regexp_extract(position_name, ' - Ward (.*)$')
                        when position_name like '% - Place %'
                        then regexp_extract(position_name, ' - Place (.*)$')
                        when position_name like '% - Branch %'
                        then regexp_extract(position_name, ' - Branch (.*)$')
                        when position_name like '% - Subdistrict %'
                        then regexp_extract(position_name, ' - Subdistrict (.*)$')
                        when position_name like '% - Zone %'
                        then regexp_extract(position_name, ' - Zone (.*)$')
                        when sub_area_name is not null and sub_area_value is not null
                        then sub_area_value
                        else ''
                    end
                )
            ) as district_clean,

            -- Election date
            try_cast(election_day as date) as election_date,

            -- Party extraction (same logic as int__ballotready_clean_candidacies)
            lower(
                trim(
                    case
                        when parties like '%Independent%'
                        then 'independent'
                        when parties like '%Nonpartisan%'
                        then 'nonpartisan'
                        else ''
                    end
                )
            ) as party_clean,

            -- BallotReady native IDs
            cast(race_id as string) as br_race_id,
            cast(candidacy_id as string) as br_candidacy_id,
            cast(candidate_id as string) as br_candidate_id,
            cast(position_id as int) as br_position_id,

            -- Raw fields for embedding text
            position_name as official_office_name,

            -- For candidate code generation (keep raw columns for downstream CTE)
            first_name as _br_first_name,
            last_name as _br_last_name,
            state as _br_state,
            position_name as _br_position_name,
            normalized_position_name as _br_normalized_position_name,

            -- Source timestamp
            _airbyte_extracted_at as _source_extracted_at

        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            candidacy_id is not null
            and first_name is not null
            and last_name is not null
            -- DEV FILTER: limit to single state for testing
            and upper(trim(state)) = 'OH'
    ),

    ballotready as (
        select
            record_id,
            source_system,
            first_name_clean,
            last_name_clean,
            full_name_clean,
            first_name_raw,
            last_name_raw,
            state_abbr,
            email_clean,
            phone_clean,
            candidate_office_clean,
            office_type_clean,
            office_level_clean,
            city_clean,
            district_clean,
            election_date,
            year(election_date) as election_year,
            party_clean,
            br_race_id,
            br_candidacy_id,
            br_candidate_id,
            br_position_id,
            -- Generate candidate_code using same macro as TS
            {{
                generate_candidate_code(
                    "_br_first_name",
                    "_br_last_name",
                    "_br_state",
                    map_ballotready_office_type(
                        generate_candidate_office_from_position(
                            "_br_position_name", "_br_normalized_position_name"
                        )
                    ),
                    extract_city_from_office_name("_br_position_name"),
                )
            }} as candidate_code,
            official_office_name,
            _source_extracted_at
        from ballotready_raw
    ),

    unioned as (
        select
            record_id,
            source_system,
            first_name_clean,
            last_name_clean,
            full_name_clean,
            first_name_raw,
            last_name_raw,
            state_abbr,
            email_clean,
            phone_clean,
            candidate_office_clean,
            office_type_clean,
            office_level_clean,
            city_clean,
            district_clean,
            election_date,
            election_year,
            party_clean,
            br_race_id,
            br_candidacy_id,
            br_candidate_id,
            br_position_id,
            candidate_code,
            official_office_name,
            _source_extracted_at
        from techspeed_with_year

        union all

        select
            record_id,
            source_system,
            first_name_clean,
            last_name_clean,
            full_name_clean,
            first_name_raw,
            last_name_raw,
            state_abbr,
            email_clean,
            phone_clean,
            candidate_office_clean,
            office_type_clean,
            office_level_clean,
            city_clean,
            district_clean,
            election_date,
            election_year,
            party_clean,
            br_race_id,
            br_candidacy_id,
            br_candidate_id,
            br_position_id,
            candidate_code,
            official_office_name,
            _source_extracted_at
        from ballotready
    )

select *
from unioned
