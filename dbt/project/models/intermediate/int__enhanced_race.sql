{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_race"],
    )
}}

with
    election_frequencies as (
        select tbl_pos.database_id, e.databaseid as pe_frequency_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_pos
        lateral view explode(tbl_pos.election_frequencies) exploded_table as e
    ),
    election_frequency as (
        select
            tbl_pos.database_id as position_database_id,
            last(tbl_freq.frequency) as frequency
        from election_frequencies as tbl_pos
        left join
            {{ ref("int__ballotready_position_election_frequency") }} as tbl_freq
            on tbl_pos.pe_frequency_database_id = tbl_freq.database_id
        where tbl_freq.valid_to is null
        group by position_database_id
    ),
    filing_period_ids as (
        select
            tbl_race.database_id as race_database_id,
            /*
            Take the filing period. Of the 1MM races, only 10k have more than 1 filing period.
            In these cases, the last value is the latest filing period.
            */
            last(fp.databaseid) as filing_period_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_race
        lateral view explode(filing_periods) as fp
        group by race_database_id
    ),
    enhanced_race as (
        select
            {{ generate_salted_uuid(fields=["tbl_race.id"], salt="ballotready") }}
            as id,
            tbl_race.id as br_hash_id,
            tbl_race.database_id as br_database_id,
            tbl_race.is_primary,
            tbl_race.is_runoff,
            tbl_race.created_at,
            tbl_race.updated_at,
            to_timestamp(tbl_election.election_day) as election_date,
            tbl_position.state as `state`,
            tbl_position.level as position_level,
            tbl_normalized_position.name as normalized_position_name,
            tbl_position.name as position_name,
            tbl_position.description as position_description,
            tbl_position.filing_address as filing_office_address,
            tbl_position.filing_phone as filing_phone_number,
            tbl_position.paperwork_instructions,
            tbl_position.filing_requirements,
            tbl_position.partisan_type,
            tbl_position.employment_type,
            tbl_position.eligibility_requirements,
            tbl_position.salary,
            tbl_position.sub_area_name,
            tbl_position.sub_area_value,
            tbl_election_frequency.frequency,
            tbl_filing_period.start_on as filing_date_start,
            tbl_filing_period.end_on as filing_date_end,
            tbl_position.geo_id as position_geo_id,
            -- For some MTFCCs, need to roll up to parent geo_id of the position to
            -- get the correct place by geo_id
            -- see:
            -- https://docs.google.com/spreadsheets/d/1t1U2oECqFRKPUVO7HYI2me-YzDgtvjbPzp5h9BqKHcU/edit?gid=0#gid=0
            case
                when tbl_position.mtfcc = 'X0005'
                then left(tbl_position.geo_id, 5)  -- County Legislative District
                when tbl_position.mtfcc = 'G5220'
                then left(tbl_position.geo_id, 2)  -- State Legislative District (lower)
                when tbl_position.mtfcc = 'X0001'
                then left(tbl_position.geo_id, 7)  -- City Council District
                when tbl_position.mtfcc = 'G5210'
                then left(tbl_position.geo_id, 2)  -- State Legistlative District (upper)
                when tbl_position.mtfcc = 'X0102'
                then left(tbl_position.geo_id, 7)  -- Unified School District Subdistrict
                when tbl_position.mtfcc = 'X0010'
                then left(tbl_position.geo_id, 2)  -- Circuit Court
                when tbl_position.mtfcc = 'G5200'
                then left(tbl_position.geo_id, 2)  -- Congressional District
                when tbl_position.mtfcc = 'X0014'
                then left(tbl_position.geo_id, 2)  -- District Court
                when tbl_position.mtfcc = 'X0027'
                then left(tbl_position.geo_id, 5)  -- Fire District
                else tbl_position.geo_id
            end as position_to_place_geo_id,
            tbl_position.mtfcc as position_mtfcc,
            tbl_race_to_positions.position_names
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_race
        left join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as tbl_election
            on tbl_race.election.databaseid = tbl_election.database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
            on tbl_race.position.databaseid = tbl_position.database_id
        left join
            {{ ref("int__ballotready_normalized_position") }} as tbl_normalized_position
            on tbl_position.normalized_position.`databaseId`
            = tbl_normalized_position.database_id
        left join
            election_frequency as tbl_election_frequency
            on tbl_position.database_id = tbl_election_frequency.position_database_id
        left join
            filing_period_ids as tbl_fp
            on tbl_race.database_id = tbl_fp.race_database_id
        left join
            {{ ref("int__ballotready_filing_period") }} as tbl_filing_period
            on tbl_fp.filing_period_database_id = tbl_filing_period.database_id
        left join
            {{ ref("int__race_to_positions") }} as tbl_race_to_positions
            on tbl_race.database_id = tbl_race_to_positions.race_database_id
        {% if is_incremental() %}
            where tbl_race.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    race_w_place as (
        select
            tbl_race.id,
            tbl_race.br_hash_id,
            tbl_race.br_database_id,
            tbl_race.is_primary,
            tbl_race.is_runoff,
            tbl_race.created_at,
            tbl_race.updated_at,
            tbl_race.election_date,
            tbl_race.`state`,
            tbl_race.position_level,
            tbl_race.normalized_position_name,
            tbl_race.position_name,
            tbl_race.position_description,
            tbl_race.filing_office_address,
            tbl_race.filing_phone_number,
            tbl_race.paperwork_instructions,
            tbl_race.filing_requirements,
            tbl_race.partisan_type,
            tbl_race.employment_type,
            tbl_race.eligibility_requirements,
            tbl_race.salary,
            tbl_race.sub_area_name,
            tbl_race.sub_area_value,
            tbl_race.frequency,
            tbl_race.filing_date_start,
            tbl_race.filing_date_end,
            tbl_race.position_geo_id,
            tbl_race.position_mtfcc,
            tbl_race.position_to_place_geo_id,
            tbl_place.id as place_id,
            tbl_place.name as place_name,
            tbl_place.place_name_slug,
            tbl_race.position_names
        from enhanced_race as tbl_race
        left join
            {{ ref("int__enhanced_place") }} as tbl_place
            on tbl_race.position_to_place_geo_id = tbl_place.geo_id
    )

select
    id,
    br_hash_id,
    br_database_id,
    is_primary,
    is_runoff,
    created_at,
    updated_at,
    election_date,
    `state`,
    position_level,
    normalized_position_name,
    position_name,
    position_description,
    filing_office_address,
    filing_phone_number,
    paperwork_instructions,
    filing_requirements,
    partisan_type,
    employment_type,
    eligibility_requirements,
    salary,
    sub_area_name,
    sub_area_value,
    frequency,
    filing_date_start,
    filing_date_end,
    position_geo_id,
    position_mtfcc,
    position_to_place_geo_id,
    place_id,
    place_name,
    place_name_slug,
    position_names
from race_w_place
