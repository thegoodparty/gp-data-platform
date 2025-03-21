{{
    config(
        materialized="table",
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
        select tbl_pos.database_id as position_database_id, tbl_freq.frequency
        from election_frequencies as tbl_pos
        left join
            {{ ref("int__ballotready_position_election_frequency") }} as tbl_freq
            on tbl_pos.pe_frequency_database_id = tbl_freq.database_id
        where tbl_freq.valid_to is null
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
            tbl_position.slug as position_slug,
            concat(tbl_position.state, '-', tbl_position.slug) as state_slug,
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
            tbl_filing_period.end_on as filing_date_end
        -- need to add field `place` (how to represent object?)
        -- need to add int `place_id` (database_id?)
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
        {% if is_incremental() %}
            where tbl_race.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
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
    position_slug,
    state_slug,
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
    filing_date_end
-- need to add field `place` (how to represent object?)
-- need to add int `place_id` (database_id?)
from enhanced_race
