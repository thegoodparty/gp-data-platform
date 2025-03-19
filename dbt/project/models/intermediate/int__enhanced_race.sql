{{ config(materialized="view") }}

/*
schema to build:
model Race {
  electionDate            DateTime      @map("election_date")
  electionDay             String        @map("election_day")
  // Position
  locationName            String?       @map("location_name")
  filingDateStart         String?       @map("filing_date_start")
  filingDateEnd           String?       @map("filing_date_end")
  // PositionElection
  Place                   Place?        @relation(fields: [placeId], references: [id])
  placeId                 String?       @db.Uuid
}
*/
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
        where tbl_freq.valid_on is null
    ),
    enhanced_race as (
        select
            uuid(md5(concat("ballotready", "-", tbl_race.id))) as id,
            tbl_race.id as br_hash_id,
            tbl_race.database_id as br_database_id,
            tbl_race.is_primary,
            tbl_race.is_runoff,
            tbl_race.created_at,
            tbl_race.updated_at,
            to_timestamp(tbl_election.election_day) as election_date,
            tbl_position.slug as position_slug,
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
            tbl_election_frequency.frequency
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_race
        left join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as tbl_election
            on tbl_race.election_id = tbl_election.id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
            on tbl_race.position_id = tbl_position.id
        left join
            {{ ref("int__ballotready_normalized_position") }} as tbl_normalized_position
            on tbl_position.normalized_position.`databaseId`
            = tbl_normalized_position.database_id
        left join
            {{ ref("int__ballotready_position_election_frequency") }} as tbl_frequency
            on tbl_position.election_frequencies[0].databaseid
            = tbl_frequency.database_id
        left join
            election_frequency as tbl_election_frequency
            on tbl_position.database_id = tbl_election_frequency.position_database_id
    )

select *
from enhanced_race
