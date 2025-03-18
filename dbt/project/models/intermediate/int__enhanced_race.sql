{{ config(materialized="view") }}

/*
schema to build:
model Race {
  electionDate            DateTime      @map("election_date")
  electionDay             String        @map("election_day")
  state                   String?       @db.Char(2)
  // Position
  locationName            String?       @map("location_name")
  filingDateStart         String?       @map("filing_date_start")
  filingDateEnd           String?       @map("filing_date_end")
  salary                  String?       @map("salary")
  // PositionElection
  frequency               Int[]         @map("frequency")
  county                  County?       @relation(fields: [countyId], references: [id])
  # look at Place, geoId, etc
  municipality            Municipality? @relation(fields: [municipalityId], references: [id])
  candidacies             Candidacy[]
  countyId                String?       @db.Uuid
  municipalityId          String?       @db.Uuid
}
*/
with
    enhnced_race as (
        select
            uuid(md5(concat("ballotready", "-", tbl_race.id))) as id,
            tbl_race.id as br_hash_id,
            tbl_race.database_id as br_database_id,
            tbl_race.is_primary,
            tbl_race.is_runoff,
            tbl_election.election_day as election_date,
            tbl_position.slug as position_slug,
            concat(tbl_position.state, "-", tbl_position.slug) as state_slug,
            tbl_position.state as `state`,
            tbl_position.level as position_level,
            tbl_normalized_position.name as normalized_position_name,
            tbl_position.name as position_name,
            tbl_position.description as position_description,
            tbl_position.filing_address as filing_office_address,
            tbl_position.filing_phone as filing_phone_number,
            tbl_position.paperwork_instructions,
            tbl_position.filing_requirementsm,
            tbl_position.partisan_type,
            tbl_position.employment_type,
            tbl_position.eligibility_requirements,
            tbl_position.salary

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
    )

select *
from enhanced_race
