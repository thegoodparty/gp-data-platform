{{ config(materialized="view") }}

/*
schema to build:
model Race {
  id                      String        @id @map("id") @db.Uuid
  brHashId                String?       @map("br_hash_id")
  brDatabaseId            Int?          @map("br_database_id")
  electionDate            DateTime      @map("election_date")
  electionDay             String        @map("election_day")
  positionSlug            String        @map("position_slug")
  stateSlug               String?       @map("state_slug") // state-positionSlug
  state                   String?       @db.Char(2)
  // Position
  positionLevel           PositionLevel @map("position_level")
  normalizedPositionName  String?       @map("normalized_position_name")
  positionName            String?       @map("position_name")
  positionDescription     String?       @map("position_description")
  filingOfficeAddress     String?       @map("filing_office_address")
  filingPhoneNumber       String?       @map("filing_phone_number")
  paperworkInstructions   String?       @map("paperwork_instructions")
  filingRequirements      String?       @map("filing_requirements")
  isRunoff                Boolean?      @map("is_runoff")
  isPrimary               Boolean?      @map("is_primary")
  partisanType            String?       @map("partisan_type")
  locationName            String?       @map("location_name")
  filingDateStart         String?       @map("filing_date_start")
  filingDateEnd           String?       @map("filing_date_end")
  employmentType          String?       @map("employment_type")
  eligibilityRequirements String?       @map("eligibility_requirements")
  salary                  String?       @map("salary")
  // PositionElection
  frequency               Int[]         @map("frequency")
  county                  County?       @relation(fields: [countyId], references: [id])
  municipality            Municipality? @relation(fields: [municipalityId], references: [id])
  candidacies             Candidacy[]
  countyId                String?       @db.Uuid
  municipalityId          String?       @db.Uuid
}
*/
with
    base_race as (
        select
            uuid(md5(concat("ballotready", "-", tbl_race.id))) as guid,
            tbl_race.id as br_hash_id,
            tbl_race.database_id as br_database_id,
            tbl_election.election_day as election_date,
            tbl_position.slug as position_slug,
            concat(tbl_position.state, "-", tbl_position.slug) as state_slug,
            tbl_position.state as state,
            tbl_position.level as position_level
            -- need to get normalized position
            tbl_normalized_position.name as normalized_position_name,
            tbl_position.name as position_name,
            tbl_position.description as position_description,
            tbl_position.filing_address as filing_office_address,
            tbl_position.filing_phone as filing_phone_number,
            tbl_position.paperwork_instructions as paperwork_instructions,
            tbl_position.filing_requirements as filing_requirements,

        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_race
        left join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as tbl_election
            on tbl_race.election_id = tbl_election.id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
            on tbl_race.position_id = tbl_position.id
        left join
            {{ ref("int__ballotready_normalized_position") }} as tbl_normalized_position
            on tbl_position.normalized_position.`databaseid`
            = tbl_normalized_position.database_id
    )

select *
from base_race
