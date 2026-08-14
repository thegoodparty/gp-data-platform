-- Candidacy ids for still-upcoming BallotReady races, read from the API race
-- roster (race.candidacies). The S3 candidacies feed omits many upcoming
-- general-stage rosters that the API race object already carries, so the
-- candidacy and party fetches seed their worklist from this source in addition
-- to S3. Without it, those candidacies are never fetched and never reach
-- election-api, so a candidate's competitive landscape falls back to the
-- primary field. Scoped to upcoming elections to keep the API fetch bounded.
with
    api_race as (
        select
            database_id as race_database_id,
            election.databaseid as election_database_id,
            updated_at as race_updated_at,
            _airbyte_extracted_at as race_extracted_at,
            candidacies
        from {{ ref("stg_airbyte_source__ballotready_api_race") }}
    ),
    upcoming_elections as (
        select database_id as election_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_election") }}
        where election_day >= current_date()
    ),
    exploded as (
        select
            api_race.race_updated_at,
            api_race.race_extracted_at,
            candidacy.databaseid as br_candidacy_id
        from api_race
        inner join
            upcoming_elections
            on api_race.election_database_id = upcoming_elections.election_database_id
        lateral view explode(api_race.candidacies) as candidacy
    )
select
    cast(br_candidacy_id as int) as br_candidacy_id,
    max(race_updated_at) as race_updated_at,
    max(race_extracted_at) as race_extracted_at
from exploded
where br_candidacy_id is not null
group by cast(br_candidacy_id as int)
