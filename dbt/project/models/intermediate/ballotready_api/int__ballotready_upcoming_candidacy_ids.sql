-- Candidacy ids for still-upcoming BallotReady races, read from the API race
-- roster (race.candidacies). The S3 candidacies feed omits many upcoming
-- general-stage rosters that the API race object already carries; that gap is
-- now covered by the extract_ballotready Airflow DAG's own worklist query, not
-- by a dbt model reading this one. This model has no downstream refs: its job
-- is the relationships test in int__ballotready_py.yaml, which checks every id
-- found here against int__ballotready_candidacy. Scoped to upcoming elections
-- to bound that check.
with
    api_race as (
        select
            database_id as race_database_id,
            election.databaseid as election_database_id,
            updated_at as race_updated_at,
            candidacies
        from {{ ref("stg_airbyte_source__ballotready_api_race") }}
    ),
    upcoming_elections as (
        select database_id as election_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_election") }}
        where election_day >= current_date()
    ),
    exploded as (
        select api_race.race_updated_at, candidacy.databaseid as br_candidacy_id
        from api_race
        inner join
            upcoming_elections
            on api_race.election_database_id = upcoming_elections.election_database_id
        lateral view explode(api_race.candidacies) as candidacy
    )
select
    cast(br_candidacy_id as int) as br_candidacy_id,
    max(race_updated_at) as race_updated_at
from exploded
where br_candidacy_id is not null
group by cast(br_candidacy_id as int)
