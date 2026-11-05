-- Races BallotReady still publishes. A superseded copy is excluded here so every
-- consumer sees one race per stage; read the _with_superseded model only where an
-- id that already left BallotReady must still resolve.
select * except (is_superseded, superseded_by_database_id)
from {{ ref("stg_airbyte_source__ballotready_api_race_with_superseded") }}
where not is_superseded
