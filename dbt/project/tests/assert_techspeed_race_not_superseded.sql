-- TechSpeed copies a BallotReady race id into each delivery, and BallotReady
-- sometimes deletes a race and re-creates it. Every consumer reads the deduped
-- race staging, so a TechSpeed row left on the deleted copy silently loses its
-- position, election and public race. Staging remaps these to the kept race.
select ts.br_race_id, count(*) as techspeed_rows
from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
inner join
    {{ ref("stg_airbyte_source__ballotready_api_race_with_superseded") }} as race
    on ts.br_race_id = cast(race.database_id as string)
where race.is_superseded
group by ts.br_race_id
