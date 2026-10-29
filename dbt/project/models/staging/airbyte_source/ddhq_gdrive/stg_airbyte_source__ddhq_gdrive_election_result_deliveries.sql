-- Each DDHQ result row as delivered, with its file's date. Reads the delivery
-- feed alone: the results staging model swaps in the master backfill for older
-- dates, which drops the record of when each file arrived.
with
    source as (
        select * from {{ source("airbyte_source", "ddhq_gdrive_election_results") }}
    ),

    renamed as (
        select
            cast(cast(nullif(candidate_id, '') as float) as int) as candidate_id,
            cast(try_cast(race_id as float) as int) as ddhq_race_id,
            {{ cast_to_boolean("is_winner", ["y"], ["n"]) }} as is_winner,
            _ab_source_file_url as source_file,
            to_date(_ab_source_file_last_modified) as delivered_on
        from source
    )

select *
from renamed
where
    candidate_id is not null
    and ddhq_race_id is not null
    and {{ dsar_not_suppressed("candidate_id", "ddhq_candidate_id") }}
