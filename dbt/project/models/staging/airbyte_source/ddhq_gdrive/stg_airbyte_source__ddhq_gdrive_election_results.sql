with
    source as (
        select * from {{ source("airbyte_source", "ddhq_gdrive_election_results") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("votes") }},
            {{ adapter.quote("race_id") }},
            {{ adapter.quote("candidate") }},
            {{ adapter.quote("is_winner") }},
            {{ adapter.quote("race_name") }},
            {{ adapter.quote("candidate_id") }},
            {{ adapter.quote("election_type") }},
            {{ adapter.quote("is_uncontested") }},
            {{ adapter.quote("candidate_party") }},
            {{ adapter.quote("_ab_source_file_url") }},
            {{ adapter.quote("number_of_seats_in_election") }},
            {{ adapter.quote("_ab_source_file_last_modified") }},
            {{ adapter.quote("total_number_of_ballots_in_race") }}

        from source
    )
select *
from renamed
