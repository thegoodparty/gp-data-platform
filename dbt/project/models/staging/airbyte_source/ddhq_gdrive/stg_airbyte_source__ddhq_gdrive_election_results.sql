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
            cast({{ adapter.quote("date") }} as date) as date,
            {{ adapter.quote("votes") }},
            cast(try_cast({{ adapter.quote("race_id") }} as float) as int) as race_id,
            {{ adapter.quote("candidate") }},
            {{ adapter.quote("is_winner") }},
            {{ adapter.quote("race_name") }},
            cast(
                try_cast({{ adapter.quote("candidate_id") }} as float) as int
            ) as candidate_id,
            {{ adapter.quote("election_type") }},
            cast({{ adapter.quote("is_uncontested") }} as boolean) as is_uncontested,
            {{ adapter.quote("candidate_party") }},
            {{ adapter.quote("_ab_source_file_url") }},
            cast(
                try_cast(
                    {{ adapter.quote("number_of_seats_in_election") }} as float
                ) as int
            ) as number_of_seats_in_election,
            {{ adapter.quote("_ab_source_file_last_modified") }},
            {{ adapter.quote("total_number_of_ballots_in_race") }}
        from source
    )
select *
from renamed
