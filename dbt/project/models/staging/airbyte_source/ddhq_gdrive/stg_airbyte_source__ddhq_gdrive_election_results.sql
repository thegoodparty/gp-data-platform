with
    source as (
        select * from {{ source("airbyte_source", "ddhq_gdrive_election_results") }}
    ),

    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(date as date) as election_date,
            votes,
            cast(try_cast(race_id as float) as int) as ddhq_race_id,
            candidate,
            {{ ref("parse_human_name") }} (candidate) as candidate_parsed_name,
            candidate_parsed_name.first as candidate_first_name,
            candidate_parsed_name.middle as candidate_middle_name,
            candidate_parsed_name.last as candidate_last_name,
            candidate_parsed_name.suffix as candidate_suffix,
            candidate_parsed_name.nickname as candidate_nickname,
            {{ cast_to_boolean("is_winner", ["y"], ["n"]) }} as is_winner,
            race_name,
            cast(cast(nullif(candidate_id, '') as float) as int) as candidate_id,
            election_type,
            cast(is_uncontested as boolean) as is_uncontested,
            candidate_party,
            _ab_source_file_url,
            cast(
                try_cast(number_of_seats_in_election as float) as int
            ) as number_of_seats_in_election,
            _ab_source_file_last_modified,
            total_number_of_ballots_in_race
        from source
    ),

    invalid as (
        select _airbyte_raw_id
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results_invalid") }}
    )

select *
from renamed
where
    _airbyte_raw_id
    not in (select _airbyte_raw_id from invalid where _airbyte_raw_id is not null)
