{{
    config(
        materialized="incremental",
        unique_key=["candidate_id", "candidate", "race_id"],
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["intermediate", "ddhq", "election_results"],
    )
}}

with
    election_results as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            date,
            votes,
            race_id,
            initcap(regexp_replace(trim(candidate), '\s+', ' ')) as candidate,
            is_winner,
            trim(cast(race_name as string)) as race_name,
            upper(substr(race_name, 1, 2)) as extracted_state,
            candidate_id,
            case
                when lower(election_type) like '%primary%'
                then 'primary'
                when lower(election_type) like '%runoff%'
                then 'runoff'
                when lower(election_type) like '%general%'
                then 'general'
                else election_type
            end as election_type,
            is_uncontested,
            case
                when trim(candidate_party) in ('nan', 'None')
                then null
                else trim(candidate_party)
            end as candidate_party,
            _ab_source_file_url,
            number_of_seats_in_election,
            _ab_source_file_last_modified,
            total_number_of_ballots_in_race
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                >= (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    ),
    filtered_election_results as (
        select *
        from election_results
        where
            candidate_id is not null
            and candidate is not null
            and candidate != ''
            and race_name is not null
            and race_name != ''
            and extracted_state in (
                select distinct state_postal_code
                from {{ ref("int__general_states_zip_code_range") }}
            )
    )

select *
from filtered_election_results
