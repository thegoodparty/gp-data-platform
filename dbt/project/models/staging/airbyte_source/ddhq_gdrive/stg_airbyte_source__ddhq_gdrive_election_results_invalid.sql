-- DDHQ election results rows that fail data quality checks.
-- Reads from the raw source (not the staging model) since staging
-- filters these rows out.
--
-- Invalid reasons:
-- null_candidate_id: candidate_id could not be cast to int (empty or non-numeric)
-- unresolvable_state: race_name does not start with a recognized state name
--
with
    source as (
        select * from {{ source("airbyte_source", "ddhq_gdrive_election_results") }}
    ),

    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            cast(date as date) as election_date,
            cast(try_cast(race_id as float) as int) as ddhq_race_id,
            candidate,
            is_winner,
            race_name,
            cast(cast(nullif(candidate_id, '') as float) as int) as candidate_id,
            election_type,
            candidate_party,
            _ab_source_file_url
        from source
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    with_checks as (
        select
            r.*,
            coalesce(
                cs_two.state_cleaned_postal_code, cs_one.state_cleaned_postal_code
            ) as resolved_state,
            case
                when r.candidate_id is null
                then 'null_candidate_id'
                when
                    coalesce(
                        cs_two.state_cleaned_postal_code,
                        cs_one.state_cleaned_postal_code
                    )
                    is null
                then 'unresolvable_state'
            end as invalid_reason
        from renamed as r
        left join
            clean_states as cs_one
            on upper(split(r.race_name, ' ')[0]) = upper(trim(cs_one.state_raw))
        left join
            clean_states as cs_two
            on upper(
                concat(split(r.race_name, ' ')[0], ' ', split(r.race_name, ' ')[1])
            )
            = upper(trim(cs_two.state_raw))
    )

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    election_date,
    ddhq_race_id,
    candidate_id,
    candidate,
    race_name,
    election_type,
    candidate_party,
    is_winner,
    invalid_reason,
    _ab_source_file_url
from with_checks
where invalid_reason is not null
