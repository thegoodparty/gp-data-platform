-- DDHQ election results rows that fail data quality checks.
-- Captures rows excluded from downstream models for investigation.
--
-- Invalid reasons:
-- null_candidate_id: candidate_id could not be cast to int (empty or non-numeric)
-- unresolvable_state: race_name does not start with a recognized state name
--
with
    stg as (
        select * from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    with_checks as (
        select
            stg.*,
            coalesce(
                cs_two.state_cleaned_postal_code, cs_one.state_cleaned_postal_code
            ) as resolved_state,
            case
                when stg.candidate_id is null
                then 'null_candidate_id'
                when
                    coalesce(
                        cs_two.state_cleaned_postal_code,
                        cs_one.state_cleaned_postal_code
                    )
                    is null
                then 'unresolvable_state'
            end as invalid_reason
        from stg
        left join
            clean_states as cs_one
            on upper(split(stg.race_name, ' ')[0]) = upper(trim(cs_one.state_raw))
        left join
            clean_states as cs_two
            on upper(
                concat(split(stg.race_name, ' ')[0], ' ', split(stg.race_name, ' ')[1])
            )
            = upper(trim(cs_two.state_raw))
    )

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    date,
    race_id,
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
