-- TechSpeed candidate rows that fail data quality checks.
-- Reads from the raw source (not the staging model) since staging
-- filters these rows out.
--
-- Invalid reasons:
-- null_name_or_state: first_name, last_name, or state is null/empty. The state
-- check strips non-alphabetic characters first, mirroring the staging model's
-- normalization: column-shifted rows arrive with a ZIP code in the state cell,
-- which is non-empty raw but normalizes to '' and so is no state at all.
--
with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    with_checks as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            trim(first_name) as first_name,
            trim(last_name) as last_name,
            trim(state) as state,
            email,
            phone,
            office_name as official_office_name,
            office_normalized as candidate_office,
            office_type,
            _ab_source_file_url,
            case
                when
                    nullif(trim(first_name), '') is null
                    or nullif(trim(last_name), '') is null
                    or nullif(trim(regexp_replace(state, '[^A-Za-z ]', '')), '') is null
                then 'null_name_or_state'
            end as invalid_reason
        from source
    )

select *
from with_checks
where invalid_reason is not null
