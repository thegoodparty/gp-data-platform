-- TechSpeed candidate rows that fail data quality checks.
-- Reads from the raw source (not the staging model) since staging
-- filters these rows out.
--
-- Invalid reasons:
-- null_name_or_state: first_name, last_name, or state is null/empty. The state
-- check strips non-alphabetic characters first, mirroring the staging model's
-- normalization: column-shifted rows arrive with a ZIP code in the state cell,
-- which is non-empty raw but normalizes to '' and so is no state at all.
-- unrecognized_state: the state, after the clean_states mapping staging applies,
-- is not a postal code the candidate mart's is_state_abbreviation test accepts.
-- A delivery with an unbalanced quote folds the rows after it into one and a
-- field list lands in the state cell as text; staging would pass that text
-- through as the postal code and the mart test would fail the whole build.
--
{% set allowed_states = get_us_states_list(include_US=true, include_territories=true) %}

with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    normalized as (
        select *, trim(regexp_replace(state, '[^A-Za-z ]', '')) as state_normalized
        from source
    ),

    -- Same mapping staging applies, so the check sees the code the mart would.
    with_postal_code as (
        select
            src.*,
            coalesce(
                cs.state_cleaned_postal_code, src.state_normalized
            ) as state_postal_code
        from normalized as src
        left join
            clean_states as cs
            on upper(src.state_normalized) = upper(trim(cs.state_raw))
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
                    or nullif(state_normalized, '') is null
                then 'null_name_or_state'
                when
                    state_postal_code not in (
                        {%- for allowed_state in allowed_states -%}
                            '{{ allowed_state }}'{% if not loop.last %}, {% endif %}
                        {%- endfor -%}
                    )
                then 'unrecognized_state'
            end as invalid_reason
        from with_postal_code
    )

select *
from with_checks
where
    invalid_reason is not null
    -- Rejected rows are still materialized and queryable, so they filter like the
    -- valid ones do.
    and {{ dsar_not_suppressed("email", "email") }}
    and {{ dsar_not_suppressed("phone", "phone") }}
