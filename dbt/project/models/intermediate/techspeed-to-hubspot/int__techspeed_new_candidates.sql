{{
    config(
        materialized="incremental",
        unique_key="candidate_id_source",
        on_schema_change="sync_all_columns",
        tags=["techspeed", "incremental"],
    )
}}

with
    techspeed_raw as (
        select *
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
        {% if is_incremental() %}
            -- Only process files that haven't been uploaded yet
            where
                _ab_source_file_url not in (
                    select source_file_url
                    from {{ ref("int__techspeed_uploaded_files") }}
                )
        {% endif %}
    ),

    -- Identify which records are new vs previously processed
    new_record_identification as (
        select
            *,
            case
                when
                    _ab_source_file_url in (
                        select source_file_url
                        from {{ ref("int__techspeed_uploaded_files") }}
                    )
                then 'old'
                else 'new'
            end as new_records
        from techspeed_raw
    ),

    -- Filter to only new records for downstream processing
    new_candidates_only as (
        select * from new_record_identification where new_records = 'new'
    )

select *
from new_candidates_only
