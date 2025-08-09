{{
    config(
        materialized="incremental",
        unique_key="candidate_id_source",
        on_schema_change="sync_all_columns",
        tags=["techspeed", "incremental"]
    )
}}

with
    techspeed_raw as (
        select *
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    {% if is_incremental() %}
    -- Get processed files from tracking table (only on incremental runs)
    processed_files as (
        select source_file_url
        from {{ ref("int__techspeed_uploaded_files") }}
    ),
    {% endif %}

    -- Identify new vs previously processed records
    new_record_identification as (
        select
            tr.*,
            {% if is_incremental() %}
            case
                when pf.source_file_url is not null then 'old'
                else 'new'
            end as new_records
            {% else %}
            'new' as new_records  -- All records are new on first run
            {% endif %}
        from techspeed_raw tr
        {% if is_incremental() %}
        left join processed_files pf 
            on tr._ab_source_file_url = pf.source_file_url
        {% endif %}
    ),

    -- Filter to only new records for downstream processing
    new_candidates_only as (
        select * 
        from new_record_identification 
        where new_records = 'new'
    )

select *
from new_candidates_only