{{ 
    config(
        materialized="incremental",
        unique_key="source_file_url",
        tags=["techspeed", "tracking"]
    ) 
}}

-- Tracking table for TechSpeed files that have been successfully processed
-- Creates table on first run, then does nothing (data added via post-hooks elsewhere)

{% if not is_incremental() %}
    -- First run only: create empty table with correct schema
    select
        cast(null as string) as source_file_url,
        cast(null as timestamp) as uploaded_at,
        cast(null as date) as processing_date
    where 1 = 0
{% else %}
    -- Subsequent runs: do nothing, table exists and data is managed elsewhere
    select
        cast(null as string) as source_file_url,
        cast(null as timestamp) as uploaded_at,
        cast(null as date) as processing_date
    where 1 = 0
{% endif %}
