{{ 
    config(
        materialized="table",
        tags=["techspeed", "tracking"]
    ) 
}}

-- Only create table if it doesn't exist
{% set target_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier='int__techspeed_uploaded_files'
) %}

{% if target_relation is none %}
    -- Table doesn't exist, create it with correct schema
    select
        cast(null as string) as source_file_url,
        cast(null as timestamp) as uploaded_at,
        cast(null as date) as processing_date
    where 1 = 0
{% else %}
    -- Table exists, return existing data (no changes)
    select * from {{ target_relation }}
{% endif %}