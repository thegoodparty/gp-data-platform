{{ config(materialized="table", tags=["techspeed", "tracking"]) }}

-- Tracking table for TechSpeed files that have been successfully processed
-- This replaces the manual tracking in sandbox.uploaded_files for dbt-managed
-- processing
select
    cast(null as string) as source_file_url,
    cast(null as timestamp) as uploaded_at,
    cast(null as date) as processing_date

-- This creates an empty table with the correct schema
-- Records will be inserted via post-hook or separate insert statement
where 1 = 0
