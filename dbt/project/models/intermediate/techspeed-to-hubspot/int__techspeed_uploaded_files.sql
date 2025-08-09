-- Simple tracking table for TechSpeed files
select
    'placeholder' as source_file_url,
    current_timestamp() as uploaded_at,
    current_date() as processing_date
where 1 = 0