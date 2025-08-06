with
    source as (
        select *
        from
            {{ source("airbyte_source", "techspeed_gdrive_marketing_data_enrichment") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("city") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("gp_ts") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("office") }},
            {{ adapter.quote("open_seat") }},
            {{ adapter.quote("opponents") }},
            {{ adapter.quote("record_ID") }},
            {{ adapter.quote("ts_status") }},
            {{ adapter.quote("ts_comment") }},
            {{ adapter.quote("office_type") }},
            {{ adapter.quote("is_incumbent") }},
            {{ adapter.quote("seats_available") }},
            {{ adapter.quote("_ab_source_file_url") }},
            {{ adapter.quote("comment_related_url") }},
            {{ adapter.quote("partisan_nonpartisan") }},
            {{ adapter.quote("_ab_source_file_last_modified") }}

        from source
    )
select *
from renamed
