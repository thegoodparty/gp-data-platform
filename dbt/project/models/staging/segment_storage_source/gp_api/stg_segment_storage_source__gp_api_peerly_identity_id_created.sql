with
    source as (
        select *
        from {{ source("segment_storage_source", "peerly_identity_id_created") }}
    ),

    renamed as (
        select
            -- identifiers
            id,
            campaign_id,
            user_id,
            peerly_identity_id,
            company_hubspot_id,
            contact_hubspot_id,
            context_traits_hubspot_id,

            -- event
            event,
            event_text,

            -- traits
            email,
            context_traits_email,

            -- segment context
            context_library_name,
            context_library_version,

            -- timestamps: segment lands original_timestamp as a string; the rest
            -- already arrive typed, so this is the only cast needed for proper typing
            timestamp,
            cast(original_timestamp as timestamp) as original_timestamp,
            sent_at,
            received_at,
            received_date,
            uuid_ts

        from source
    )

select *
from renamed
