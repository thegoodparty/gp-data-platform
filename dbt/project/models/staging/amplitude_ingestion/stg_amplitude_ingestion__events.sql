-- Reads the Lakeflow Connect export in the raw catalog, keeping the columns and
-- types of the Airbyte stream it replaced so downstream reads are unchanged.
with
    source as (select * from {{ source("amplitude_ingestion", "events") }}),
    renamed as (
        select
            app,
            dma,
            -- Always empty in the Airbyte stream and absent from the new export.
            cast(null as string) as adid,
            city,
            {{ amplitude_properties_to_json("data") }} as data,
            cast(null as string) as idfa,
            -- Airbyte always delivered an empty object here.
            '{}' as plan,
            uuid,
            {{ amplitude_properties_to_json("groups") }} as groups,
            paying,
            region,
            cast(null as bigint) as _schema,
            country,
            library,
            os_name,
            user_id,
            event_id,
            language,
            platform,
            -- Every row of the Airbyte stream was an event.
            'event' as data_type,
            device_id,
            cast(null as string) as source_id,
            `$insert_id` as _insert_id,
            event_time,
            event_type,
            ip_address,
            os_version,
            cast(null as string) as partner_id,
            cast(session_id as decimal(38, 10)) as session_id,
            cast(null as string) as _insert_key,
            device_type,
            sample_rate,
            amplitude_id,
            cast(null as string) as device_brand,
            cast(null as string) as device_model,
            cast(location_lat as decimal(38, 10)) as location_lat,
            cast(location_lng as decimal(38, 10)) as location_lng,
            version_name,
            device_family,
            start_version,
            device_carrier,
            processed_time,
            {{ amplitude_properties_to_json("user_properties") }} as user_properties,
            {{ amplitude_properties_to_json("event_properties") }} as event_properties,
            {{ amplitude_properties_to_json("group_properties") }} as group_properties,
            client_event_time,
            client_upload_time,
            server_upload_time,
            cast(null as timestamp) as user_creation_time,
            cast(null as string) as device_manufacturer,
            cast(null as string) as amplitude_event_type,
            cast(null as boolean) as is_attribution_event,
            server_received_time,
            cast(null as string) as global_user_properties,
            amplitude_attribution_ids

        from source
    )
select *
from renamed
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
