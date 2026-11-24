-- Frozen Airbyte calls plus the Lakeflow calls table. The frozen feed wins on
-- id: the Lakeflow activity tables carry no associations, and
-- int__user_hubspot_touches drops unassociated activities.
{% set lakeflow_calls %}
    select
        c.hs_object_id as id,
        false as archived,
        hs_gdpr_deleted as is_gdpr_deleted,
        hs_createdate as created_at,
        hs_lastmodifieddate as updated_at,
        -- No Lakeflow column carries an association.
        cast(null as string) as contacts,
        cast(null as string) as companies,
        cast(null as string) as deals,
        cast(null as string) as tickets,
        hs_timestamp,
        -- Lakeflow uses '' where Airbyte used NULL; the decimal casts keep the
        -- frozen side's types.
        nullif(hs_call_direction, '') as hs_call_direction,
        nullif(hs_call_status, '') as hs_call_status,
        nullif(hs_call_disposition, '') as hs_call_disposition,
        try_cast(nullif(hs_call_duration, '') as decimal(38, 10))
            as hs_call_duration,
        try_cast(
            nullif(hs_call_callee_object_id, '') as decimal(38, 10)
        ) as hs_call_callee_object_id,
        nullif(hs_call_callee_object_type, '') as hs_call_callee_object_type,
        nullif(hs_call_from_number, '') as hs_call_from_number,
        nullif(hs_call_to_number, '') as hs_call_to_number,
        nullif(hs_call_has_voicemail, '') as hs_call_has_voicemail,
        nullif(hs_call_recording_url, '') as hs_call_recording_url,
        nullif(hs_call_body, '') as hs_call_body,
        nullif(hs_call_summary, '') as hs_call_summary,
        nullif(hs_call_title, '') as hs_call_title,
        nullif(hs_call_source, '') as hs_call_source,
        nullif(hs_activity_type, '') as hs_activity_type,
        nullif(hubspot_owner_id, '') as hubspot_owner_id,
        -- Lakeflow has no loader timestamp column.
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "calls") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id
        and lt.activity_type = 'CALL'
{% endset %}

with
    frozen as (
        select
            cast(id as string) as id,
            archived,
            properties_hs_gdpr_deleted as is_gdpr_deleted,
            createdat as created_at,
            updatedat as updated_at,
            contacts,
            companies,
            deals,
            tickets,
            properties_hs_timestamp as hs_timestamp,
            properties_hs_call_direction as hs_call_direction,
            properties_hs_call_status as hs_call_status,
            properties_hs_call_disposition as hs_call_disposition,
            properties_hs_call_duration as hs_call_duration,
            try_cast(
                properties_hs_call_callee_object_id as decimal(38, 10)
            ) as hs_call_callee_object_id,
            properties_hs_call_callee_object_type as hs_call_callee_object_type,
            properties_hs_call_from_number as hs_call_from_number,
            properties_hs_call_to_number as hs_call_to_number,
            properties_hs_call_has_voicemail as hs_call_has_voicemail,
            properties_hs_call_recording_url as hs_call_recording_url,
            properties_hs_call_body as hs_call_body,
            properties_hs_call_summary as hs_call_summary,
            properties_hs_call_title as hs_call_title,
            properties_hs_call_source as hs_call_source,
            properties_hs_activity_type as hs_activity_type,
            properties_hubspot_owner_id as hubspot_owner_id,
            _airbyte_extracted_at
        from {{ source("airbyte_source", "hubspot_api_engagements_calls") }}
    ),

    -- One row per call id either feed holds. Association columns come from the
    -- frozen row outright; the rest refresh from Lakeflow when it has a value.
    joined as (
        select
            coalesce(f.id, l.id) as id,
            coalesce(f.archived, l.archived) as archived,
            -- True if either feed says so, so an erasure is never undone.
            greatest(f.is_gdpr_deleted, l.is_gdpr_deleted) as is_gdpr_deleted,
            coalesce(f.created_at, l.created_at) as created_at,
            coalesce(l.updated_at, f.updated_at) as updated_at,
            f.contacts,
            f.companies,
            f.deals,
            f.tickets,
            coalesce(l.hs_timestamp, f.hs_timestamp) as hs_timestamp,
            coalesce(l.hs_call_direction, f.hs_call_direction) as hs_call_direction,
            coalesce(l.hs_call_status, f.hs_call_status) as hs_call_status,
            coalesce(
                l.hs_call_disposition, f.hs_call_disposition
            ) as hs_call_disposition,
            coalesce(l.hs_call_duration, f.hs_call_duration) as hs_call_duration,
            coalesce(
                l.hs_call_callee_object_id, f.hs_call_callee_object_id
            ) as hs_call_callee_object_id,
            coalesce(
                l.hs_call_callee_object_type, f.hs_call_callee_object_type
            ) as hs_call_callee_object_type,
            coalesce(
                l.hs_call_from_number, f.hs_call_from_number
            ) as hs_call_from_number,
            coalesce(l.hs_call_to_number, f.hs_call_to_number) as hs_call_to_number,
            coalesce(
                l.hs_call_has_voicemail, f.hs_call_has_voicemail
            ) as hs_call_has_voicemail,
            coalesce(
                l.hs_call_recording_url, f.hs_call_recording_url
            ) as hs_call_recording_url,
            coalesce(l.hs_call_body, f.hs_call_body) as hs_call_body,
            coalesce(l.hs_call_summary, f.hs_call_summary) as hs_call_summary,
            coalesce(l.hs_call_title, f.hs_call_title) as hs_call_title,
            coalesce(l.hs_call_source, f.hs_call_source) as hs_call_source,
            coalesce(l.hs_activity_type, f.hs_activity_type) as hs_activity_type,
            coalesce(l.hubspot_owner_id, f.hubspot_owner_id) as hubspot_owner_id,
            -- The frozen stamp must win where there is one: int__hubspot_calls
            -- breaks its per-id dedupe on this column, and taking the later of
            -- the two would tie every frozen sync of a shared id.
            coalesce(
                f._airbyte_extracted_at, l._airbyte_extracted_at
            ) as _airbyte_extracted_at,
            -- Both feeds' numbers are carried out of the join unfiltered and
            -- tested in the where below, because a suppressed frozen number must
            -- exclude the call even when a Lakeflow number wins the coalesce.
            -- Filtering the frozen side before the join would instead let the
            -- Lakeflow row resurface as a Lakeflow-only row still carrying the
            -- erased call's body, summary and associations. These four are
            -- dropped at the final select so the output shape is unchanged.
            f.hs_call_from_number as frozen_hs_call_from_number,
            f.hs_call_to_number as frozen_hs_call_to_number,
            l.hs_call_from_number as lakeflow_hs_call_from_number,
            l.hs_call_to_number as lakeflow_hs_call_to_number
        from frozen as f
        full outer join ({{ lakeflow_calls }}) as l on l.id = f.id
    )

-- A suppressed number excludes the call whichever feed supplied it.
select
    * except (
        frozen_hs_call_from_number,
        frozen_hs_call_to_number,
        lakeflow_hs_call_from_number,
        lakeflow_hs_call_to_number
    )
from joined
where
    {{ dsar_not_suppressed("frozen_hs_call_from_number", "phone") }}
    and {{ dsar_not_suppressed("frozen_hs_call_to_number", "phone") }}
    and {{ dsar_not_suppressed("lakeflow_hs_call_from_number", "phone") }}
    and {{ dsar_not_suppressed("lakeflow_hs_call_to_number", "phone") }}
