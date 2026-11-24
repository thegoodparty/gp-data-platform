-- Frozen Airbyte engagements plus a v1 projection of the five Lakeflow activity
-- tables. The name still says airbyte because 138 models ref it.
--
-- The frozen feed wins on id for every type: the Lakeflow activity tables carry
-- no associations, and int__user_hubspot_touches drops unassociated activities,
-- so a Lakeflow-owned row would leave the touch history while the row count
-- stayed flat. lakeflow_authoritative_engagement_types holds the exceptions and
-- is empty. SMS, CONVERSATION_SESSION and CUSTOM_CHANNEL_CONVERSATION have no
-- Lakeflow table at all. The loader time is materialized separately because
-- `_metadata` cannot survive a view referenced twice.
{% set associations %}
    -- No Lakeflow column carries an association of any kind.
    cast(null as string) as contact_ids,
    cast(null as string) as company_ids,
    cast(null as string) as deal_ids,
{% endset %}

{% set lakeflow_calls %}
    select
        c.hs_object_id as id,
        'CALL' as engagement_type,
        true as is_active,
        c.hs_gdpr_deleted as is_gdpr_deleted,
        c.hs_timestamp as occurred_at,
        c.hs_createdate as created_at,
        c.hs_lastmodifieddate as updated_at,
        nullif(c.hubspot_owner_id, '') as hubspot_owner_id,
        {{ associations }}
        -- A call's title lives on the calls stream, not the engagement envelope.
        cast(null as string) as subject,
        nullif(c.hs_call_status, '') as status,
        nullif(c.hs_call_body, '') as body,
        cast(null as string) as internal_meeting_notes,
        cast(null as string) as email_text,
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "calls") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id and lt.activity_type = 'CALL'
    -- A Lakeflow-only call from a suppressed number must not appear; a shared id
    -- keeps its frozen row below regardless.
    where
        {{ dsar_not_suppressed("nullif(c.hs_call_from_number, '')", "phone") }}
        and {{ dsar_not_suppressed("nullif(c.hs_call_to_number, '')", "phone") }}
{% endset %}

{% set lakeflow_emails %}
    select
        c.hs_object_id as id,
        -- From the column, not the table name: the name would relabel inbound outbound.
        nullif(c.hs_email_direction, '') as engagement_type,
        true as is_active,
        c.hs_gdpr_deleted as is_gdpr_deleted,
        c.hs_timestamp as occurred_at,
        c.hs_createdate as created_at,
        c.hs_lastmodifieddate as updated_at,
        nullif(c.hubspot_owner_id, '') as hubspot_owner_id,
        -- The one Lakeflow association that reaches a person, in the frozen
        -- feed's own format so downstream parsing is identical.
        case
            when nullif(c.hs_email_associated_contact_id, '') is not null
            then
                concat(
                    '[',
                    nullif(c.hs_email_associated_contact_id, ''),
                    ']'
                )
        end as contact_ids,
        cast(null as string) as company_ids,
        cast(null as string) as deal_ids,
        nullif(c.hs_email_subject, '') as subject,
        nullif(c.hs_email_status, '') as status,
        -- An email's plain text belongs in email_text.
        cast(null as string) as body,
        cast(null as string) as internal_meeting_notes,
        nullif(c.hs_email_text, '') as email_text,
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "emails") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id and lt.activity_type = 'EMAIL'
{% endset %}

{% set lakeflow_meetings %}
    select
        c.hs_object_id as id,
        'MEETING' as engagement_type,
        true as is_active,
        c.hs_gdpr_deleted as is_gdpr_deleted,
        c.hs_timestamp as occurred_at,
        c.hs_createdate as created_at,
        c.hs_lastmodifieddate as updated_at,
        nullif(c.hubspot_owner_id, '') as hubspot_owner_id,
        {{ associations }}
        cast(null as string) as subject,
        cast(null as string) as status,
        nullif(c.hs_meeting_body, '') as body,
        nullif(c.hs_internal_meeting_notes, '') as internal_meeting_notes,
        cast(null as string) as email_text,
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "meetings") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id and lt.activity_type = 'MEETING'
{% endset %}

{% set lakeflow_notes %}
    select
        c.hs_object_id as id,
        'NOTE' as engagement_type,
        true as is_active,
        c.hs_gdpr_deleted as is_gdpr_deleted,
        c.hs_timestamp as occurred_at,
        c.hs_createdate as created_at,
        c.hs_lastmodifieddate as updated_at,
        nullif(c.hubspot_owner_id, '') as hubspot_owner_id,
        {{ associations }}
        cast(null as string) as subject,
        cast(null as string) as status,
        nullif(c.hs_note_body, '') as body,
        cast(null as string) as internal_meeting_notes,
        cast(null as string) as email_text,
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "notes") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id and lt.activity_type = 'NOTE'
{% endset %}

{% set lakeflow_tasks %}
    select
        c.hs_object_id as id,
        'TASK' as engagement_type,
        true as is_active,
        c.hs_gdpr_deleted as is_gdpr_deleted,
        c.hs_timestamp as occurred_at,
        c.hs_createdate as created_at,
        c.hs_lastmodifieddate as updated_at,
        nullif(c.hubspot_owner_id, '') as hubspot_owner_id,
        {{ associations }}
        nullif(c.hs_task_subject, '') as subject,
        nullif(c.hs_task_status, '') as status,
        nullif(c.hs_task_body, '') as body,
        cast(null as string) as internal_meeting_notes,
        cast(null as string) as email_text,
        lt.loaded_at as _airbyte_extracted_at
    from {{ source("lakeflow_source", "tasks") }} as c
    inner join
        {{ ref("int__hubspot_lakeflow_activity_loaded_at") }} as lt
        on lt.activity_id = c.hs_object_id and lt.activity_type = 'TASK'
{% endset %}

{% set lakeflow %}
    select * from ({{ lakeflow_calls }})
    union all
    select * from ({{ lakeflow_emails }})
    union all
    select * from ({{ lakeflow_meetings }})
    union all
    select * from ({{ lakeflow_notes }})
    union all
    select * from ({{ lakeflow_tasks }})
{% endset %}

{%- set authoritative_types = var("lakeflow_authoritative_engagement_types", []) -%}

with
    frozen as (
        select
            cast(id as string) as id,
            type as engagement_type,
            active as is_active,
            gdprdeleted as is_gdpr_deleted,
            -- When the activity happened, as against when HubSpot recorded it.
            timestamp_millis(`timestamp`) as occurred_at,
            timestamp_millis(createdat) as created_at,
            timestamp_millis(lastupdated) as updated_at,
            cast(ownerid as string) as hubspot_owner_id,
            associations_contactids as contact_ids,
            associations_companyids as company_ids,
            associations_dealids as deal_ids,
            metadata_subject as subject,
            metadata_status as status,
            -- Free text. body is notes, meetings, tasks and SMS; text is the plain
            -- form of an email, whose body column is always empty.
            metadata_body as body,
            metadata_internalmeetingnotes as internal_meeting_notes,
            metadata_text as email_text,
            _airbyte_extracted_at
        from {{ source("airbyte_source", "hubspot_api_engagements") }}
    ),

    -- Whether this row comes from Lakeflow. A Lakeflow-only row must, having no
    -- frozen value to fall back on; a shared row only for a switched type.
    {%- set wins -%}
    f.id is null
    {%- if authoritative_types %}
        or (
            l.engagement_type in (
                {{ authoritative_types | map("tojson") | join(", ") }}
            )
        )
    {%- endif -%}
    {%- endset -%}

    -- One row per engagement id either feed holds.
    joined as (
        select
            coalesce(f.id, l.id) as id,
            case
                when {{ wins }} then l.engagement_type else f.engagement_type
            end as engagement_type,
            coalesce(f.is_active, l.is_active) as is_active,
            -- True if either feed says so, so an erasure is never undone.
            greatest(f.is_gdpr_deleted, l.is_gdpr_deleted) as is_gdpr_deleted,
            case
                when {{ wins }} then l.occurred_at else f.occurred_at
            end as occurred_at,
            coalesce(f.created_at, l.created_at) as created_at,
            case when {{ wins }} then l.updated_at else f.updated_at end as updated_at,
            case
                when {{ wins }} then l.hubspot_owner_id else f.hubspot_owner_id
            end as hubspot_owner_id,
            case
                when {{ wins }} then l.contact_ids else f.contact_ids
            end as contact_ids,
            case
                when {{ wins }} then l.company_ids else f.company_ids
            end as company_ids,
            case when {{ wins }} then l.deal_ids else f.deal_ids end as deal_ids,
            case when {{ wins }} then l.subject else f.subject end as subject,
            case when {{ wins }} then l.status else f.status end as status,
            case when {{ wins }} then l.body else f.body end as body,
            case
                when {{ wins }}
                then l.internal_meeting_notes
                else f.internal_meeting_notes
            end as internal_meeting_notes,
            case when {{ wins }} then l.email_text else f.email_text end as email_text,
            -- The frozen stamp must win where there is one, since this column
            -- breaks the per-id dedupe of a stream that can repeat a row per sync.
            coalesce(
                f._airbyte_extracted_at, l._airbyte_extracted_at
            ) as _airbyte_extracted_at
        from frozen as f
        full outer join ({{ lakeflow }}) as l on l.id = f.id
    )

select *
from joined
