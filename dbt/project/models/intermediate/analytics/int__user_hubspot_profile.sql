-- The HubSpot slice of the end-to-end user view: acquisition and sales contact
-- at gp-api user grain, one row per user whether or not they reach a contact.
--
-- Only facts HubSpot originates are read here. Fields that merely surface in
-- HubSpot belong to the system that produces them: UTMs are carried on 60 of
-- 362k contacts and come from product telemetry instead.
--
-- Table, not view: a five-relation join read by the wide journey table and by
-- its tests.
{{ config(materialized="table") }}

with
    keys as (
        select user_id, hubspot_contact_id, hubspot_key_source, hubspot_contact_count
        from {{ ref("int__user_resolved_keys") }}
    ),

    users as (
        -- Signup is stored without a zone while HubSpot stamps are instants,
        -- so the cast happens once here rather than by implicit rule inside
        -- each comparison below.
        select user_id, cast(created_at as timestamp) as registered_at
        from {{ ref("users") }}
    ),

    contacts as (
        select
            cast(id as string) as contact_id,
            hubspot_owner_id,
            lifecycle_stage,
            win_lifecycle_stage,
            serve_lifecycle_stage,
            hs_analytics_source,
            hs_analytics_source_data_1,
            conversion_source,
            contact_created_at,
            hs_analytics_first_timestamp,
            last_contacted_at,
            lower(trim(email)) as contact_email
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    -- An exact email edge would reach 4,074 of the users who resolve to no
    -- contact. Publishing it beside the reason keeps that recoverable
    -- population visible: entity resolution owns closing it, not this model.
    email_reachable as (
        select distinct lower(trim(u.email)) as user_email
        from {{ ref("stg_airbyte_source__gp_api_db_user") }} as u
        inner join contacts as c on c.contact_email = lower(trim(u.email))
    ),

    own_ids as (
        select
            cast(id as bigint) as user_id,
            hubspot_contact_id,
            lower(trim(email)) as user_email
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    touch_rollup as (
        select
            user_id,
            count(case when channel = 'call' then 1 end) as call_count,
            count(case when is_connected_call then 1 end) as connected_call_count,
            count(case when channel = 'email' then 1 end) as email_count,
            count(case when channel = 'meeting' then 1 end) as meeting_count,
            count(case when channel = 'sms' then 1 end) as sms_count,
            count(
                case when channel = 'inbound_email' then 1 end
            ) as inbound_email_count,
            count(case when channel = 'task' then 1 end) as task_count,
            count(case when channel = 'note' then 1 end) as note_count,
            count(case when is_outbound_touch then 1 end) as touch_count_total,
            min(case when channel = 'call' then touch_at end) as first_call_at,
            max(case when channel = 'call' then touch_at end) as last_call_at,
            min(case when channel = 'email' then touch_at end) as first_email_at,
            max(case when channel = 'email' then touch_at end) as last_email_at,
            min(case when channel = 'meeting' then touch_at end) as first_meeting_at,
            max(case when channel = 'meeting' then touch_at end) as last_meeting_at,
            min(case when channel = 'sms' then touch_at end) as first_sms_at,
            max(case when channel = 'sms' then touch_at end) as last_sms_at
        from {{ ref("int__user_hubspot_touches") }}
        group by user_id
    )

select
    k.user_id,
    k.hubspot_key_source <> 'none' as has_hubspot_contact,
    case
        when k.hubspot_key_source <> 'none'
        then null
        -- A native id that no longer resolves is a different problem from
        -- never having had one, so the two are not collapsed.
        when o.hubspot_contact_id is not null
        then 'dangling_contact_id'
        when er.user_email is not null
        then 'email_match_unresolved'
        else 'no_contact_found'
    end as hubspot_unmatched_reason,
    case
        when k.hubspot_key_source = 'none' then er.user_email is not null
    end as hubspot_email_match_available,
    k.hubspot_contact_count,

    c.hubspot_owner_id as hs_owner_id,
    c.lifecycle_stage as hs_lifecycle_stage,
    c.win_lifecycle_stage as hs_win_lifecycle_stage,
    c.serve_lifecycle_stage as hs_serve_lifecycle_stage,
    c.hs_analytics_source,
    c.hs_analytics_source_data_1 as hs_analytics_source_detail,
    c.conversion_source as hs_conversion_source,
    c.contact_created_at as hs_contact_created_at,
    c.hs_analytics_first_timestamp as hs_first_analytics_at,
    c.last_contacted_at as hs_last_contacted_at,

    -- Earliest moment we know of, from either side. least() skips nulls, so a
    -- user with no contact falls back to their own signup.
    least(
        c.contact_created_at, c.hs_analytics_first_timestamp, u.registered_at
    ) as first_touch_at,
    -- True for 38,768 users, where the contact was created by the signup sync
    -- rather than by an earlier marketing touch. Without it every lead-time
    -- average is computed over a population that is 61% zeros.
    to_date(first_touch_at) = to_date(u.registered_at) as first_touch_is_signup,
    -- OFFLINE means the contact reached HubSpot by import or integration
    -- rather than by its own web session: 20,188 of the 21,767 are list
    -- imports. Timing agrees independently, 91% of those contacts predate
    -- signup by a day or more against 12% of the web-sourced ones. Owner
    -- assignment is deliberately not part of the rule: 92% of matched users
    -- carry an owner, so it separates nothing.
    case
        when k.hubspot_key_source = 'none' or c.hs_analytics_source is null
        then 'unknown'
        when c.hs_analytics_source = 'OFFLINE'
        then 'sales_sourced'
        else 'marketing_sourced'
    end as acquisition_motion,

    -- Zero where we can see HubSpot and nothing happened, null where we cannot
    -- see it at all. Reading one as the other reports an unresolved user as an
    -- uncontacted one.
    case when has_hubspot_contact then coalesce(t.call_count, 0) end as call_count,
    case
        when has_hubspot_contact then coalesce(t.connected_call_count, 0)
    end as connected_call_count,
    case when has_hubspot_contact then coalesce(t.email_count, 0) end as email_count,
    case
        when has_hubspot_contact then coalesce(t.meeting_count, 0)
    end as meeting_count,
    case when has_hubspot_contact then coalesce(t.sms_count, 0) end as sms_count,
    case
        when has_hubspot_contact then coalesce(t.inbound_email_count, 0)
    end as inbound_email_count,
    case when has_hubspot_contact then coalesce(t.task_count, 0) end as task_count,
    case when has_hubspot_contact then coalesce(t.note_count, 0) end as note_count,
    case
        when has_hubspot_contact then coalesce(t.touch_count_total, 0)
    end as touch_count_total,
    t.first_call_at,
    t.last_call_at,
    t.first_email_at,
    t.last_email_at,
    t.first_meeting_at,
    t.last_meeting_at,
    t.first_sms_at,
    t.last_sms_at
from keys as k
left join users as u using (user_id)
left join own_ids as o using (user_id)
left join email_reachable as er on er.user_email = o.user_email
left join contacts as c on c.contact_id = k.hubspot_contact_id
left join touch_rollup as t on t.user_id = k.user_id
