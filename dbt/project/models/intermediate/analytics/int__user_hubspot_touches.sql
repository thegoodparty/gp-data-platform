-- One row per user per HubSpot engagement: the sales and marketing contact
-- history behind the per-user counts, at the grain a reader can audit.
--
-- Table, not view: an explode over 950k engagements unioned with the call
-- stream, read by the per-user rollup, by the mart and by several tests.
{{ config(materialized="table") }}

-- Both legs suppress right-to-erasure rows. Redundant against today's data,
-- because HubSpot strips the contact association when it erases an engagement
-- and all 18 erased rows already fail the association filter, but a partial
-- erasure or a sync landing the association first would otherwise put a
-- suppressed touch on a user row.
with
    keys as (
        select user_id, gp_person_id, hubspot_contact_id
        from {{ ref("int__user_resolved_keys") }}
    ),

    -- Touch history follows every HubSpot contact the person group reaches,
    -- not only the named one. Named-only leaves 665 users with no history at
    -- all and loses 13% of touches, while the duplicates are overwhelmingly
    -- the same human under a second address: of the 1,467 extra contacts
    -- carrying a different email, 1,305 share the user's surname and 100 do
    -- not. The named contact is unioned in because it can resolve through the
    -- user's own live id while the person graph does not hold that contact.
    user_contacts as (
        select k.user_id, pi.source_id as contact_id
        from keys as k
        join {{ ref("person_identifiers") }} as pi on pi.gp_person_id = k.gp_person_id
        where pi.source_name = 'hubspot'
        union
        select user_id, hubspot_contact_id as contact_id
        from keys
        where hubspot_contact_id is not null
    ),

    call_touches as (
        select
            cast(id as string) as touch_id,
            'call' as channel,
            -- hs_timestamp is when the call happened, created_at when the
            -- record was written.
            coalesce(hs_timestamp, created_at) as touch_at,
            hubspot_owner_id,
            outcome_family as call_outcome_family,
            explode(from_json(contacts, 'array<string>')) as contact_id
        from {{ ref("int__hubspot_calls") }}
        where
            contacts is not null
            and trim(contacts) not in ('', '[]')
            and not coalesce(is_gdpr_deleted, false)
    ),

    other_touches as (
        select
            id as touch_id,
            case
                engagement_type
                when 'EMAIL'
                then 'email'
                when 'INCOMING_EMAIL'
                then 'inbound_email'
                when 'MEETING'
                then 'meeting'
                when 'SMS'
                then 'sms'
                when 'TASK'
                then 'task'
                when 'NOTE'
                then 'note'
                when 'CONVERSATION_SESSION'
                then 'conversation'
                when 'CUSTOM_CHANNEL_CONVERSATION'
                then 'conversation'
                -- A type HubSpot adds later lands here rather than vanishing.
                else 'other'
            end as channel,
            occurred_at as touch_at,
            hubspot_owner_id,
            cast(null as string) as call_outcome_family,
            explode(from_json(contact_ids, 'array<string>')) as contact_id
        from {{ ref("stg_airbyte_source__hubspot_api_engagements") }}
        where
            engagement_type <> 'CALL'
            and contact_ids is not null
            and trim(contact_ids) not in ('', '[]')
            and not coalesce(is_gdpr_deleted, false)
    ),

    all_touches as (
        select *
        from call_touches
        union all
        select *
        from other_touches
    ),

    final as (
        select
            uc.user_id,
            t.touch_id,
            t.contact_id,
            t.channel,
            -- What we did to reach the person. Tasks, notes, inbound mail and
            -- chat sessions stay on the row as history but are never counted
            -- as contact made.
            t.channel in ('call', 'email', 'meeting', 'sms') as is_outbound_touch,
            -- The disposition seed groups every connected outcome under a
            -- connected_ prefix; null for every channel but calls.
            case
                when t.channel = 'call'
                then startswith(t.call_outcome_family, 'connected_')
            end as is_connected_call,
            t.touch_at,
            t.hubspot_owner_id,
            t.call_outcome_family
        from all_touches as t
        join user_contacts as uc on uc.contact_id = t.contact_id
        -- One engagement can be associated with two contacts in the same
        -- person group, which would otherwise count the touch twice.
        qualify
            row_number() over (
                partition by uc.user_id, t.touch_id order by t.contact_id
            )
            = 1
    )

select *
from final
