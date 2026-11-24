-- Catches a precedence flip before it empties the touch history: a frozen row
-- that takes its association from Lakeflow leaves int__user_hubspot_touches
-- while the staging row count stays flat. Covers both staging models because
-- only the engagements one has a switch. Computed at check time, not pinned.
with

    -- Calls: the frozen stream's own `contacts` column.
    call_mismatches as (
        select staged.id
        from {{ ref("stg_airbyte_source__hubspot_api_engagements_calls") }} as staged
        inner join
            {{ source("airbyte_source", "hubspot_api_engagements_calls") }} as frozen
            on frozen.id = staged.id
        where
            frozen.contacts is not null
            and trim(frozen.contacts) not in ('', '[]')
            and coalesce(
                array_sort(from_json(staged.contacts, 'array<string>'))
                = array_sort(from_json(frozen.contacts, 'array<string>')),
                false
            )
            = false
    ),

    -- Every other activity type, whose association column is `contact_ids` on
    -- the engagements stream. Covers the types the switch can reach, so a type
    -- moved to Lakeflow without its associations loaded shows up here.
    engagement_mismatches as (
        select staged.id
        from {{ ref("stg_airbyte_source__hubspot_api_engagements") }} as staged
        inner join
            {{ source("airbyte_source", "hubspot_api_engagements") }} as frozen
            on frozen.id = cast(staged.id as string)
        where
            staged.engagement_type in (
                'CALL',
                'EMAIL',
                'INCOMING_EMAIL',
                'FORWARDED_EMAIL',
                'MEETING',
                'NOTE',
                'TASK'
            )
            and frozen.associations_contactids is not null
            and trim(frozen.associations_contactids) not in ('', '[]')
            and coalesce(
                array_sort(from_json(staged.contact_ids, 'array<string>')) = array_sort(
                    from_json(frozen.associations_contactids, 'array<string>')
                ),
                false
            )
            = false
    )

select id
from call_mismatches

union all

select id
from engagement_mismatches
