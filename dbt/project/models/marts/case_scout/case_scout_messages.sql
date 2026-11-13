-- Live over gp-api Postgres through federation, read nightly by the case scout
-- with a filter on created_at. created_at passes through bare and is a grouping
-- key below, so that filter reaches the chat_message scan in Postgres.
with
    conversations as (
        select id, scope, owner_user_id, organization_slug
        from {{ source("gp_api_pg", "chat_conversation") }}
        where deleted_at is null
    ),

    users as (
        select id, email, first_name, last_name from {{ source("gp_api_pg", "user") }}
    ),

    organizations as (
        select slug, owner_id, position_id, override_district_id, custom_position_name
        from {{ source("gp_api_pg", "organization") }}
    ),

    campaigns as (
        select organization_slug, details from {{ source("gp_api_pg", "campaign") }}
    ),

    -- organization.position_id and override_district_id are election-api ids,
    -- and election-api dev and prod both load from these marts.
    positions as (
        select id, name, district_id from {{ ref("m_election_api__position") }}
    ),

    districts as (
        select id, l2_district_name from {{ ref("m_election_api__district") }}
    ),

    conversation_context as (
        select
            conversations.id as conversation_id,
            conversations.scope,
            -- The product chats digest's staff and test-account rule. The email
            -- stays inside the view.
            (
                lower(owners.email) like '%@goodparty.org'
                or lower(owners.email) like '%+test%'
                or lower(owners.email) like '%mailinator%'
            ) as is_internal,
            -- Names and places tied to the account, for the scout's PII gate.
            -- The organization owner is listed too, since a team member's chat
            -- names the candidate or official rather than the member.
            array_distinct(
                filter(
                    transform(
                        array(
                            owners.first_name,
                            owners.last_name,
                            organization_owners.first_name,
                            organization_owners.last_name,
                            campaigns.details:campaigncommittee::string,
                            positions.name,
                            organizations.custom_position_name,
                            campaigns.details:office::string,
                            campaigns.details:city::string,
                            campaigns.details:county::string,
                            campaigns.details:district::string,
                            position_districts.l2_district_name,
                            override_districts.l2_district_name
                        ),
                        term -> nullif(trim(term), '')
                    ),
                    term -> term is not null
                )
            ) as denylist
        from conversations
        inner join users as owners on conversations.owner_user_id = owners.id
        left join organizations on conversations.organization_slug = organizations.slug
        left join
            users as organization_owners
            on organizations.owner_id = organization_owners.id
        left join
            campaigns on conversations.organization_slug = campaigns.organization_slug
        left join positions on organizations.position_id = positions.id
        left join
            districts as position_districts
            on positions.district_id = position_districts.id
        left join
            districts as override_districts
            on organizations.override_district_id = override_districts.id
    ),

    -- chat_message_segment has no time column. Joining it before aggregating,
    -- with every message column a grouping key, keeps a created_at filter
    -- pushable and lets a federated join pushdown bound the segment read too.
    messages_with_tools as (
        select
            messages.conversation_id,
            messages.id as message_id,
            messages.role,
            messages.content,
            messages.created_at,
            transform(
                array_sort(
                    collect_list(
                        struct(
                            segments.ordinal,
                            segments.tool_name,
                            cast(segments.payload as string) as payload
                        )
                    ) filter (where segments.chat_message_id is not null)
                ),
                segment
                -> named_struct('tool', segment.tool_name, 'input', segment.payload)
            ) as tool_calls
        from {{ source("gp_api_pg", "chat_message") }} as messages
        left join
            {{ source("gp_api_pg", "chat_message_segment") }} as segments
            on messages.id = segments.chat_message_id
            and segments.kind = 'tool'
        group by
            messages.conversation_id,
            messages.id,
            messages.role,
            messages.content,
            messages.created_at
    )

select
    messages_with_tools.conversation_id,
    conversation_context.scope,
    messages_with_tools.message_id,
    messages_with_tools.role,
    messages_with_tools.content,
    messages_with_tools.created_at,
    messages_with_tools.tool_calls,
    conversation_context.is_internal,
    conversation_context.denylist
from messages_with_tools
inner join
    conversation_context
    on messages_with_tools.conversation_id = conversation_context.conversation_id
