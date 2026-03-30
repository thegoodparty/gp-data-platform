with
    organizations as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_organization") }}
    ),

    campaigns as (select * from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}),

    elected_offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    final as (
        select
            o.slug as organization_slug,
            o.owner_id as user_id,

            u.email as user_email,
            u.first_name as user_first_name,
            u.last_name as user_last_name,

            -- Position data (replaces campaign.details.positionId / office)
            o.position_id,
            o.custom_position_name,
            o.override_district_id,

            -- Linked entities
            c.id as campaign_id,
            eo.id as elected_office_id,

            -- Organization type
            case
                when eo.id is not null then 'serve' when c.id is not null then 'win'
            end as organization_type,

            o.created_at,
            o.updated_at
        from organizations o
        left join campaigns c on o.slug = c.organization_slug
        left join elected_offices eo on o.slug = eo.organization_slug
        left join users u on o.owner_id = u.id
    )

select *
from final
