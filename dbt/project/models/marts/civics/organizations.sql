with
    organizations as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_organization") }}
    ),

    campaigns as (select * from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}),

    elected_offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    positions as (select * from {{ ref("m_election_api__position") }}),

    districts as (select * from {{ ref("m_election_api__district") }}),

    final as (
        select
            o.slug as organization_slug,
            o.owner_id as user_id,

            u.email as user_email,
            u.first_name as user_first_name,
            u.last_name as user_last_name,

            -- Position data (via organization -> election-api)
            o.position_id,
            p.name as position_name,
            p.br_database_id as ballotready_position_id,
            p.state as position_state,
            o.custom_position_name,

            -- District data (via position -> district, or override)
            o.override_district_id,
            p.district_id,
            d.l2_district_type,
            d.l2_district_name,

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
        left join positions p on o.position_id = p.id
        left join districts d on p.district_id = d.id
    )

select *
from final
