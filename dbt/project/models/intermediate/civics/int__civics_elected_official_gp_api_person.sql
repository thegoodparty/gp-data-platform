{{ config(materialized="table", tags=["civics"]) }}

-- Person-grain rollup of gp-api elected officials. One row per gp_api_user_id.
-- Adopts BR's gp_elected_official_id via the bridge for matched users (so
-- the mart's 3-way FOJ on gp_elected_official_id collapses correctly).
-- Unmatched users get a salted UUID specific to gp_api so they don't
-- accidentally collide with BR/TS canonical IDs.
--
-- hubspot_contact_id is renamed/cast in staging
-- (stg_airbyte_source__gp_api_db_user) per project convention.
with
    gp_api as (select * from {{ ref("int__civics_elected_official_gp_api") }}),

    -- Latest elected_office record per user
    latest_per_user as (
        select *
        from gp_api
        qualify
            row_number() over (
                partition by gp_api_user_id
                order by
                    created_at desc nulls last,
                    term_start_date desc nulls last,
                    gp_api_elected_office_id desc
            )
            = 1
    ),

    bridge as (select * from {{ ref("int__civics_elected_official_gp_api_bridge") }}),

    br_person as (
        select gp_elected_official_id, br_candidate_id
        from {{ ref("int__civics_elected_official_ballotready_person") }}
    ),

    -- HubSpot contact ID — column renamed/cast in staging per project convention
    raw_users as (
        select id as user_id, hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    )

select
    -- Adopt BR canonical when bridge resolves a match; otherwise self-key
    coalesce(
        br.gp_elected_official_id,
        {{
            generate_salted_uuid(
                fields=[
                    "'elected_official_gp_api_user'",
                    "cast(lpu.gp_api_user_id as string)",
                ]
            )
        }}
    ) as gp_elected_official_id,

    lpu.gp_api_user_id,

    -- Audit: which elected_office record won the rollup
    lpu.gp_api_elected_office_id as selected_gp_api_elected_office_id,
    lpu.gp_api_campaign_id as selected_gp_api_campaign_id,
    lpu.gp_api_organization_slug as selected_gp_api_organization_slug,

    -- Person attributes (latest record's snapshot)
    lpu.first_name,
    lpu.last_name,
    lpu.full_name,

    -- Contact (exposed on mart as gp_api_email / gp_api_phone)
    lpu.email,
    lpu.phone,

    -- Party (exposed on mart as gp_api_party_affiliation)
    lpu.party_affiliation as gp_api_party_affiliation,

    -- Office attributes
    lpu.candidate_office,
    lpu.office_level,
    lpu.office_type,
    lpu.state,
    lpu.term_start_date,  -- gp_api sworn_in_date

    -- HubSpot linkage
    ru.hubspot_contact_id,

    lpu.created_at,
    lpu.updated_at,

    -- Bridge match audit
    bridge.cluster_id as er_cluster_id,
    bridge.br_office_holder_id as bridge_br_office_holder_id,
    bridge.br_candidate_id as bridge_br_candidate_id

from latest_per_user lpu
left join bridge on bridge.gp_api_elected_office_id = lpu.gp_api_elected_office_id
left join br_person br on br.br_candidate_id = bridge.br_candidate_id
left join raw_users ru on ru.user_id = lpu.gp_api_user_id
