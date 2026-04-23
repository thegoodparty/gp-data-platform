{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB elected offices -> Civics mart elected_officials schema.
--
-- Grain: One row per gp_api_db_elected_office row, after filtering rows whose
-- campaign_id doesn't resolve in the campaigns mart (~9 orphans as of spec).
--
-- Source of truth for the "this Product DB user is a sitting elected official"
-- signal. `campaigns.did_win` is stale (only 165 of 1,811 elected_offices have
-- it set), so we use the presence of an elected_office row instead.
--
-- Schema narrower than int__civics_elected_official_ballotready — omits BR-
-- specific mailing/term-end/judicial/vacancy fields that Product DB doesn't
-- track. No gp_candidate_id column (elected_official is its own entity, same
-- as BR's schema).
with
    elected_offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    latest_campaigns as (select * from {{ ref("campaigns") }} where is_latest_version),

    users as (
        select user_id, first_name, last_name, email, phone from {{ ref("users") }}
    ),

    joined as (
        -- INNER JOIN to campaigns both enriches and filters orphans (rows
        -- whose campaign_id doesn't exist in the campaigns mart).
        select
            eo.id as gp_api_elected_office_id,
            eo.user_id as gp_api_user_id,
            eo.campaign_id as gp_api_campaign_id,
            eo.organization_slug as gp_api_organization_slug,
            eo.sworn_in_date,
            eo.created_at,
            eo.updated_at,
            u.first_name,
            u.last_name,
            u.email,
            u.phone,
            c.normalized_position_name as position_name,
            c.campaign_office as candidate_office,
            c.election_level,
            c.campaign_state as state,
            c.campaign_party as party_affiliation
        from elected_offices as eo
        inner join latest_campaigns as c on eo.campaign_id = c.campaign_id
        left join users as u on eo.user_id = u.user_id
    ),

    final as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "'gp_api_elected_office'",
                        "cast(gp_api_elected_office_id as string)",
                    ]
                )
            }} as gp_elected_official_id,
            gp_api_elected_office_id,
            gp_api_user_id,
            gp_api_campaign_id,
            gp_api_organization_slug,
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,
            email,
            {{ clean_phone_number("phone") }} as phone,
            cast(null as string) as office_phone,
            cast(null as string) as central_phone,
            position_name,
            candidate_office,
            case
                when lower(election_level) = 'city'
                then 'Local'
                else initcap(election_level)
            end as office_level,
            {{ map_office_type("candidate_office") }} as office_type,
            state,
            cast(null as string) as city,
            cast(null as string) as district,
            sworn_in_date as term_start_date,
            cast(null as date) as term_end_date,
            cast(null as boolean) as is_appointed,
            cast(null as boolean) as is_judicial,
            cast(null as boolean) as is_vacant,
            cast(null as boolean) as is_off_cycle,
            party_affiliation,
            cast(null as string) as website_url,
            cast(null as string) as linkedin_url,
            cast(null as string) as facebook_url,
            cast(null as string) as twitter_url,
            'gp_api' as candidate_id_source,
            created_at,
            updated_at
        from joined
    )

-- No dedup: gp_elected_official_id is a salted UUID of elected_office.id,
-- which is already unique in the source table.
select
    gp_elected_official_id,
    gp_api_elected_office_id,
    gp_api_user_id,
    gp_api_campaign_id,
    gp_api_organization_slug,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    office_phone,
    central_phone,
    position_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    term_start_date,
    term_end_date,
    is_appointed,
    is_judicial,
    is_vacant,
    is_off_cycle,
    party_affiliation,
    website_url,
    linkedin_url,
    facebook_url,
    twitter_url,
    candidate_id_source,
    created_at,
    updated_at
from final
