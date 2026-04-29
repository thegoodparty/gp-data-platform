{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB elected offices -> Civics mart elected_officials schema.
-- Grain: one row per gp_api_db_elected_office row whose campaign_id resolves
-- in the campaigns mart. Source of truth for "is a sitting elected official"
-- — campaigns.did_win is too stale to rely on. Schema is narrower than BR's
-- elected_official (no mailing/term-end/judicial/vacancy fields). No
-- gp_candidate_id (elected_official is its own entity, same as BR).
with
    elected_offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    latest_campaigns as (select * from {{ ref("campaigns") }} where is_latest_version),

    users as (
        select user_id, first_name, last_name, email, phone from {{ ref("users") }}
    ),

    -- v2.2: BR API position table for br_position_level fallback (mirrors
    -- candidacy gp_api_with_office at int__er_prematch_candidacy_stages.sql lines
    -- 318-320).
    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    joined as (
        -- INNER JOIN to campaigns both enriches and filters orphans (rows
        -- whose campaign_id doesn't exist in the campaigns mart).
        -- v2.2: demo filter mirrors candidacy gp_api_campaigns at line 262.
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
            c.campaign_office as candidate_office_input,  -- raw input to office derivation
            c.campaign_office as campaign_office_raw,  -- v2.2: preserved for prematch
            c.normalized_position_name,  -- v2.2: exposed for prematch
            c.election_level,
            c.campaign_state as state,
            -- v2.2: new from campaigns mart
            c.ballotready_position_id,
            -- v2.2: new from BR position lookup
            brp.level as br_position_level,
            {{ parse_party_affiliation("c.campaign_party") }} as party_affiliation
        from elected_offices as eo
        inner join latest_campaigns as c on eo.campaign_id = c.campaign_id
        left join users as u on eo.user_id = u.user_id
        left join br_position as brp on c.ballotready_position_id = brp.database_id
        where not coalesce(c.is_demo, false)
    ),

    -- v2.2: compute candidate_office once in its own CTE so map_office_type can
    -- reference it as a real column, instead of nesting Jinja macros inside another
    -- macro's string argument. Mirrors candidacy gp_api_with_office at
    -- int__er_prematch_candidacy_stages.sql lines 295-312.
    joined_with_office as (
        select
            j.*,
            coalesce(
                {{
                    generate_candidate_office_from_position(
                        "j.candidate_office_input",
                        "j.normalized_position_name",
                    )
                }},
                initcap(trim(j.candidate_office_input))
            ) as candidate_office
        from joined as j
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
            candidate_office,  -- computed in joined_with_office
            -- v2.3: prefer BR's 7-bucket taxonomy when br_position_level is
            -- available (~84% of gp-api EO rows have a BR position FK). Mirrors
            -- initcap(level) on int__civics_elected_official_ballotready, so
            -- ExactMatch("office_level") works cross-source for those rows.
            -- For the ~16% without a BR position FK, fall back to gp-api's
            -- native election_level (4-bucket: City/Local collapse to Local;
            -- can't distinguish City/Town/Township at this granularity).
            case
                when br_position_level is not null
                then initcap(br_position_level)
                when lower(election_level) in ('city', 'local')
                then 'Local'
                when lower(election_level) = 'county'
                then 'County'
                when lower(election_level) = 'state'
                then 'State'
                when lower(election_level) = 'federal'
                then 'Federal'
                else null
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
            -- v2.2: new exposed columns (appended to preserve existing column order)
            ballotready_position_id,
            normalized_position_name,
            br_position_level,
            campaign_office_raw,
            created_at,
            updated_at
        from joined_with_office
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
    -- v2.2 new
    ballotready_position_id,
    normalized_position_name,
    br_position_level,
    campaign_office_raw,
    created_at,
    updated_at
from final
