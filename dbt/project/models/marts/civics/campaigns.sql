with
    campaigns as (
        select * from {{ ref("stg_airbyte_internal__raw_gp_api_db_campaign") }}
    ),

    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    organizations as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_organization") }}
    ),

    positions as (select * from {{ ref("m_election_api__position") }}),

    -- Lookup: BR position database_id → normalized_position_name
    -- Resolves via BR API position's normalized_position ref to the
    -- normalized position model (fetched from CivicEngine API).
    br_normalized_lookup as (
        select
            brp.database_id as br_database_id,
            brp.name as br_position_name,
            np.name as normalized_position_name
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as brp
        inner join
            {{ ref("int__ballotready_normalized_position") }} as np
            on brp.normalized_position.databaseid = np.database_id
    ),

    versioned as (
        select
            c.*,
            u.id as _user_id,
            u.email as _user_email,
            u.first_name as _user_first_name,
            u.last_name as _user_last_name,
            u.phone as _user_phone,
            u.zip as _user_zip,
            u.created_at as _user_created_at,
            p.name as _position_name,
            p.br_database_id as _position_br_database_id,
            c.details:office::string as _legacy_office,
            cast(
                regexp_extract(
                    cast(unbase64(c.details:positionid::string) as string),
                    '/([0-9]+)$',
                    1
                ) as bigint
            ) as _legacy_br_position_id,
            -- PD's pledged BR race id. details:raceId is base64-encoded
            -- gid://ballotready/Race/<id>; the decoded numeric matches BR's
            -- race database_id, so downstream models can resolve stage by id.
            -- try_cast handles empty-string raceIds (regex on '' returns '',
            -- which would error on cast to bigint).
            try_cast(
                regexp_extract(
                    cast(unbase64(c.details:raceid::string) as string), '/([0-9]+)$', 1
                ) as bigint
            ) as ballotready_race_id,
            row_number() over (
                partition by c.id
                order by c.updated_at desc nulls last, c._airbyte_extracted_at desc
            )
            = 1 as is_latest_version
        from campaigns c
        left join users u on c.user_id = u.id
        left join organizations o on c.organization_slug = o.slug
        left join positions p on o.position_id = p.id
    ),

    -- Resolve normalized_position_name via both org and legacy paths
    with_normalized as (
        select
            v.*,
            case
                when v.is_latest_version
                then
                    case
                        when v._position_br_database_id is not null
                        then bnl_org.normalized_position_name
                        else bnl_legacy.normalized_position_name
                    end
                else bnl_legacy.normalized_position_name
            end as _normalized_position_name
        from versioned as v
        left join
            br_normalized_lookup as bnl_org
            on v._position_br_database_id = bnl_org.br_database_id
        left join
            br_normalized_lookup as bnl_legacy
            on v._legacy_br_position_id = bnl_legacy.br_database_id
    ),

    final as (
        select
            campaign_version_id,
            id as campaign_id,
            slug as campaign_slug,
            organization_slug,

            data:hubspotid::string as hubspot_id,

            _user_id as user_id,
            _user_email as user_email,
            _user_first_name as user_first_name,
            _user_last_name as user_last_name,
            _user_phone as user_phone,
            _user_zip as user_zip,
            _user_created_at as user_created_at,

            created_at,
            updated_at,

            coalesce(is_verified, false) as is_verified,
            is_active,
            is_pro,
            is_demo,
            did_win,

            details:pledged::boolean as is_pledged,

            try_cast(details:electiondate::string as date) as election_date,
            details:state::string as campaign_state,
            details:party::string as campaign_party,
            details:level::string as election_level,
            details:partisantype::string as partisan_type,

            -- For latest versions: prefer org->position, fall back to legacy
            -- For historical versions: use legacy snapshot to stay faithful
            case
                when is_latest_version
                then coalesce(_position_name, _legacy_office)
                else _legacy_office
            end as campaign_office,
            case
                when is_latest_version
                then coalesce(_position_br_database_id, _legacy_br_position_id)
                else _legacy_br_position_id
            end as ballotready_position_id,
            ballotready_race_id,
            cast(_normalized_position_name as string) as normalized_position_name,

            is_latest_version
        from with_normalized
    )

select *
from final
