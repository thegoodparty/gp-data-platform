-- A register email or phone is meant to identify one person. If it matches more
-- than one gp-api user or HubSpot contact, it is a shared value (a campaign
-- inbox, an office line) and every one of those people is being suppressed on
-- the strength of one request. Warn only: the rows are already filtered, and
-- the fix is a register correction on the ticket, not a blocked build.
-- depends_on: {{ ref("stg_source_dsar__suppressed_identifiers") }}
{{ config(severity="warn") }}

with
    register as (
        select
            identifier_type,
            case
                identifier_type
                when 'email'
                then {{ dsar_normalize("identifier_value", "email") }}
                when 'phone'
                then {{ dsar_normalize("identifier_value", "phone") }}
            end as identifier_value
        from {{ ref("stg_source_dsar__suppressed_identifiers") }}
        where identifier_type in ('email', 'phone')
    ),

    gp_api_matches as (
        select
            r.identifier_type,
            r.identifier_value,
            'gp_api_db_user' as source_name,
            count(distinct u.id) as people
        from register as r
        join
            {{ source("airbyte_source", "gp_api_db_user") }} as u
            on (
                r.identifier_type = 'email'
                and {{ dsar_normalize("u.email", "email") }} = r.identifier_value
            )
            or (
                r.identifier_type = 'phone'
                and {{ dsar_normalize("u.phone", "phone") }} = r.identifier_value
            )
        group by 1, 2
    ),

    hubspot_matches as (
        select
            r.identifier_type,
            r.identifier_value,
            'hubspot_api_contacts' as source_name,
            count(distinct c.id) as people
        from register as r
        join
            {{ source("airbyte_source", "hubspot_api_contacts") }} as c
            on (
                r.identifier_type = 'email'
                and {{
                    dsar_normalize(
                        "get_json_object(c.properties, '$.email')", "email"
                    )
                }} = r.identifier_value
            )
            or (
                r.identifier_type = 'phone'
                and {{
                    dsar_normalize(
                        "get_json_object(c.properties, '$.phone')", "phone"
                    )
                }} = r.identifier_value
            )
        group by 1, 2
    ),

    matches as (
        select *
        from gp_api_matches
        union all
        select *
        from hubspot_matches
    )

select identifier_type, source_name, people
from matches
where people > 1
