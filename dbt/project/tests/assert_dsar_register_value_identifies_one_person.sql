-- A register email or phone that matches more than one person is a shared value
-- (a campaign inbox, an office line). Warn only; the fix is a register correction.
{{ config(severity="warn") }}

{%- set first_party = [
    ("gp_api_db_user", "id", "email", "phone"),
    (
        "hubspot_api_contacts",
        "id",
        "get_json_object(properties, '$.email')",
        "get_json_object(properties, '$.phone')",
    ),
] %}

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

    -- One (type, value, person) row per contact field, from a single pass per source.
    people as (
        {%- for source_name, id_column, email_column, phone_column in first_party %}
            select
                '{{ source_name }}' as source_name,
                contact.identifier_type,
                contact.identifier_value,
                {{ id_column }} as person_id
            from {{ source("airbyte_source", source_name) }}
            lateral view
                stack(
                    2,
                    'email',
                    {{ dsar_normalize(email_column, "email") }},
                    'phone',
                    {{ dsar_normalize(phone_column, "phone") }}
                ) contact as identifier_type,
                identifier_value
                {{ "union all" if not loop.last }}
        {%- endfor %}
    )

select
    people.identifier_type,
    people.identifier_value,
    people.source_name,
    count(distinct people.person_id) as people
from register
join people using (identifier_type, identifier_value)
group by 1, 2, 3
having count(distinct people.person_id) > 1
