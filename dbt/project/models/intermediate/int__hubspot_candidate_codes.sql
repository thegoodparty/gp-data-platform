{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    candidates as (
        select
            properties_firstname,
            properties_lastname,
            properties_state,
            properties_city,
            properties_office_type,
            {{
                generate_candidate_code(
                    "properties_firstname",
                    "properties_lastname",
                    "properties_state",
                    "properties_city",
                    "properties_office_type",
                )
            }} as hubspot_candidate_code,
            `updatedAt` as updated_at
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        where
            trim(properties_firstname) is not null
            and trim(properties_firstname) <> ''
            and trim(properties_lastname) is not null
            and trim(properties_lastname) <> ''
            and trim(properties_state) is not null
            and trim(properties_state) <> ''
            and trim(properties_office_type) is not null
            and trim(properties_office_type) <> ''
            and (
                properties_type like '%Self-Filer Lead%'
                or properties_product_user = 'yes'
            )
            and properties_election_date
            between date_trunc('year', current_date) and date_trunc(
                'year', current_date + interval 1 year
            )
            - interval 1 day
            {% if is_incremental() %}
                and `updatedAt` >= (select max(updated_at) from {{ this }})
            {% endif %}
    )

select
    hubspot_candidate_code,
    updated_at,
    properties_firstname,
    properties_lastname,
    properties_state,
    properties_city,
    properties_office_type
from candidates
