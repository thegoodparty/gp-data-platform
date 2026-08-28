-- TechSpeed candidates → Civics mart candidate schema.
-- gp_candidate_id is the person id: cluster-linked TS records share the
-- BR/gp_api person's group, records absent from ER self-mint (own person).
with
    -- Must match int__civics_candidacy_techspeed.
    ts_person as (
        select
            {{ strip_ts_stage_suffix("substring_index(record_key, '|', -1)") }}
            as ts_source_candidate_id,
            min(gp_person_id) as gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'techspeed'
        group by 1
    ),

    source as (
        select
            ts.*,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    candidates_with_id as (
        select
            coalesce(
                tp.gp_person_id,
                {{
                    generate_salted_uuid(
                        fields=["'techspeed'", "source.techspeed_candidate_code"],
                        salt="person",
                    )
                }}
            ) as gp_candidate_id,

            cast(null as string) as hubspot_contact_id,
            cast(null as string) as prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,
            birth_date_parsed as birth_date,
            state_postal_code as state,
            email,
            phone as phone_number,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        left join
            ts_person as tp
            on source.techspeed_candidate_code = tp.ts_source_candidate_id
        where first_name is not null and last_name is not null and state is not null
    ),

    deduplicated as (
        select *
        from candidates_with_id
        qualify
            row_number() over (
                partition by gp_candidate_id
                order by
                    updated_at desc, email asc nulls last, phone_number asc nulls last
            )
            = 1
    )

select
    gp_candidate_id,
    hubspot_contact_id,
    prod_db_user_id,
    candidate_id_tier,
    first_name,
    last_name,
    full_name,
    birth_date,
    state,
    email,
    phone_number,
    street_address,
    website_url,
    linkedin_url,
    twitter_handle,
    facebook_url,
    instagram_handle,
    created_at,
    updated_at
from deduplicated
