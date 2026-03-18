-- BallotReady office holders -> Civics mart elected_officials schema
-- Source: stg_airbyte_source__ballotready_s3_office_holders_v3
--
-- Grain: One row per office-holder term.
--
with
    office_holders as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
    ),

    extracted_contacts_and_urls as (
        select
            office_holders.*,
            get(
                filter(contacts, x -> x.type = 'primary' and x.email is not null), 0
            ).email as email,
            get(
                filter(contacts, x -> x.type = 'office' and x.phone is not null), 0
            ).phone as office_phone,
            get(
                filter(contacts, x -> x.type = 'central' and x.phone is not null), 0
            ).phone as central_phone,
            get(filter(urls, x -> x.type = 'website'), 0).url as website_url,
            get(filter(urls, x -> x.type = 'linkedin'), 0).url as linkedin_url,
            get(filter(urls, x -> x.type = 'facebook'), 0).url as facebook_url,
            get(filter(urls, x -> x.type = 'twitter'), 0).url as twitter_url
        from office_holders
    ),

    derived_fields as (
        select
            extracted_contacts_and_urls.*,
            concat(first_name, ' ', last_name) as full_name,
            {{
                generate_candidate_office_from_position(
                    "position_name",
                    "normalized_position_name",
                )
            }} as candidate_office,
            case
                when lower(level) = 'city' then 'Local' else initcap(level)
            end as office_level,
            {{ extract_city_from_office_name("position_name") }} as city,
            coalesce(
                regexp_extract(
                    position_name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district,
            case
                when get(party_names, 0) is null
                then null
                else {{ parse_party_affiliation("get(party_names, 0)") }}
            end as party_affiliation,
            case
                when end_at >= current_date() or end_at is null then true else false
            end as is_current_term
        from extracted_contacts_and_urls
    ),

    elected_officials as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "candidate_office",
                        "cast(start_at as string)",
                        "district",
                    ]
                )
            }} as gp_elected_official_id,
            case
                when is_vacant
                then cast(null as string)
                else
                    {{
                        generate_salted_uuid(
                            fields=[
                                "first_name",
                                "last_name",
                                "state",
                                "cast(null as string)",
                                "email",
                                "coalesce(office_phone, central_phone, get(filter(contacts, x -> x.phone is not null), 0).phone)",
                            ]
                        )
                    }}
            end as gp_candidate_id,
            br_office_holder_id,
            br_candidate_id,
            br_position_id,
            br_candidacy_id,
            first_name,
            last_name,
            middle_name,
            suffix,
            full_name,
            email,
            coalesce(
                office_phone,
                central_phone,
                get(filter(contacts, x -> x.phone is not null), 0).phone
            ) as phone,
            office_phone,
            central_phone,
            position_name,
            normalized_position_name,
            candidate_office,
            office_level,
            {{ map_ballotready_office_type("candidate_office") }} as office_type,
            state,
            city,
            district,
            start_at as term_start_date,
            end_at as term_end_date,
            is_current_term,
            appointed as is_appointed,
            is_judicial,
            is_vacant,
            is_off_cycle,
            party_affiliation,
            website_url,
            linkedin_url,
            facebook_url,
            twitter_url,
            nullif(office_holder_mailing_address_line_1, '') as mailing_address_line_1,
            nullif(office_holder_mailing_address_line_2, '') as mailing_address_line_2,
            nullif(office_holder_mailing_city, '') as mailing_city,
            nullif(office_holder_mailing_state, '') as mailing_state,
            nullif(office_holder_mailing_zip, '') as mailing_zip,
            br_geo_id,
            tier,
            'ballotready' as candidate_id_source,
            office_holder_created_at as created_at,
            office_holder_updated_at as updated_at
        from derived_fields
    )

select
    gp_elected_official_id,
    gp_candidate_id,
    br_office_holder_id,
    br_candidate_id,
    br_position_id,
    br_candidacy_id,
    first_name,
    last_name,
    middle_name,
    suffix,
    full_name,
    email,
    phone,
    office_phone,
    central_phone,
    position_name,
    normalized_position_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    term_start_date,
    term_end_date,
    is_current_term,
    is_appointed,
    is_judicial,
    is_vacant,
    is_off_cycle,
    party_affiliation,
    website_url,
    linkedin_url,
    facebook_url,
    twitter_url,
    mailing_address_line_1,
    mailing_address_line_2,
    mailing_city,
    mailing_state,
    mailing_zip,
    br_geo_id,
    tier,
    candidate_id_source,
    created_at,
    updated_at
from elected_officials
