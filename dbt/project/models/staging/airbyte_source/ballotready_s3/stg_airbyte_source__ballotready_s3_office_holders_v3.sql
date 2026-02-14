with
    source as (
        select * from {{ source("airbyte_source", "ballotready_s3_office_holders_v3") }}
    ),
    renamed as (
        select
            cast(
                {{ adapter.quote("_ab_source_file_last_modified") }} as timestamp
            ) as _ab_source_file_last_modified,
            {{ adapter.quote("_ab_source_file_url") }},
            cast(
                {{ adapter.quote("_airbyte_extracted_at") }} as timestamp
            ) as _airbyte_extracted_at,
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_raw_id") }},
            cast({{ adapter.quote("appointed") }} as boolean) as appointed,
            try_cast({{ adapter.quote("candidacy_id") }} as int) as br_candidacy_id,
            try_cast({{ adapter.quote("candidate_id") }} as int) as br_candidate_id,
            from_json(
                regexp_replace({{ adapter.quote("contacts") }}, '=>', ':'),
                'ARRAY<STRUCT<email: STRING, phone: STRING, type: STRING>>'
            ) as contacts,
            try_cast({{ adapter.quote("end_at") }} as date) as end_at,
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("geo_id") }} as br_geo_id,
            try_cast({{ adapter.quote("geofence_id") }} as int) as br_geofence_id,
            cast({{ adapter.quote("id") }} as int) as br_id,
            cast({{ adapter.quote("is_judicial") }} as boolean) as is_judicial,
            cast({{ adapter.quote("is_off_cycle") }} as boolean) as is_off_cycle,
            cast({{ adapter.quote("is_vacant") }} as boolean) as is_vacant,
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("level") }},
            {{ adapter.quote("middle_name") }},
            {{ adapter.quote("mtfcc") }},
            {{ adapter.quote("nickname") }},
            try_cast(
                {{ adapter.quote("normalized_position_id") }} as int
            ) as br_normalized_position_id,
            {{ adapter.quote("normalized_position_name") }},
            try_cast({{ adapter.quote("number_of_seats") }} as int) as number_of_seats,
            cast(
                {{ adapter.quote("office_holder_created_at") }} as timestamp
            ) as office_holder_created_at,
            try_cast(
                {{ adapter.quote("office_holder_id") }} as int
            ) as br_office_holder_id,
            {{ adapter.quote("office_holder_mailing_address_line_1") }},
            {{ adapter.quote("office_holder_mailing_address_line_2") }},
            {{ adapter.quote("office_holder_mailing_city") }},
            {{ adapter.quote("office_holder_mailing_state") }},
            {{ adapter.quote("office_holder_mailing_zip") }},
            cast(
                {{ adapter.quote("office_holder_updated_at") }} as timestamp
            ) as office_holder_updated_at,
            {{ adapter.quote("office_title") }},
            from_json(
                regexp_replace({{ adapter.quote("party_names") }}, '=>', ':'),
                'ARRAY<STRING>'
            ) as party_names,
            try_cast({{ adapter.quote("position_id") }} as int) as br_position_id,
            {{ adapter.quote("position_name") }},
            try_cast({{ adapter.quote("start_at") }} as date) as start_at,
            {{ adapter.quote("state") }},
            {{ adapter.quote("sub_area_name") }},
            {{ adapter.quote("sub_area_name_secondary") }},
            {{ adapter.quote("sub_area_value") }},
            {{ adapter.quote("sub_area_value_secondary") }},
            {{ adapter.quote("suffix") }},
            {{ adapter.quote("term_date_specificity") }},
            try_cast({{ adapter.quote("tier") }} as int) as tier,
            from_json(
                regexp_replace({{ adapter.quote("urls") }}, '=>', ':'),
                'ARRAY<STRUCT<url: STRING, type: STRING>>'
            ) as urls
        from source
    )
select *
from renamed
