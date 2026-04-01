with
    source as (
        select * from {{ source("airbyte_source", "ballotready_s3_recruitment_v1") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }} as br_id,
            {{ adapter.quote("tier") }},
            {{ adapter.quote("level") }},
            {{ adapter.quote("mtfcc") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("geo_id") }} as br_geo_id,
            {{ adapter.quote("salary") }},
            {{ adapter.quote("race_id") }} as br_race_id,
            {{ adapter.quote("frequency") }},
            {{ adapter.quote("is_runoff") }},
            {{ adapter.quote("is_primary") }},
            {{ adapter.quote("election_id") }} as br_election_id,
            {{ adapter.quote("geofence_id") }} as br_geofence_id,
            {{ adapter.quote("is_judicial") }},
            {{ adapter.quote("position_id") }} as br_position_id,
            {{ adapter.quote("election_day") }},
            {{ adapter.quote("is_retention") }},
            {{ adapter.quote("is_unexpired") }},
            {{ adapter.quote("election_name") }},
            {{ adapter.quote("partisan_type") }},
            {{ adapter.quote("position_name") }},
            {{ adapter.quote("sub_area_name") }},
            {{ adapter.quote("filing_periods") }},
            {{ adapter.quote("reference_year") }},
            {{ adapter.quote("sub_area_value") }},
            {{ adapter.quote("employment_type") }},
            {{ adapter.quote("number_of_seats") }},
            {{ adapter.quote("race_created_at") }},
            {{ adapter.quote("race_updated_at") }},
            {{ adapter.quote("_ab_source_file_url") }},
            {{ adapter.quote("filing_phone_number") }},
            {{ adapter.quote("filing_requirements") }},
            {{ adapter.quote("has_blanket_primary") }},
            {{ adapter.quote("position_description") }},
            {{ adapter.quote("filing_office_address") }},
            {{ adapter.quote("normalized_position_id") }} as br_normalized_position_id,
            {{ adapter.quote("paperwork_instructions") }},
            {{ adapter.quote("sub_area_name_secondary") }},
            {{ adapter.quote("eligibility_requirements") }},
            {{ adapter.quote("normalized_position_name") }},
            {{ adapter.quote("sub_area_value_secondary") }},
            {{ adapter.quote("has_majority_vote_primary") }},
            {{ adapter.quote("_ab_source_file_last_modified") }}

        from source
    )
select *
from renamed
