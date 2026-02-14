with
    source as (
        select *
        from {{ source("historical", "ballotready_records_sent_to_techspeed") }}
    ),
    renamed as (
        select
            {{ adapter.quote("id") }} as br_id,
            {{ adapter.quote("candidacy_id") }} as br_candidacy_id,
            {{ adapter.quote("election_id") }} as br_election_id,
            {{ adapter.quote("election_name") }},
            {{ adapter.quote("election_day") }},
            {{ adapter.quote("position_id") }} as br_position_id,
            {{ adapter.quote("mtfcc") }},
            {{ adapter.quote("geo_id") }} as br_geo_id,
            {{ adapter.quote("position_name") }},
            {{ adapter.quote("sub_area_name") }},
            {{ adapter.quote("sub_area_value") }},
            {{ adapter.quote("sub_area_name_secondary") }},
            {{ adapter.quote("sub_area_value_secondary") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("level") }},
            {{ adapter.quote("tier") }},
            {{ adapter.quote("is_judicial") }},
            {{ adapter.quote("is_retention") }},
            {{ adapter.quote("number_of_seats") }},
            {{ adapter.quote("normalized_position_id") }} as br_normalized_position_id,
            {{ adapter.quote("normalized_position_name") }},
            {{ adapter.quote("race_id") }} as br_race_id,
            {{ adapter.quote("geofence_id") }} as br_geofence_id,
            {{ adapter.quote("geofence_is_not_exact") }},
            {{ adapter.quote("is_primary") }},
            {{ adapter.quote("is_runoff") }},
            {{ adapter.quote("is_unexpired") }},
            {{ adapter.quote("candidate_id") }} as br_candidate_id,
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("middle_name") }},
            {{ adapter.quote("nickname") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("suffix") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("image_url") }},
            {{ adapter.quote("parties") }},
            {{ adapter.quote("urls") }},
            {{ adapter.quote("election_result") }},
            {{ adapter.quote("candidacy_created_at") }},
            {{ adapter.quote("candidacy_updated_at") }},
            {{ adapter.quote("upload_datetime") }}

        from source
    )
select *
from renamed
