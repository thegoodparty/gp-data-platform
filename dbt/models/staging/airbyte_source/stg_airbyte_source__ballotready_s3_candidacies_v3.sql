with 

source as (

    select * from {{ source('airbyte_source', 'ballotready_s3_candidacies_v3') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        _airbyte_generation_id,
        id,
        tier,
        urls,
        email,
        level,
        mtfcc,
        phone,
        state,
        geo_id,
        suffix,
        parties,
        race_id,
        nickname,
        image_url,
        is_runoff,
        last_name,
        first_name,
        is_primary,
        election_id,
        geofence_id,
        is_judicial,
        middle_name,
        position_id,
        candidacy_id,
        candidate_id,
        election_day,
        is_retention,
        is_unexpired,
        election_name,
        position_name,
        sub_area_name,
        sub_area_value,
        election_result,
        number_of_seats,
        _ab_source_file_url,
        candidacy_created_at,
        candidacy_updated_at,
        geofence_is_not_exact,
        normalized_position_id,
        sub_area_name_secondary,
        normalized_position_name,
        sub_area_value_secondary,
        _ab_source_file_last_modified

    from source

)

select * from renamed
