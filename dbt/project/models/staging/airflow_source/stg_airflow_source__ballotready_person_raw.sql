{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_person_raw") }}

select
    requested_id,
    loaded_at,
    get_json_object(payload, '$.bioText') as bio_text,
    {{ br_id_ref_array("$.candidacies") }} as candidacies,
    from_json(
        get_json_object(payload, '$.contacts'),
        'array<struct<email:string,fax:string,phone:string,type:string>>'
    ) as contacts,
    cast(get_json_object(payload, '$.createdAt') as timestamp) as created_at,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    from_json(
        get_json_object(payload, '$.degrees'),
        'array<struct<databaseId:int,degree:string,gradYear:int,id:string,major:string,school:string>>'
    ) as degrees,
    -- `end` is a reserved word, so the struct field needs backticks even though the
    -- surrounding literal is a string.
    from_json(
        get_json_object(payload, '$.experiences'),
        'array<struct<databaseId:int,`end`:string,id:string,organization:string,start:string,title:string,type:string>>'
    ) as experiences,
    get_json_object(payload, '$.firstName') as first_name,
    get_json_object(payload, '$.fullName') as full_name,
    get_json_object(payload, '$.id') as id,
    from_json(
        get_json_object(payload, '$.images'), 'array<struct<type:string,url:string>>'
    ) as images,
    get_json_object(payload, '$.lastName') as last_name,
    get_json_object(payload, '$.middleName') as middle_name,
    get_json_object(payload, '$.nickname') as nickname,
    {{ br_id_ref_array("$.officeHolders") }} as office_holders,
    get_json_object(payload, '$.slug') as slug,
    get_json_object(payload, '$.suffix') as suffix,
    cast(get_json_object(payload, '$.updatedAt') as timestamp) as updated_at,
    from_json(
        get_json_object(payload, '$.urls'),
        'array<struct<databaseId:int,id:string,type:string,url:string>>'
    ) as urls,
    loaded_at as feed_extracted_at
from current_rows
