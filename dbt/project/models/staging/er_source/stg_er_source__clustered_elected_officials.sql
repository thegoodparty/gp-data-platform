with

    source as (select * from {{ source("er_source", "clustered_elected_officials") }}),

    renamed as (

        select
            cluster_id,
            unique_id,
            source_id,
            source_name,
            first_name,
            last_name,
            first_name_aliases,
            state,
            party,
            candidate_office,
            office_level,
            office_type,
            district_raw,
            cast(district_identifier as int) as district_identifier,
            email,
            phone,
            official_office_name,
            city,
            cast(term_start_date as date) as term_start_date,
            cast(term_end_date as date) as term_end_date,
            cast(br_office_holder_id as int) as br_office_holder_id,
            cast(br_candidate_id as int) as br_candidate_id,
            ts_officeholder_id,
            ts_position_id

        from source

    )

select *
from renamed
