with

    source as (select * from {{ source("er_source", "clustered_election_stages") }}),

    renamed as (

        select
            cluster_id,
            unique_id,
            source_id,
            source_name,
            state,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            district_raw,
            cast(district_identifier as int) as district_identifier,
            cast(election_date as date) as election_date,
            election_stage,
            is_special,
            cast(ballotready_position_id as bigint) as ballotready_position_id,
            cast(br_race_id as int) as br_race_id,
            seat_name

        from source

    )

select *
from renamed
