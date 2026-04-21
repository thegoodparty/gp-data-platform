with

    source as (select * from {{ source("er_source", "clustered_candidacy_stages") }}),

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
            cast(election_date as date) as election_date,
            election_stage,
            email,
            phone,
            cast(br_race_id as int) as br_race_id,
            official_office_name,
            cast(br_candidacy_id as string) as br_candidacy_id,
            seat_name,
            partisan_type

        from source

    )

select *
from renamed
