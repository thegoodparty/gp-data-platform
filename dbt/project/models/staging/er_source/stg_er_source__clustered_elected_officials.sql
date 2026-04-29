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
            cast(ballotready_position_id as bigint) as ballotready_position_id,
            cast(is_win_icp as boolean) as is_win_icp,
            cast(is_serve_icp as boolean) as is_serve_icp,
            cast(is_win_supersize_icp as boolean) as is_win_supersize_icp,
            cast(br_office_holder_id as int) as br_office_holder_id,
            cast(br_candidate_id as int) as br_candidate_id,
            ts_officeholder_id,
            ts_position_id,
            cast(gp_api_user_id as bigint) as gp_api_user_id,
            cast(gp_api_campaign_id as bigint) as gp_api_campaign_id,
            gp_api_elected_office_id,
            gp_api_organization_slug

        from source

    )

select *
from renamed
