with
    position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    normalized_position as (
        select * from {{ ref("int__ballotready_normalized_position") }}
    ),

    l2_match as (
        select * from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
    ),

    district_counts as (select * from {{ ref("int__l2_district_aggregations") }})

select
    position.database_id as br_database_position_id,
    position.id as br_position_id,
    position.state,
    position.name as br_position_name,
    normalized_position.name as normalized_position_type,
    l2_match.l2_district_name,
    l2_match.l2_district_type,
    l2_match.is_matched,
    district_counts.voter_count,
    position.judicial,
    position.appointed,
    position.updated_at,

    -- ICP-Office-Win Flag
    case
        when
            position.judicial = true
            or position.appointed = true
            or normalized_position.name
            not in ({{ get_icp_office_normalized_position_names() }})
        then false
        when district_counts.voter_count is null
        then null
        when district_counts.voter_count not between 500 and 50000
        then false
        else true
    end as icp_office_win,

    -- ICP-Office-Serve Flag
    case
        when
            position.judicial = true
            or position.appointed = true
            or normalized_position.name
            not in ({{ get_icp_office_normalized_position_names() }})
        then false
        when district_counts.voter_count is null
        then null
        when district_counts.voter_count not between 1000 and 100000
        then false
        else true
    end as icp_office_serve

from position

left join
    normalized_position
    on position.normalized_position.`databaseId` = normalized_position.database_id

left join l2_match on position.database_id = l2_match.br_database_id

left join
    district_counts
    on l2_match.l2_district_name = district_counts.district_name
    and l2_match.l2_district_type = district_counts.district_type
    and position.state = district_counts.state_postal_code
