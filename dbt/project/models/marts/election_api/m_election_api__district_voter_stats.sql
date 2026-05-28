{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "district_voter_stats"],
    )
}}

-- District-level voter aggregates that feed election-api's DistrictVoterStats
-- table (one row per District). The campaign-strategy / Race API uses these
-- via Race -> Position -> District -> DistrictVoterStats. Counts come from
-- int__l2_district_aggregations, which dedupes voters per district by
-- lalvoterid and segments by phone presence. Joined to m_election_api__district
-- on the salted id (both models hash the same (state, district_type,
-- district_name) tuple with the default salt, so the ids match).
select
    {{ generate_salted_uuid(fields=["tbl_district.id"], salt="district_voter_stats") }}
    as id,
    tbl_district.id as district_id,
    tbl_agg.voter_count as registered_voters,
    tbl_agg.unique_cellphones,
    tbl_agg.unique_landlines,
    tbl_agg.loaded_at as updated_at
from {{ ref("m_election_api__district") }} as tbl_district
inner join
    {{ ref("int__l2_district_aggregations") }} as tbl_agg
    on tbl_district.id = tbl_agg.id
