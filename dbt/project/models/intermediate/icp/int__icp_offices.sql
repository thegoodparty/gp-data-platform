with
    position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    normalized_position as (
        select * from {{ ref("int__ballotready_normalized_position") }}
    ),

    l2_match as (
        -- the allocation normalizes district names (case, whitespace, trailing
        -- "(EST.)"); the match snapshot does not, so carry a normalized copy for
        -- the population join and the voter-count fallback below.
        select
            *,
            {{ normalize_l2_district_name("l2_district_name") }}
            as normalized_district_name
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
    ),

    -- aliased on both lookups: a bare voter_count on either would shadow the
    -- coalesced select alias the gates read, silently gating on the exact name only.
    district_counts as (
        select
            state_postal_code,
            district_type,
            district_name,
            voter_count as voter_count_exact
        from {{ ref("int__l2_district_aggregations") }}
    ),

    -- Fallback keyed on the normalized name: the match snapshot carries L2's
    -- "(EST.)" and whitespace drift, so the exact join above misses and leaves the
    -- position unsized with null ICP gates.
    --
    -- max() not sum(): 7 keys nationwide have two raw spellings of one district, so
    -- summing double counts its voters. Deduping also keeps this join 1:1.
    district_counts_normalized as (
        select
            state_postal_code,
            district_type,
            {{ normalize_l2_district_name("district_name") }} as district_name,
            max(voter_count) as voter_count_normalized
        from {{ ref("int__l2_district_aggregations") }}
        group by
            state_postal_code,
            district_type,
            {{ normalize_l2_district_name("district_name") }}
    ),

    district_pop as (select * from {{ ref("int__district_population") }}),

    icp_position_names as (
        select name, serve_eligible, win_effective_date
        from {{ ref("icp_normalized_position_names") }}
    )

select
    position.database_id as br_database_position_id,
    position.id as br_position_id,
    position.state,
    position.name as br_position_name,
    normalized_position.name as normalized_position_type,
    l2_match.l2_district_name,
    l2_match.l2_district_type,
    l2_match.is_matched,
    -- exact name first, normalized only as a fallback, so no already-sized position
    -- changes value
    coalesce(
        district_counts.voter_count_exact,
        district_counts_normalized.voter_count_normalized
    ) as voter_count,
    -- census constituents for the same district. Carried for sizing and market
    -- analysis; no ICP gate reads it yet. Null where the district's type is
    -- outside the allocation's curated type set, so it is not a
    -- drop-in replacement for voter_count today.
    district_pop.district_population,
    position.is_judicial,
    position.is_appointed,
    position.updated_at,

    case
        when
            position.is_judicial
            or position.is_appointed
            or icp_position_names.name is null
        then false
        when voter_count is null
        then null
        when voter_count not between 500 and 100000
        then false
        else true
    end as icp_office_win,

    case
        when
            position.is_judicial
            or position.is_appointed
            or icp_position_names.name is null
            or not icp_position_names.serve_eligible
        then false
        when voter_count is null
        then null
        when voter_count not between 1000 and 100000
        then false
        else true
    end as icp_office_serve,

    -- Large offices (>100K voters) that meet all other ICP criteria
    -- These need separate consideration outside the standard Win process
    case
        when
            position.is_judicial
            or position.is_appointed
            or icp_position_names.name is null
        then false
        when voter_count is null
        then null
        when voter_count <= 100000
        then false
        else true
    end as icp_win_supersize,

    cast(icp_position_names.win_effective_date as date) as icp_win_effective_date

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

left join
    district_counts_normalized
    on l2_match.normalized_district_name = district_counts_normalized.district_name
    and l2_match.l2_district_type = district_counts_normalized.district_type
    and position.state = district_counts_normalized.state_postal_code

left join
    district_pop
    on l2_match.normalized_district_name = district_pop.district_name
    and l2_match.l2_district_type = district_pop.district_type
    and position.state = district_pop.state_postal_code

left join icp_position_names on normalized_position.name = icp_position_names.name
