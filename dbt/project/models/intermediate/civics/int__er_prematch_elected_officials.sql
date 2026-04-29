{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady+TechSpeed (deterministically joined) x gp-api
-- elected officials. Term grain on BR side; gp-api at elected-office grain.
--
-- Source 1: ballotready_techspeed (BR term LEFT JOIN canonical_ids LEFT JOIN raw TS,
-- with reused-ID guard inside the TS join clause to prevent row explosion).
-- Source 2: gp_api (with new ballotready_position_id, raw campaign_office_raw).
--
-- Spec: .tickets/data-1731/eo-prematch-rewrite-design-spec-v2.md (v2.2)
with
    nickname_aliases as (
        select
            name1, array_distinct(array_append(collect_list(name2), name1)) as aliases
        from {{ ref("nicknames") }}
        group by name1
    ),

    -- Source 1 build: BR term + crosswalk + raw TS (guard-in-join)
    br_term as (select * from {{ ref("int__civics_elected_official_ballotready") }}),
    crosswalk as (
        select * from {{ ref("int__civics_elected_official_canonical_ids") }}
    ),
    ts_raw as (select * from {{ ref("int__civics_elected_official_techspeed") }}),
    br_with_ts as (
        select
            br.*,
            cx.ts_officeholder_id as cx_ts_officeholder_id,
            cx.ts_officeholder_id_is_reused,
            ts.phone as ts_phone,
            ts.email as ts_email,
            ts.ts_position_id  -- raw TS intermediate exposes ts_position_id (renamed from position_id)
        from br_term as br
        left join crosswalk as cx on br.br_office_holder_id = cx.br_office_holder_id
        left join
            ts_raw as ts
            on cx.ts_officeholder_id = ts.ts_officeholder_id
            and not coalesce(cx.ts_officeholder_id_is_reused, false)  -- v2.2 Blocker fix: in-join guard
    ),

    ballotready_techspeed as (
        select
            'ballotready_techspeed' as source_name,
            cast(b.br_office_holder_id as string) as source_id,
            lower(trim(b.first_name)) as first_name,
            lower(trim(b.last_name)) as last_name,
            b.state,
            b.party_affiliation as party,
            b.candidate_office,
            b.office_level,
            b.office_type,
            {{ extract_district_raw("b.position_name") }} as district_raw,
            {{ extract_district_identifier("b.position_name") }} as district_identifier,
            -- TS-wins coalesce; ts_phone/ts_email are null for reused-ID rows by
            -- construction (in-join guard)
            coalesce(b.ts_email, b.email) as email,
            -- Phones from upstream are already cleaned; re-cleaning is idempotent.
            {{ clean_phone_number("coalesce(b.ts_phone, b.phone)") }} as phone,
            b.position_name as official_office_name,
            b.city,
            b.term_start_date,
            b.term_end_date,
            -- v2.2: ballotready_position_id from BR side
            b.br_position_id as ballotready_position_id,
            -- ICP flags (term grain)
            b.is_win_icp,
            b.is_serve_icp,
            b.is_win_supersize_icp,
            b.br_office_holder_id,
            b.br_candidate_id,
            b.cx_ts_officeholder_id as ts_officeholder_id,
            b.ts_position_id,
            cast(null as bigint) as gp_api_user_id,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as string) as gp_api_elected_office_id,
            cast(null as string) as gp_api_organization_slug
        from br_with_ts as b
        where
            not coalesce(b.is_vacant, false)
            and b.br_candidate_id is not null
            and nullif(trim(b.first_name), '') is not null
            and nullif(trim(b.last_name), '') is not null
            and b.state != 'US'
    ),

    gp_api as (
        select
            'gp_api' as source_name,
            cast(g.gp_api_elected_office_id as string) as source_id,
            lower(trim(g.first_name)) as first_name,
            lower(trim(g.last_name)) as last_name,
            g.state,
            -- party_affiliation already derived in the gp-api intermediate via
            -- parse_party_affiliation
            g.party_affiliation as party,
            g.candidate_office,
            g.office_level,
            g.office_type,
            -- v2.2: extract district from RAW campaign_office_raw, not normalized
            -- position_name
            {{ extract_district_raw("g.campaign_office_raw") }} as district_raw,
            {{ extract_district_identifier("g.campaign_office_raw") }}
            as district_identifier,
            g.email,
            -- gp-api phone is already cleaned by clean_phone_number in the
            -- intermediate; pass through
            g.phone,
            -- v2.2: official_office_name from RAW text (preserves seat/district
            -- context)
            trim(g.campaign_office_raw) as official_office_name,
            cast(null as string) as city,
            g.term_start_date,
            cast(null as date) as term_end_date,
            g.ballotready_position_id,
            cast(null as boolean) as is_win_icp,
            cast(null as boolean) as is_serve_icp,
            cast(null as boolean) as is_win_supersize_icp,
            cast(null as int) as br_office_holder_id,
            cast(null as int) as br_candidate_id,
            cast(null as string) as ts_officeholder_id,
            cast(null as string) as ts_position_id,
            g.gp_api_user_id,
            g.gp_api_campaign_id,
            g.gp_api_elected_office_id,
            g.gp_api_organization_slug
        from {{ ref("int__civics_elected_official_gp_api") }} as g
        where
            nullif(trim(g.first_name), '') is not null
            and nullif(trim(g.last_name), '') is not null
            and g.state is not null
            -- v2.2: filter null campaign_office at the prematch level (mirrors
            -- candidacy line 266).
            and nullif(trim(g.campaign_office_raw), '') is not null
    ),

    unioned as (
        select *
        from ballotready_techspeed
        union all
        select *
        from gp_api
    )

-- Splink treats empty strings as real values for exact matching, so convert
-- sparse columns to NULL where empty so missing data is handled correctly.
select
    source_name || '|' || source_id as unique_id,
    source_id,
    u.source_name,
    u.first_name,
    u.last_name,
    -- Array of first_name + all known nicknames for Splink ArrayIntersectLevel
    coalesce(na.aliases, array(u.first_name)) as first_name_aliases,
    u.state,
    party,
    candidate_office,
    office_level,
    office_type,
    nullif(district_raw, '') as district_raw,
    district_identifier,
    nullif(email, '') as email,
    nullif(phone, '') as phone,
    nullif(lower(trim(official_office_name)), '') as official_office_name,
    nullif(city, '') as city,
    term_start_date,
    term_end_date,
    ballotready_position_id,
    is_win_icp,
    is_serve_icp,
    is_win_supersize_icp,
    br_office_holder_id,
    br_candidate_id,
    ts_officeholder_id,
    ts_position_id,
    gp_api_user_id,
    gp_api_campaign_id,
    gp_api_elected_office_id,
    gp_api_organization_slug
from unioned as u
left join nickname_aliases as na on u.first_name = na.name1
