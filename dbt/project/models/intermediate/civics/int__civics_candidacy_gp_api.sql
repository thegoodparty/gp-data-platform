{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns -> Civics mart candidacy schema.
-- Grain: one row per latest-version campaign with an election_date.
-- Schema aligns with int__civics_candidacy_ballotready for the union.
with
    latest_campaigns as (
        -- 2026+ scope: pre-2026 campaigns belong to the archive_2025 path
        -- (from int__civics_candidacy_2025) and would duplicate at
        -- (product_campaign_id, general_election_date) grain in the candidacy
        -- mart since the two sources hash gp_candidacy_id differently
        -- (HubSpot identity vs gp_api person+race hash).
        select *
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and not coalesce(is_demo, false)
            and ballotready_position_id is not null
            and election_date >= '2026-01-01'
    ),

    -- Source user fields from the users mart, not campaigns' denormalized
    -- user_* fields, to keep the person hash aligned with candidate_gp_api
    -- (the two marts can be built at different times).
    users as (
        select user_id, first_name, last_name, email, phone from {{ ref("users") }}
    ),

    -- Must match user_state in int__civics_candidate_gp_api.
    user_state as (
        -- latest_campaigns is already filtered to non-demo / 2026+ / BR-anchored;
        -- mirror is in int__civics_candidate_gp_api so both models hash
        -- gp_candidate_id over the same campaign universe.
        select user_id, campaign_state as state
        from latest_campaigns
        qualify row_number() over (partition by user_id order by created_at desc) = 1
    ),

    -- All stages of a campaign in the crosswalk resolve to the same
    -- canonical_gp_candidacy_id / canonical_gp_election_id (the inner joins
    -- bottom out on br_cs.gp_candidacy_id), so any non-null wins.
    er_canonical as (
        -- max() not any_value(): deterministic across runs.
        select
            gp_api_campaign_id,
            max(canonical_gp_candidacy_id) as canonical_gp_candidacy_id,
            max(canonical_gp_election_id) as canonical_gp_election_id,
            max(canonical_gp_candidate_id) as canonical_gp_candidate_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- BR stage spine lookup: maps (br_position_id, election_year) →
    -- BR's gp_election_id and all 4 stage dates. Every surviving gp_api
    -- campaign has ballotready_position_id (Task 1 filter), so this lookup
    -- resolves for all gp_api campaigns. The candidacy row reflects the
    -- full election cycle (all calendar stages BR knows about); per-stage
    -- match to a specific PD pledge happens in
    -- int__civics_candidacy_stage_gp_api via exact-date join.
    br_stage_spine_lookup as (
        select
            br_position_id,
            year(election_date) as election_year,
            any_value(gp_election_id) as gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as general_election_date,
            max(
                case when stage_type = 'primary runoff' then election_date end
            ) as primary_runoff_election_date,
            max(
                case when stage_type = 'general runoff' then election_date end
            ) as general_runoff_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by br_position_id, year(election_date)
    ),

    -- Must mirror user_er_canonical in int__civics_candidate_gp_api so
    -- candidacy.gp_candidate_id matches candidate.gp_candidate_id.
    user_er_canonical as (
        select c.user_id, max(xw.canonical_gp_candidate_id) as canonical_gp_candidate_id
        from latest_campaigns as c
        inner join
            {{ ref("int__civics_er_canonical_ids") }} as xw
            on c.campaign_id = xw.gp_api_campaign_id
        group by c.user_id
    ),

    enriched as (
        -- Aliases below match the unprefixed column names that
        -- generate_gp_election_id() expects in scope.
        select
            c.campaign_id,
            c.user_id,
            u.first_name as user_first_name,
            u.last_name as user_last_name,
            u.email as user_email,
            u.phone as user_phone,
            c.hubspot_id,
            c.is_verified,
            c.is_pledged,
            c.is_demo,
            c.did_win,
            c.ballotready_position_id,
            c.created_at,
            c.updated_at,
            us.state as user_state,
            c.campaign_state as state,
            c.normalized_position_name as official_office_name,
            c.campaign_office as candidate_office,
            nullif(c.election_level, '') as office_level,
            {{ map_office_type("c.campaign_office") }} as office_type,
            {{ extract_city_from_office_name("c.normalized_position_name") }} as city,
            {{ extract_district_geographic("c.normalized_position_name") }} as district,
            cast(null as string) as seat_name,
            cast(null as int) as seats_available,
            c.election_date,
            c.election_date as general_election_date,
            {{ parse_party_affiliation("c.campaign_party") }} as party_affiliation,
            c.partisan_type,
            uec.canonical_gp_candidate_id as user_canonical_gp_candidate_id
        from latest_campaigns as c
        inner join users as u on c.user_id = u.user_id
        left join user_state as us on c.user_id = us.user_id
        left join user_er_canonical as uec on c.user_id = uec.user_id
    ),

    candidacies_with_ids as (
        select
            -- Salt field order matches int__civics_candidacy_ballotready.
            coalesce(
                xw.canonical_gp_candidacy_id,
                {{
                    generate_gp_api_gp_candidacy_id(
                        first_name="user_first_name",
                        last_name="user_last_name",
                        general_election_date="enriched.general_election_date",
                    )
                }}
            ) as gp_candidacy_id,

            -- Window over the person hash matches the cross-user cascade in
            -- candidate_gp_api, so hash-collision users share canonical IDs.
            coalesce(
                max(user_canonical_gp_candidate_id) over (
                    partition by
                        {{
                            generate_gp_api_gp_candidate_id(
                                first_name="user_first_name",
                                last_name="user_last_name",
                                state="user_state",
                                email="user_email",
                                phone="user_phone",
                            )
                        }}
                ),
                {{
                    generate_gp_api_gp_candidate_id(
                        first_name="user_first_name",
                        last_name="user_last_name",
                        state="user_state",
                        email="user_email",
                        phone="user_phone",
                    )
                }}
            ) as gp_candidate_id,

            -- Prefer BR's natural gp_election_id (via br_stage_spine_lookup)
            -- over the canonical_ids non-BR-cluster derivation: every gp_api
            -- campaign has ballotready_position_id (Task 1 filter), so
            -- bss.gp_election_id is always populated and aligns with BR's
            -- election mart row. The non-BR cluster canonical would create a
            -- separate election row that shares br_position_database_id with
            -- BR's natural row, fanning out downstream joins (e.g.,
            -- m_election_api__zip_to_position).
            coalesce(
                bss.gp_election_id,
                xw.canonical_gp_election_id,
                {{ generate_gp_election_id() }}
            ) as gp_election_id,

            campaign_id as product_campaign_id,
            hubspot_id as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,
            'gp_api' as candidate_id_source,
            party_affiliation,
            cast(null as boolean) as is_incumbent,
            cast(null as boolean) as is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            case when did_win then 'Won' else null end as candidacy_result,
            is_pledged,
            is_verified,
            cast(null as string) as verification_status_reason,
            -- Race-level, matching BR (br_position.is_partisan) and TS (ts.partisan).
            case
                when lower(partisan_type) = 'nonpartisan'
                then false
                when nullif(trim(partisan_type), '') is null
                then null
                else true
            end as is_partisan,
            -- All 4 stage dates are sourced from BR's spine via the
            -- (br_position_id, election_year) join. Replaces the prior
            -- pattern of carrying campaign.election_date as general only
            -- and leaving primary / runoff dates NULL. general_election_date
            -- falls back to the campaign's own date when BR's spine has
            -- no general stage for the race (rare — e.g., judicial-only
            -- races) so the not_null guarantee holds.
            bss.primary_election_date,
            bss.primary_runoff_election_date,
            coalesce(
                bss.general_election_date, enriched.general_election_date
            ) as general_election_date,
            bss.general_runoff_election_date,
            ballotready_position_id as br_position_database_id,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as br_race_id,
            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,
            is_demo,
            created_at,
            updated_at
        from enriched
        left join er_canonical as xw on enriched.campaign_id = xw.gp_api_campaign_id
        left join
            br_stage_spine_lookup as bss
            on enriched.ballotready_position_id = bss.br_position_id
            and year(enriched.general_election_date) = bss.election_year
        where enriched.general_election_date is not null
    ),

    -- Referential integrity: drop candidacies whose gp_candidate_id doesn't
    -- resolve (e.g. user filtered out by campaign_count > 0).
    valid_candidates as (
        select gp_candidate_id from {{ ref("int__civics_candidate_gp_api") }}
    ),

    filtered as (
        select candidacies_with_ids.*
        from candidacies_with_ids
        inner join
            valid_candidates
            on candidacies_with_ids.gp_candidate_id = valid_candidates.gp_candidate_id
    ),

    deduplicated as (
        select *
        from filtered
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    gp_candidacy_id,
    gp_candidate_id,
    gp_election_id,
    product_campaign_id,
    hubspot_contact_id,
    hubspot_company_ids,
    candidate_id_source,
    party_affiliation,
    is_incumbent,
    is_open_seat,
    candidate_office,
    official_office_name,
    office_level,
    office_type,
    candidacy_result,
    is_pledged,
    is_verified,
    verification_status_reason,
    is_partisan,
    primary_election_date,
    primary_runoff_election_date,
    general_election_date,
    general_runoff_election_date,
    br_position_database_id,
    br_candidacy_id,
    br_race_id,
    viability_score,
    win_number,
    win_number_model,
    is_demo,
    created_at,
    updated_at
from deduplicated
