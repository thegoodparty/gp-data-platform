{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB campaigns -> Civics mart candidacy schema.
--
-- Grain: One row per latest-version campaign with an election_date.
--
-- Schema aligned with int__civics_candidacy_ballotready so the downstream mart
-- union can stack this source directly. Fields not tracked in Product DB
-- (incumbency, open seat, verification reason, runoff dates, viability,
-- win number) are null.
--
-- gp_candidate_id is computed against the *user's* state (most recent non-demo
-- campaign's campaign_state), matching int__civics_candidate_gp_api's hash.
-- This keeps the referential integrity INNER JOIN to candidate_gp_api intact
-- even when a single user runs in multiple states.
with
    latest_campaigns as (select * from {{ ref("campaigns") }} where is_latest_version),

    -- User fields are sourced from the users mart to match
    -- int__civics_candidate_gp_api's source exactly. The campaigns mart
    -- denormalizes user_first_name/user_last_name/user_email/user_phone by
    -- joining to staging at campaigns-mart build time, which can drift from
    -- the users mart's snapshot and produce inconsistent person hashes
    -- between candidate and candidacy (observed for users whose name changed
    -- between mart builds).
    users as (
        select user_id, first_name, last_name, email, phone from {{ ref("users") }}
    ),

    -- Per-user state: most recent non-demo campaign's state (matches the
    -- derivation in int__civics_candidate_gp_api, required for gp_candidate_id
    -- consistency across models).
    user_state as (
        select user_id, campaign_state as state
        from latest_campaigns
        where not is_demo
        qualify row_number() over (partition by user_id order by created_at desc) = 1
    ),

    -- ER canonical IDs at candidacy grain. A campaign may appear on multiple
    -- stage rows in the crosswalk, but all stages of a campaign resolve to the
    -- same canonical_gp_candidacy_id / canonical_gp_election_id (verified: the
    -- crosswalk's inner joins bottom out on br_cs.gp_candidacy_id).
    er_canonical as (
        -- max() is deterministic across runs (any_value() is explicitly
        -- non-deterministic in Spark). Keeps this collapse stable with the
        -- sibling CTE in int__civics_candidacy_stage_gp_api if a campaign's
        -- stages ever resolve to multiple canonical_gp_candidacy_ids.
        select
            gp_api_campaign_id,
            max(canonical_gp_candidacy_id) as canonical_gp_candidacy_id,
            max(canonical_gp_election_id) as canonical_gp_election_id,
            max(canonical_gp_candidate_id) as canonical_gp_candidate_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where gp_api_campaign_id is not null
        group by gp_api_campaign_id
    ),

    -- Per-user canonical_gp_candidate_id lookup: any ER match across any of
    -- the user's campaigns. Must mirror int__civics_candidate_gp_api's
    -- user_er_canonical CTE so candidacy.gp_candidate_id matches
    -- candidate.gp_candidate_id.
    user_er_canonical as (
        select c.user_id, max(xw.canonical_gp_candidate_id) as canonical_gp_candidate_id
        from latest_campaigns as c
        inner join
            {{ ref("int__civics_er_canonical_ids") }} as xw
            on c.campaign_id = xw.gp_api_campaign_id
        group by c.user_id
    ),

    enriched as (
        -- Expose columns with the names generate_gp_election_id() expects in
        -- scope: official_office_name, candidate_office, office_level,
        -- office_type, state, city, district, seat_name, election_date,
        -- seats_available.
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
                        first_name="user_first_name", last_name="user_last_name"
                    )
                }}
            ) as gp_candidacy_id,

            -- Window propagation over the person hash (matches candidate model's
            -- cross-user cascade), so campaigns under hash-collision users adopt
            -- the same canonical_gp_candidate_id the candidate row resolves to.
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

            -- gp_election_id: salted UUID over (official_office_name, ..., state,
            -- ..., election_date, seats_available).
            -- Prefer ER canonical when available.
            coalesce(
                xw.canonical_gp_election_id, {{ generate_gp_election_id() }}
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
            -- Race-level, from Product DB's details:partisanType. Values in
            -- source: 'nonpartisan', 'partisan', 'partisan for primary...'
            -- plus null. Matches the race-level semantics of BR
            -- (br_position.is_partisan) and TS (ts.partisan).
            case
                when lower(partisan_type) = 'nonpartisan'
                then false
                when nullif(trim(partisan_type), '') is null
                then null
                else true
            end as is_partisan,
            cast(null as date) as primary_election_date,
            cast(null as date) as primary_runoff_election_date,
            general_election_date,
            cast(null as date) as general_runoff_election_date,
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
        where general_election_date is not null
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
