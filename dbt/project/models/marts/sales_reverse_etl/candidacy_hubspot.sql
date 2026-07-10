-- Civics -> HubSpot lead feed (reverse-ETL mart `candidacy_hubspot`). One row per
-- gp_candidacy_id regardless of whether BallotReady, TechSpeed, DDHQ, or gp_api
-- identified the candidacy, replacing the per-provider legacy feeds. Output is the
-- Title-Case HubSpot import contract with owner/Type set for all rows. 30-day rolling
-- window on greatest(created_at, updated_at).
with
    icp_ts_offices as (
        select
            explode(
                array(
                    'city council',
                    'city commission',
                    'city commissioner',
                    'alderman',
                    'alderperson',
                    'village board',
                    'village council',
                    'village trustee',
                    'village councill',
                    'village president',
                    'municipal assembly',
                    'metro council',
                    'legislative council',
                    'city legislative council',
                    'mayor',
                    'town council',
                    'town board',
                    'town commission',
                    'town commissioner',
                    'town trustee',
                    'town board member',
                    'township board',
                    'town board chairperson',
                    'state legislature',
                    'county council',
                    'county commission',
                    'county commissioner',
                    'county legislator',
                    'board of supervisor',
                    'board of supervisors',
                    'board of commissioner',
                    'county committee',
                    'township supervisor',
                    'town supervisor',
                    'county supervisor',
                    'county executive',
                    'city council president',
                    'city president',
                    'town select board',
                    'town selectmen',
                    'town selectperson',
                    'town selectboard',
                    'town meeting representative',
                    'city ward moderator',
                    'town moderator'
                )
            ) as ts_office
    ),

    civics_base as (
        select
            cy.gp_candidacy_id,
            cy.gp_candidate_id,
            cy.candidate_id_source,
            cy.candidate_office,
            cy.official_office_name,
            cy.office_level,
            cy.office_type,
            cy.party_affiliation,
            cy.is_partisan,
            cy.is_open_seat,
            cy.is_incumbent,
            cy.is_win_icp,
            cy.is_serve_icp,
            cy.is_win_supersize_icp,
            cy.primary_election_date,
            cy.general_election_date,
            cy.viability_score,
            cy.score_viability_automated,
            cy.source_systems,
            cy.created_at,
            cy.updated_at,
            c.candidate_id_tier,
            c.first_name,
            c.last_name,
            c.state,
            c.email,
            c.phone_number,
            c.birth_date,
            c.street_address,
            c.website_url,
            c.linkedin_url,
            c.twitter_handle,
            c.facebook_url,
            c.instagram_handle,
            -- TS form's BR race id; fallback leg of the emitted br_race_id.
            ts_int.br_race_id as br_race_id_ts,
            br_int.br_candidacy_id,
            br_int.br_race_id as br_race_id_br,
            e.population,
            e.city,
            e.district,
            e.filing_deadline,
            e.seats_available,
            e.election_date,
            e.is_uncontested,
            e.number_of_opponents,
            -- nearest-upcoming stage, needed by both `Election Type` and the
            -- appended election_stage (select-list lateral refs can't look ahead).
            case
                when cy.primary_election_date >= current_date()
                then 'primary'
                when cy.general_election_date >= current_date()
                then 'general'
            end as election_stage
        from {{ ref("candidacy_scored") }} as cy
        join {{ ref("candidate") }} as c on cy.gp_candidate_id = c.gp_candidate_id
        left join {{ ref("election") }} as e on cy.gp_election_id = e.gp_election_id
        left join
            {{ ref("int__civics_candidacy_ballotready") }} as br_int
            on cy.gp_candidacy_id = br_int.gp_candidacy_id
        -- LEFT JOIN, never INNER: an INNER join here silently collapses the unified
        -- feed back
        -- to TechSpeed-sourced candidacies only, re-creating the provider
        -- stratification this
        -- feed exists to remove.
        left join
            {{ ref("int__civics_candidacy_techspeed") }} as ts_int
            on cy.gp_candidacy_id = ts_int.gp_candidacy_id
        where
            greatest(cy.created_at, cy.updated_at) >= current_date() - interval 30 day
            -- HS-eligible: has email or phone
            and (
                (c.email is not null and c.email != '')
                or (c.phone_number is not null and c.phone_number != '')
            )
            -- non-major-party (inherited verbatim from the legacy feeds)
            and (
                cy.party_affiliation is null
                or (
                    cy.party_affiliation not ilike '%democrat%'
                    and cy.party_affiliation not ilike '%republican%'
                )
            )
            -- belt-and-suspenders not-already-in-HubSpot
            and cy.hubspot_contact_id is null
            and not exists (
                select 1
                from {{ ref("int__hubspot_contacts") }} as hs
                where
                    (
                        c.email is not null
                        and c.email != ''
                        and lower(trim(hs.email)) = lower(trim(c.email))
                    )
                    or (
                        c.phone_number is not null
                        and c.phone_number != ''
                        and length(regexp_replace(c.phone_number, '[^0-9]', '')) >= 10
                        and regexp_replace(hs.phone_number, '[^0-9]', '')
                        = regexp_replace(c.phone_number, '[^0-9]', '')
                    )
                    or (
                        br_int.br_candidacy_id is not null
                        and hs.br_candidacy_id = try_cast(br_int.br_candidacy_id as int)
                    )
            )
    )

select
    coalesce(b.candidate_id_source, '') as `Candidate ID Source`,
    coalesce(b.first_name, '') as `First Name`,
    coalesce(b.last_name, '') as `Last Name`,
    case
        when b.is_incumbent
        then 'Incumbent'
        when not b.is_incumbent
        then 'Challenger'
        else ''
    end as `Candidate Type`,
    coalesce(b.party_affiliation, '') as `Party Affiliation`,
    coalesce(b.email, '') as `Email`,
    coalesce(b.phone_number, '') as `Phone Number`,
    coalesce(b.candidate_id_tier, '') as `Candidate ID Tier`,
    coalesce(b.website_url, '') as `Website URL`,
    coalesce(b.linkedin_url, '') as `LinkedIn URL`,
    coalesce(b.instagram_handle, '') as `Instagram Handle`,
    coalesce(b.twitter_handle, '') as `Twitter Handle`,
    coalesce(b.facebook_url, '') as `Facebook URL`,
    coalesce(cast(b.birth_date as string), '') as `Birth Date`,
    coalesce(b.street_address, '') as `Street Address`,
    coalesce(b.state, '') as `State/Region`,
    -- blank since the TS fuzzy pipeline removal: no civics-side zip yet
    '' as postal_code,
    coalesce(b.district, '') as `District`,
    coalesce(b.city, '') as `City`,
    b.population as `population`,
    coalesce(b.official_office_name, '') as `Official Office Name`,
    coalesce(b.candidate_office, '') as `Candidate Office`,
    coalesce(b.office_type, '') as `Office Type`,
    coalesce(b.office_level, '') as `Office Level`,
    coalesce(cast(b.filing_deadline as string), '') as `Filing Deadline`,
    coalesce(
        cast(b.br_race_id_br as string), cast(b.br_race_id_ts as string), ''
    ) as br_race_id,
    coalesce(cast(b.primary_election_date as string), '') as `Primary Election Date`,
    coalesce(cast(b.general_election_date as string), '') as `General Election Date`,
    coalesce(cast(b.election_date as string), '') as `Election Date`,
    coalesce(initcap(b.election_stage), '') as `Election Type`,
    case
        when b.is_uncontested
        then 'Uncontested'
        when not b.is_uncontested
        then 'Contested'
        else ''
    end as `Uncontested`,
    -- election carries number_of_opponents as string (DDHQ null-cast widens the
    -- coalesce); round-trip through int keeps this column string-typed as before.
    cast(
        try_cast(b.number_of_opponents as int) + 1 as string
    ) as `Number of Candidates`,
    b.seats_available as `Number of Seats Available`,
    case
        when b.is_open_seat then 'Yes' when not b.is_open_seat then 'No' else ''
    end as `Open Seat`,
    case
        when b.is_partisan
        then 'Partisan'
        when not b.is_partisan
        then 'Nonpartisan'
        else ''
    end as `Partisan Type`,
    -- Type / Contact Owner / Owner Name set for ALL rows (the legacy feed sourced
    -- these from
    -- the TechSpeed passthrough join, leaving non-TS rows unassigned). Owner via
    -- env_var keeps
    -- the real name out of the repo; the empty default is caught by the not-blank
    -- test, so a
    -- misconfigured prod fails closed rather than silently shipping unassigned leads.
    'Self-Filer Lead' as `Type`,
    '{{ env_var("DBT_CIVICS_HUBSPOT_CONTACT_OWNER", "") }}' as `Contact Owner`,
    '{{ env_var("DBT_CIVICS_HUBSPOT_OWNER_NAME", "") }}' as `Owner Name`,
    current_timestamp() as added_to_mart_at,
    -- Viability comes from candidacy_scored (the broad civics viability source of
    -- truth). Its civics scorer is canonical; the prior-value gap-fill is
    -- 2025-archive-only since the TechSpeed fuzzy-dedupe removal. Scale is
    -- unchanged (0 to 5), so accepted_range still holds.
    b.viability_score as viability_rating_2_0,
    b.score_viability_automated as score_viability_automated,
    coalesce(
        b.is_win_icp,
        b.population between 500 and 100000
        and lower(trim(b.candidate_office)) in (select ts_office from icp_ts_offices),
        false
    ) as icp_win,
    coalesce(
        b.is_serve_icp,
        b.population between 1000 and 100000
        and lower(trim(b.candidate_office)) in (select ts_office from icp_ts_offices),
        false
    ) as icp_serve,
    coalesce(
        b.is_win_supersize_icp,
        b.population > 100000
        and lower(trim(b.candidate_office)) in (select ts_office from icp_ts_offices),
        false
    ) as icp_win_supersize,
    'Civics Net New' as lead_source,

    -- === appended: match-back key + reverse-ETL key inputs + recency ===
    cast(b.br_candidacy_id as string) as candidacy_id,
    {{
        generate_candidate_code(
            "b.first_name", "b.last_name", "b.state", "b.office_type"
        )
    }} as candidate_code,
    b.gp_candidacy_id,
    b.gp_candidate_id,
    -- informational on this feed (no future-date filter, so a row may have only past
    -- stages);
    -- nearest-upcoming where one exists, else null/most-recent.
    year(
        coalesce(
            case
                when b.primary_election_date >= current_date()
                then b.primary_election_date
            end,
            case
                when b.general_election_date >= current_date()
                then b.general_election_date
            end,
            b.general_election_date,
            b.primary_election_date
        )
    ) as election_year,
    b.election_stage,
    greatest(b.created_at, b.updated_at) as last_activity_at
from civics_base as b
