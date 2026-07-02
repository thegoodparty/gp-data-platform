-- Elected officials -> HubSpot Contacts export (deterministic match pass).
--
-- dbt *analysis*: compiled and validated but never materialized. Run the
-- compiled SQL ad hoc and export the result to CSV for the Sales team to upload
-- to HubSpot Contacts. Plan: .tickets/eo_hubspot_export_plan.md
--
-- One row per in-scope elected official. hubspot_contact_id is populated only
-- where the person already exists as a HubSpot contact (so Sales does not create
-- duplicates); the match_method column records how the link was made.
--
-- Scope:
-- - current officeholders only (latest term not yet ended)
-- - contactable (has email or phone)
-- - Independent / 3rd-party only: excludes Republicans, Democrats, and
-- unknown party (keeps Nonpartisan, Independent, Other, Libertarian, Green)
-- - is_serve_icp is exposed as a column but is NOT a scope filter
--
-- Deterministic match layers, highest precision first.
-- L0 gp_api link already resolved on the mart (gp_api user -> HubSpot contact)
-- L1 HubSpot br_candidacy_id -> BR candidacy -> br_candidate_id (person)
-- L2 exact normalized email
-- L3 exact normalized phone AND last-name agreement
--
-- A 50-record audit (see .tickets/audit_50_sample.md) confirmed these four tiers
-- carry no false-positive matches. The two lower-confidence approaches trialed
-- earlier -- exact full name + state, and a Splink fuzzy pass -- both leaked
-- false positives (same name + state, different person/locality) and were
-- dropped from the authoritative link.
with
    eo as (
        select
            e.gp_elected_official_id,
            e.br_candidate_id,
            e.first_name,
            e.last_name,
            e.full_name,
            e.email,
            e.phone,
            e.party_affiliation,
            e.tier,
            e.website_url,
            e.linkedin_url,
            e.twitter_url,
            e.facebook_url,
            e.mailing_address_line_1,
            e.mailing_address_line_2,
            coalesce(e.mailing_city, e.city) as city,
            coalesce(e.mailing_state, e.state) as state_region,
            e.mailing_zip,
            e.district,
            e.candidate_office,
            e.office_type,
            e.office_level,
            e.source_systems,
            e.hubspot_contact_id as existing_hubspot_contact_id,
            t.is_serve_icp
        from {{ ref("elected_officials") }} as e
        left join
            {{ ref("elected_official_terms") }} as t
            on e.selected_gp_elected_official_term_id = t.gp_elected_official_term_id
        where
            (e.term_end_date >= current_date or e.term_end_date is null)  -- current
            and (e.email is not null or e.phone is not null)  -- contactable
            -- Independent / 3rd-party only: drop the two major parties and
            -- unknown party. Keeps Nonpartisan, Independent, Other, Libertarian,
            -- Green. (is_serve_icp is exposed as a column, no longer a filter.)
            and e.party_affiliation is not null
            and e.party_affiliation not in ('Republican', 'Democrat')
    ),

    hs as (
        select
            id as hubspot_contact_id,
            lower(trim(email)) as email_norm,
            {{ clean_phone_number("phone") }} as phone_norm,
            lower(trim(last_name)) as last_name_norm,
            cast(br_candidacy_id as string) as br_candidacy_id,
            coalesce(last_contacted_at, updated_at) as contact_last_activity
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    -- Bridge HubSpot's BR candidacy id to the BR person key (L1). Raw BR
    -- candidacies staging carries both keys at full candidacy grain.
    cand_bridge as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id, br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    matches as (
        -- L0: gp_api link already carried on the mart
        select
            gp_elected_official_id,
            existing_hubspot_contact_id as hubspot_contact_id,
            0 as match_priority,
            'gp_api_user' as match_method,
            cast(null as timestamp) as contact_last_activity
        from eo
        where existing_hubspot_contact_id is not null

        union all

        -- L1: HubSpot br_candidacy_id -> person
        select
            eo.gp_elected_official_id,
            hs.hubspot_contact_id,
            1 as match_priority,
            'br_candidacy_id' as match_method,
            hs.contact_last_activity
        from eo
        join cand_bridge as cb on eo.br_candidate_id = cb.br_candidate_id
        join hs on hs.br_candidacy_id = cb.br_candidacy_id

        union all

        -- L2: exact normalized email
        select
            eo.gp_elected_official_id,
            hs.hubspot_contact_id,
            2 as match_priority,
            'email' as match_method,
            hs.contact_last_activity
        from eo
        join hs on lower(trim(eo.email)) = hs.email_norm
        where hs.email_norm is not null

        union all

        -- L3: exact normalized phone AND last-name agreement (phone alone is
        -- unsafe: shared office/central lines)
        select
            eo.gp_elected_official_id,
            hs.hubspot_contact_id,
            3 as match_priority,
            'phone_lastname' as match_method,
            hs.contact_last_activity
        from eo
        join
            hs
            on {{ clean_phone_number("eo.phone") }} = hs.phone_norm
            and lower(trim(eo.last_name)) = hs.last_name_norm
        where hs.phone_norm is not null
    ),

    -- One contact per EO: keep each EO's strongest match.
    best_per_eo as (
        select
            gp_elected_official_id,
            hubspot_contact_id,
            match_method,
            match_priority,
            contact_last_activity
        from matches
        qualify
            row_number() over (
                partition by gp_elected_official_id
                order by match_priority asc, contact_last_activity desc nulls last
            )
            = 1
    ),

    -- One EO per contact: enforce a strict 1:1 link so no HubSpot contact is
    -- referenced by more than one exported row. A contact claimed by multiple
    -- EOs (shared office email/phone, common name) goes to its strongest EO;
    -- the losers drop to unmatched for the fuzzy/manual pass.
    best_match as (
        select gp_elected_official_id, hubspot_contact_id, match_method
        from best_per_eo
        qualify
            row_number() over (
                partition by hubspot_contact_id
                order by match_priority asc, contact_last_activity desc nulls last
            )
            = 1
    )

select
    bm.hubspot_contact_id,
    eo.br_candidate_id as person_id,
    eo.first_name as `First Name`,
    eo.last_name as `Last Name`,
    eo.email as `Email`,
    eo.phone as `Phone Number`,
    eo.party_affiliation as `Party Affiliation`,
    cast(eo.tier as string) as `Candidate ID Tier`,
    eo.website_url as `Website URL`,
    eo.linkedin_url as `LinkedIn URL`,
    eo.twitter_url as `Twitter Handle`,
    eo.facebook_url as `Facebook URL`,
    eo.mailing_address_line_1 as `Street Address`,
    eo.mailing_address_line_2 as `Street Address 2`,
    eo.city as `City`,
    eo.state_region as `State/Region`,
    eo.mailing_zip as `postal_code`,
    eo.district as `District`,
    eo.candidate_office as `Candidate Office`,
    eo.candidate_office as `Official Office Name`,
    eo.office_type as `Office Type`,
    eo.office_level as `Office Level`,
    eo.is_serve_icp,
    case
        when
            array_contains(eo.source_systems, 'ballotready')
            and array_contains(eo.source_systems, 'techspeed')
        then 'TS + BR'
        when array_contains(eo.source_systems, 'techspeed')
        then 'TechSpeed Incumbents'
        else 'Ballotready'
    end as source_systems,
    bm.match_method
from eo
left join best_match as bm on eo.gp_elected_official_id = bm.gp_elected_official_id
