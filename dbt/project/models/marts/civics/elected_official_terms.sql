-- Civics mart elected_official_terms table
-- Term-grain BR fact table with minimal TS provenance markers.
--
-- Grain: One row per BR elected-official term (br_office_holder_id).
-- Same row count as int__civics_elected_official_ballotready (~530K).
--
-- TS provenance: ts_officeholder_id, has_direct_ts_term_match, and
-- ts_officeholder_id_is_reused are surfaced via LEFT JOIN to the EO
-- crosswalk so consumers can identify TS-matched terms and audit
-- contamination risk. NO TS-derived term-grain attributes (is_incumbent,
-- contact rollup) are exposed here — those are person-grain by nature
-- of TS data and live on elected_officials only.
--
-- ICP flags pass through from int__civics_elected_official_ballotready,
-- where Win ICP / Win Supersize ICP are gated by the candidacy's
-- election_day (joined via br_candidacy_id). NULL election_day yields
-- NULL flag. is_serve_icp has no effective-date gate.
--
-- source_systems is join-based per the candidacy mart convention.
with
    br_terms as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    -- TS provenance via crosswalk LEFT JOIN. Crosswalk grain (1 row per
    -- ts_officeholder_id with deterministic 1:1 to br_office_holder_id)
    -- means at most 1 TS row per BR term — no fan-out.
    ts_provenance as (
        select br_office_holder_id, ts_officeholder_id, ts_officeholder_id_is_reused
        from {{ ref("int__civics_elected_official_canonical_ids") }}
    ),

    gp_api_bridge as (
        select * from {{ ref("int__civics_elected_official_gp_api_bridge") }}
    )

select
    -- PK (term-grain canonical UUID, from BR intermediate)
    br.gp_elected_official_term_id,

    -- Person FK (from BR intermediate; NULL for vacancies)
    br.gp_elected_official_id,

    -- Source IDs
    br.br_office_holder_id,
    br.br_candidate_id,
    br.br_position_id,
    br.br_candidacy_id,
    br.br_geo_id,

    -- TS provenance (NULL when no direct TS match)
    ts.ts_officeholder_id,
    ts.ts_officeholder_id is not null as has_direct_ts_term_match,
    coalesce(ts.ts_officeholder_id_is_reused, false) as ts_officeholder_id_is_reused,

    -- Name (BR term)
    br.first_name,
    br.last_name,
    br.middle_name,
    br.suffix,
    br.full_name,

    -- Contact (BR-only at term grain; TS contact lives on elected_officials)
    br.email,
    br.phone,
    br.office_phone,
    br.central_phone,

    -- Position/Office (BR term)
    br.position_name,
    br.normalized_position_name,
    br.candidate_office,
    br.office_level,
    br.office_type,

    -- Geography (BR term)
    br.state,
    br.city,
    br.district,

    -- Term dates
    br.term_start_date,
    br.term_end_date,

    -- Term-scoped flags (BR)
    br.is_appointed,
    br.is_judicial,
    br.is_vacant,
    br.is_off_cycle,

    -- Party (BR term)
    br.party_affiliation,

    -- Social (BR term)
    br.website_url,
    br.linkedin_url,
    br.facebook_url,
    br.twitter_url,

    -- Mailing (BR term)
    br.mailing_address_line_1,
    br.mailing_address_line_2,
    br.mailing_city,
    br.mailing_state,
    br.mailing_zip,

    -- Metadata
    br.br_position_tier as tier,
    br.candidate_id_source,

    -- ICP (passed through from BR intermediate)
    br.is_win_icp,
    br.is_serve_icp,
    br.is_win_supersize_icp,

    -- gp_api term attachment (bridge LEFT JOIN; 1:1 by br_office_holder_id)
    gp.gp_api_elected_office_id,
    gp.gp_api_user_id,
    gp.gp_api_campaign_id,
    gp.gp_api_organization_slug,
    gp.hubspot_company_id,
    gp.days_to_sworn_in,

    -- source_systems: join-based per the candidacy mart convention. BR is always
    -- present (this mart is BR-spine); TS is added when the term has a
    -- direct ts_officeholder_id match; gp_api when the bridge attached a record.
    array_compact(
        array(
            'ballotready',
            case when ts.ts_officeholder_id is not null then 'techspeed' end,
            case when gp.gp_api_elected_office_id is not null then 'gp_api' end
        )
    ) as source_systems,

    br.created_at,
    br.updated_at

from br_terms as br
left join ts_provenance as ts on br.br_office_holder_id = ts.br_office_holder_id
left join gp_api_bridge as gp on br.br_office_holder_id = gp.br_office_holder_id
