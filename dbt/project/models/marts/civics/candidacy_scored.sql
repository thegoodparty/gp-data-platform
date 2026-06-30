-- Civics mart `candidacy_scored`: the candidacy mart with broad civics
-- viability layered on. This is the sales/Win-facing viability source going
-- forward; consumers read `viability_score` / `score_viability_automated` here,
-- not on `candidacy`. The analytics consumers (users_win_candidacy,
-- leads_win_candidacy) read here as of this PR; the HubSpot reverse-ETL feed
-- (sales_reverse_etl/candidacy_hubspot) migrates in a staged fast-follow once
-- prod validation passes.
--
-- DATA-1938. The new civics scorer (int__civics_viability_scoring) is canonical
-- and OVERWRITES the old candidacy value wherever it scored the row; the old
-- value gap-fills rows the new scorer could not score, so coverage never
-- regresses. The scorer reads candidacy + election as leaves, so layering its
-- output here (downstream) is ACYCLIC -- the score never flows back onto candidacy.
--
-- Column list mirrors candidacy.sql's final SELECT verbatim and in order; only
-- the two viability columns are overridden. They are gated on the SAME condition
-- so the numeric score and its label can never come from different sources.
with
    scored as (
        select gp_candidacy_id, viability_rating_2_0, score_viability_automated
        from {{ ref("int__civics_viability_scoring") }}
    )

select
    cy.gp_candidacy_id,
    cy.gp_candidate_id,
    cy.gp_election_id,
    cy.product_campaign_id,
    cy.hubspot_contact_id,
    cy.hubspot_company_ids,
    cy.candidate_id_source,
    cy.party_affiliation,
    cy.is_incumbent,
    cy.is_open_seat,
    cy.candidate_office,
    cy.official_office_name,
    cy.office_level,
    cy.office_type,
    cy.candidacy_result,
    cy.general_election_result,
    cy.latest_stage_reached,
    cy.latest_stage_result,
    cy.is_pledged,
    cy.is_verified,
    cy.verification_status_reason,
    cy.is_partisan,
    cy.primary_election_date,
    cy.primary_runoff_election_date,
    cy.general_election_date,
    cy.general_runoff_election_date,
    cy.br_position_database_id,
    -- Viability override: new civics scorer wins, old candidacy value gap-fills.
    case
        when scored.viability_rating_2_0 is not null
        then scored.viability_rating_2_0
        else cy.viability_score
    end as viability_score,
    cy.win_number,
    cy.win_number_model,
    cy.is_win_icp,
    cy.is_serve_icp,
    cy.is_win_supersize_icp,
    -- Label gated on the SAME condition as the score above, so the pair is always
    -- internally consistent (never a new number with an old label).
    case
        when scored.viability_rating_2_0 is not null
        then scored.score_viability_automated
        else cy.score_viability_automated
    end as score_viability_automated,
    cy.source_systems,
    cy.created_at,
    cy.updated_at
from {{ ref("candidacy") }} as cy
left join scored on cy.gp_candidacy_id = scored.gp_candidacy_id
