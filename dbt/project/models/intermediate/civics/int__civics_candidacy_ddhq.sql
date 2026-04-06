-- DDHQ → Civics mart candidacy
-- Derived from: int__civics_candidacy_stage_ddhq
--
-- Grain: One row per candidacy (candidate + position + election year)
--
-- Rolls up from candidacy-stage grain: groups by gp_candidacy_id,
-- extracting stage-specific dates and overall candidacy result.
with
    source as (select * from {{ ref("int__civics_candidacy_stage_ddhq") }}),

    candidacies as (
        select
            gp_candidacy_id,
            any_value(gp_candidate_id) as gp_candidate_id,
            any_value(gp_election_id) as gp_election_id,

            cast(null as string) as br_candidacy_id,
            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,

            'ddhq' as candidate_id_source,
            any_value(source_candidate_id) as candidate_code,

            any_value(party_affiliation) as party_affiliation,
            cast(null as boolean) as is_incumbent,
            cast(null as boolean) as is_open_seat,
            any_value(candidate_office) as candidate_office,
            any_value(official_office_name) as official_office_name,
            any_value(office_level) as office_level,
            any_value(office_type) as office_type,

            cast(null as boolean) as is_pledged,
            cast(null as boolean) as is_verified,
            cast(null as string) as verification_status_reason,
            cast(null as boolean) as is_partisan,

            -- Candidacy result: prefer general, then primary
            coalesce(
                max(case when election_stage = 'general' then election_result end),
                max(case when election_stage = 'primary' then election_result end)
            ) as candidacy_result,

            -- Stage-specific dates (already computed at candidacy level)
            any_value(candidacy_primary_date) as primary_election_date,
            any_value(candidacy_general_date) as general_election_date,
            any_value(candidacy_primary_runoff_date) as primary_runoff_election_date,
            any_value(candidacy_general_runoff_date) as general_runoff_election_date,

            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            min(created_at) as created_at,
            max(updated_at) as updated_at

        from source
        group by gp_candidacy_id
    )

select *
from candidacies
