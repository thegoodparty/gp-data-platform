{{ config(materialized="view", tblproperties={}) }}

-- Databricks-owned attributes -> EXISTING HubSpot companies, matched on the
-- company's Record ID (hubspot_company_id). A HubSpot company is one campaign in
-- one cycle, and the Serve Priority Segment belongs to that campaign's office, so
-- this is the grain the segment is applied at. Companion to
-- contact_attributes_hubspot, which stays for contacts that carry a position of
-- their own. New company attributes we own that are not viability belong here.
--
-- Position and company resolve the way company_viability_hubspot resolves a
-- company's candidacy: explode candidacy_scored.hubspot_company_ids and let the
-- live cycle win when archive-reused ids are carried by several candidacies. The
-- segment then comes from int__icp_offices on the candidacy's BallotReady
-- position, as in candidacy_hubspot.
--
-- A snapshot, not a diff like the contact feed: the company staging model does not
-- yet expose HubSpot's current serve_priority_segment, because the company
-- property does not exist until it is created in HubSpot. Once it does, expose it
-- there and add the equal_null filter contact_attributes_hubspot uses.
--
-- An unknown on our side (no resolved position, or an unsized serve_priority
-- district) is not sent, so it stays blank in HubSpot rather than being asserted
-- false. A resolved position off the list is a known false and is sent.
--
-- A view for the same reason as company_viability_hubspot: ops reads it right
-- before an import. Empty tblproperties: Delta column mapping is meaningless on a
-- view.
with
    exploded as (
        select
            explode(from_json(cy.hubspot_company_ids, 'array<string>')) as record_id,
            cy.gp_candidacy_id,
            cy.br_position_database_id,
            cy.updated_at,
            -- null once every stage is in the past
            least(
                case
                    when cy.primary_election_date >= current_date()
                    then cy.primary_election_date
                end,
                case
                    when cy.general_election_date >= current_date()
                    then cy.general_election_date
                end
            ) as next_election_date,
            greatest(
                cy.primary_election_date, cy.general_election_date
            ) as latest_election_date
        from {{ ref("candidacy_scored") }} as cy
        where
            cy.hubspot_company_ids is not null
            and trim(cy.hubspot_company_ids) not in ('', '[]')
    ),

    -- The 2025 HubSpot archive reuses company ids across cycles. Live cycle wins.
    ranked as (
        select
            *,
            row_number() over (
                partition by record_id
                order by
                    case when next_election_date is null then 1 else 0 end,
                    next_election_date asc,
                    latest_election_date desc,
                    updated_at desc,
                    gp_candidacy_id
            ) as rn
        from exploded
        where record_id is not null and trim(record_id) <> ''
    )

-- INNER join: a Record ID missing from the ingest no longer exists in HubSpot and
-- would only error on import. 1:1 on the position (unique in int__icp_offices).
select
    r.record_id as hubspot_company_id,
    icp.is_serve_priority_segment as serve_priority_segment
from ranked as r
join
    {{ ref("stg_airbyte_source__hubspot_api_companies") }} as c
    on r.record_id = cast(c.id as string)
join
    {{ ref("int__icp_offices") }} as icp
    on r.br_position_database_id = icp.br_database_position_id
where r.rn = 1 and icp.is_serve_priority_segment is not null
