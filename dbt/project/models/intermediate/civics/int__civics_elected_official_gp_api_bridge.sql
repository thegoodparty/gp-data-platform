{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Term-grain bridge from gp-api elected_office records to BR+TS terms.
-- Output: at most one row per gp_api elected-office record AND at most one
-- row per BR term (both sides 1:1 after pass 2).
--
-- Pass 1: pick best BR term per gp_api elected_office (multi-term ambiguity
-- resolution by sworn_in_date proximity to BR term_start_date — gp_api
-- sworn_in_date is user-entered and often NULL or noisy, so this is a
-- tiebreaker, not a filter; the bridge always emits a row when the cluster
-- has one BR term).
-- Pass 2: deterministic suppression of br_office_holder_id collisions when
-- multiple gp_api rows in the same Splink cluster pick the same BR term.
--
-- BR-side rows are filtered to only those whose br_candidate_id resolves to
-- int__civics_elected_official_ballotready_person. This handles transient
-- data drift between the matcha cluster output (a snapshot) and the latest
-- BR person rollup — without it, an unresolvable br_candidate_id would
-- silently self-key the gp_api user instead of producing a clean bridge
-- match. The bridge_br_candidate_resolves_to_br_person YAML test asserts
-- this contract.
--
-- The campaigns LEFT JOIN attaches `hubspot_company_id` (alias of
-- `campaigns.hubspot_id`) at the term/campaign grain.
with
    clustered as (
        select * from {{ ref("stg_er_source__clustered_elected_officials") }}
    ),

    gp_api_side as (
        select
            cluster_id,
            cast(source_id as string) as gp_api_elected_office_id,
            gp_api_user_id,
            gp_api_campaign_id,
            gp_api_organization_slug,
            term_start_date as sworn_in_date
        from clustered
        where source_name = 'gp_api'
    ),

    br_resolvable as (
        select br_candidate_id
        from {{ ref("int__civics_elected_official_ballotready_person") }}
    ),

    br_side as (
        select
            c.cluster_id,
            c.br_office_holder_id,
            c.br_candidate_id,
            c.ts_officeholder_id,
            c.term_start_date as br_term_start_date
        from clustered as c
        inner join br_resolvable as r on r.br_candidate_id = c.br_candidate_id
        where c.source_name = 'ballotready_techspeed'
    ),

    -- Pass 1: 1 BR term per gp_api elected_office (closest by sworn_in_date)
    pass1 as (
        select
            g.gp_api_elected_office_id,
            g.gp_api_user_id,
            g.gp_api_campaign_id,
            g.gp_api_organization_slug,
            g.sworn_in_date,
            g.cluster_id,
            b.br_office_holder_id,
            b.br_candidate_id,
            b.ts_officeholder_id,
            b.br_term_start_date,
            abs(datediff(b.br_term_start_date, g.sworn_in_date)) as days_to_sworn_in
        from gp_api_side g
        inner join br_side b using (cluster_id)
        qualify
            row_number() over (
                partition by g.gp_api_elected_office_id
                order by
                    abs(datediff(b.br_term_start_date, g.sworn_in_date)) asc nulls last,
                    b.br_term_start_date desc nulls last,
                    b.br_office_holder_id asc
            )
            = 1
    ),

    -- Pass 2: deterministic suppression of br_office_holder_id collisions
    deduped as (
        select *
        from pass1
        qualify
            row_number() over (
                partition by br_office_holder_id
                order by
                    days_to_sworn_in asc nulls last,
                    sworn_in_date desc nulls last,
                    gp_api_elected_office_id asc
            )
            = 1
    ),

    -- HubSpot company ID alias: campaigns.hubspot_id at the campaign grain
    campaigns as (
        select campaign_id, hubspot_id as hubspot_company_id
        from {{ ref("campaigns") }}
        where is_latest_version
    )

select deduped.*, campaigns.hubspot_company_id
from deduped
left join campaigns on campaigns.campaign_id = deduped.gp_api_campaign_id
