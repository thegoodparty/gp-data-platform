{{
    config(
        materialized="table",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "elected_official_support"],
    )
}}

-- Election API Elected_Office_Support table.
-- Grain: one row per gp-api elected_office instance (elected_office_id), tied to
-- a user/official. support_constituents is the official's general-election
-- winning votes; total_constituents is their office's L2 registered-voter count.
-- The product shows support_constituents / total_constituents.
--
-- support_constituents is taken by precedence:
-- 1. The official's OWN DDHQ winning votes, read from the civics
-- elected_official_terms mart (ddhq_winning_votes). Civics resolves the DDHQ
-- winner to each office via the matcha elected_official entity resolution, so
-- this layer builds on the civics mart rather than the ER staging cluster.
-- Correct for multi-seat offices (the official's own votes, not the race top
-- winner). For gp_api offices with no BallotReady term (absent from the
-- BR-spine terms mart) the same figure is read from the civics intermediate
-- int__civics_elected_official_ddhq_matched_votes as a supplement.
-- 2. per_position (FALLBACK): a position-level figure from each office's most
-- recent GENERAL win in the civics data, scoped to elections from 2026 onward
-- (the repo-wide HubSpot->BallotReady candidate-provenance cutover). Multi-
-- seat offices use the top winner by votes. Used only where there is no DDHQ
-- match. The office is read from election_stage, not the elected-officials
-- roster, so every 2026+ general winner is covered.
--
-- Population: every metric column is populated and positive. The civics path
-- keeps only contested wins with a coherent vote tally (votes <= total) and an
-- L2 count, and drops rows whose projected support rounds to zero. Offices with
-- no support figure from either source are dropped.
with
    general_winning_stages as (
        select gp_election_stage_id, votes_received, election_stage_date
        from {{ ref("candidacy_stage") }}
        where
            is_winner
            and lower(election_stage) like '%general%'
            -- 2026+ only: the civics path is scoped to the post-cutover window.
            -- Earlier (Nov 2025) general results reach the model via the DDHQ
            -- votes from civics (terms_votes / gp_only_votes), not this path.
            and election_stage_date >= date '2026-01-01'
    ),

    -- Resolve the office straight from election_stage (which carries
    -- br_position_id, number_of_seats, and total_votes_cast for the race),
    -- rather than routing the winner through elected_official_terms. The roster
    -- join only kept winners that were entity-resolved into an elected-official
    -- record, dropping otherwise-valid general winners; this covers them all.
    joined as (
        select
            es.br_position_id,
            ws.gp_election_stage_id,
            ws.election_stage_date,
            es.number_of_seats,
            try_cast(ws.votes_received as bigint) as votes_received,
            try_cast(
                nullif(es.total_votes_cast, 'uncontested') as bigint
            ) as total_votes_cast,
            pos.icp_voter_count
        from general_winning_stages as ws
        inner join
            {{ ref("election_stage") }} as es
            on ws.gp_election_stage_id = es.gp_election_stage_id
        left join
            {{ ref("positions") }} as pos
            on es.br_position_id = pos.br_position_database_id
    ),

    scored as (
        select
            br_position_id,
            votes_received,
            icp_voter_count,
            election_stage_date,
            gp_election_stage_id,
            cast(
                round(
                    icp_voter_count * votes_received / cast(total_votes_cast as double)
                ) as bigint
            ) as projected_registered_supporters
        from joined
        where
            votes_received > 0
            and total_votes_cast > 0
            and votes_received <= total_votes_cast
            and icp_voter_count > 0
    ),

    -- Collapse to one row per position from that office's most recent general
    -- win. Rank by election recency first, so the row reflects the current
    -- holder's election; within that election, multi-seat offices have several
    -- co-winners, so the top winner by votes is kept (gp_election_stage_id
    -- breaks ties for determinism). Drops rows whose projected support rounds to
    -- zero (a tiny district with a low share).
    per_position as (
        select br_position_id, votes_received, icp_voter_count
        from scored
        where projected_registered_supporters > 0
        qualify
            row_number() over (
                partition by br_position_id
                order by
                    election_stage_date desc nulls last,
                    votes_received desc,
                    gp_election_stage_id desc
            )
            = 1
    ),

    -- ===== Primary source: the official's own DDHQ votes, read from the civics
    -- elected_official_terms mart =====
    -- The matcha elected_official entity resolution links each gp_api office to
    -- its DDHQ general-winner record; civics resolves that to ddhq_winning_votes
    -- (the official's OWN votes) and surfaces it on elected_official_terms. We
    -- read it from there so the election_api layer builds on the civics mart
    -- rather than reaching into the ER staging cluster. One row per office (the
    -- terms mart may carry several BR terms for one office, all sharing the same
    -- ddhq_winning_votes, so max() collapses them without changing the value).
    terms_votes as (
        select
            gp_api_elected_office_id as elected_office_id,
            max(ddhq_winning_votes) as votes_received
        from {{ ref("elected_official_terms") }}
        where gp_api_elected_office_id is not null and ddhq_winning_votes is not null
        group by gp_api_elected_office_id
    ),

    -- gp_api-only supplement: elected_official_terms is BR-spine, so offices with
    -- no BallotReady term are absent. Read their DDHQ votes from the civics
    -- intermediate directly (the same source that feeds terms), only for offices
    -- not already covered above.
    gp_only_votes as (
        select
            dv.gp_api_elected_office_id as elected_office_id,
            dv.ddhq_winning_votes as votes_received
        from {{ ref("int__civics_elected_official_ddhq_matched_votes") }} as dv
        left join
            terms_votes as tv on tv.elected_office_id = dv.gp_api_elected_office_id
        where tv.elected_office_id is null
    ),

    -- Fan out to one row per gp-api elected_office instance. Each office maps to
    -- a single position via organization.position_id -> Position.id.
    -- support_constituents is the official's own DDHQ votes (from the civics
    -- terms mart, else the gp_api-only supplement), falling back to the
    -- position-level civics general-win figure. total_constituents is the
    -- position's L2 voter count. The join keys are unique, so there is no fan-out
    -- beyond one row per elected_office_id. Offices with no support figure from
    -- any source are dropped.
    office_support as (
        select
            eo.id as elected_office_id,
            coalesce(
                tv.votes_received, gv.votes_received, pp.votes_received
            ) as support_constituents,
            pos_l2.icp_voter_count as total_constituents
        from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }} as eo
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_organization") }} as org
            on eo.organization_slug = org.slug
        inner join
            {{ ref("m_election_api__position") }} as pos on org.position_id = pos.id
        left join terms_votes as tv on tv.elected_office_id = eo.id
        left join gp_only_votes as gv on gv.elected_office_id = eo.id
        left join per_position as pp on pp.br_position_id = pos.br_database_id
        left join
            {{ ref("positions") }} as pos_l2
            on pos_l2.br_position_database_id = pos.br_database_id
        where
            coalesce(tv.votes_received, gv.votes_received, pp.votes_received) > 0
            and pos_l2.icp_voter_count > 0
    )

select
    office_support.elected_office_id,
    office_support.support_constituents,
    office_support.total_constituents,
    -- Full table rebuild each run, so the table reflects current coverage with no
    -- stale rows for offices that lost their support figure. created_at therefore
    -- tracks the build time (not a stable first-seen timestamp).
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from office_support
