{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="elected_office_id",
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
-- 1. cluster_ddhq (PRIMARY): the DDHQ general-winner record clustered to the
-- official by the matcha elected_official entity resolution (fuzzy name +
-- office + an election-cycle date comparison). This is the official's OWN
-- votes, so it is correct for multi-seat offices (no top-winner over-
-- attribution) and is the single DDHQ-matching path -- all office-name
-- normalization now lives in the matcher (the prematch's official_office_norm
-- via the office_match_keys macro), not in a separate supplement here.
-- 2. per_position (FALLBACK): a position-level figure from each office's most
-- recent GENERAL win in the civics data, scoped to elections from 2026 onward
-- (the repo-wide HubSpot->BallotReady candidate-provenance cutover). Multi-
-- seat offices use the top winner by votes. Used only where the cluster has
-- no match. The office is read from election_stage, not the elected-officials
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
            -- cluster (cluster_ddhq), not this path.
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

    -- ===== Primary source: the DDHQ winner clustered to the official by the
    -- matcha elected_official entity resolution =====
    -- The matcha Splink matcher resolves each gp_api elected_office to its DDHQ
    -- general-winner record (fuzzy name + office + state, with the election-cycle
    -- date comparison picking the right term). The clustered DDHQ row carries the
    -- official's OWN votes (ddhq_votes). One row per elected_office_id (the
    -- official's most recent winning cycle).
    cluster_ddhq as (
        select
            g.gp_api_elected_office_id as elected_office_id,
            d.ddhq_votes as votes_received
        from {{ ref("stg_er_source__clustered_elected_officials") }} as g
        inner join
            {{ ref("stg_er_source__clustered_elected_officials") }} as d
            on g.cluster_id = d.cluster_id
            and d.source_name = 'ddhq'
            and d.ddhq_votes is not null
        where g.source_name = 'gp_api' and g.gp_api_elected_office_id is not null
        qualify
            row_number() over (
                partition by g.gp_api_elected_office_id
                order by d.term_start_date desc nulls last, d.ddhq_votes desc
            )
            = 1
    ),

    -- Fan out to one row per gp-api elected_office instance. Each office maps to
    -- a single position via organization.position_id -> Position.id.
    -- support_constituents is the official's own DDHQ votes from the cluster
    -- where present, else the position-level civics figure. total_constituents
    -- is the position's L2 voter count. The join keys are unique, so there is no
    -- fan-out beyond one row per elected_office_id. Offices with no support
    -- figure from either source are dropped.
    office_support as (
        select
            eo.id as elected_office_id,
            coalesce(cd.votes_received, pp.votes_received) as support_constituents,
            pos_l2.icp_voter_count as total_constituents
        from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }} as eo
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_organization") }} as org
            on eo.organization_slug = org.slug
        inner join
            {{ ref("m_election_api__position") }} as pos on org.position_id = pos.id
        left join cluster_ddhq as cd on cd.elected_office_id = eo.id
        left join per_position as pp on pp.br_position_id = pos.br_database_id
        left join
            {{ ref("positions") }} as pos_l2
            on pos_l2.br_position_database_id = pos.br_database_id
        where
            coalesce(cd.votes_received, pp.votes_received) > 0
            and pos_l2.icp_voter_count > 0
    )

select
    office_support.elected_office_id,
    office_support.support_constituents,
    office_support.total_constituents,
    {% if is_incremental() %} coalesce(existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at
from office_support
{% if is_incremental() %}
    left join
        {{ this }} as existing
        on office_support.elected_office_id = existing.elected_office_id
{% endif %}
