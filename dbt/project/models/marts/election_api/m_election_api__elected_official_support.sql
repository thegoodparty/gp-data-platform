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
-- Grain: one row per gp-api elected_office instance (elected_office_id), which is
-- tied to a user/official. Each office resolves to a single position via
-- organization.position_id; the position's support numbers are attached, so every
-- office at a position shares them. support_constituents is the position's
-- general-election winning votes (position-level: multi-seat offices use the top
-- winner by votes); total_constituents is the position's L2 registered-voter
-- count. The product shows support_constituents / total_constituents.
-- Per-position support is computed first (below) from each office's most recent
-- GENERAL win, then fanned out to elected_office instances at the end.
-- Primaries are excluded (a primary win does not confer office); 'general' covers
-- general, general special, general runoff, and general special runoff.
-- Population: every metric column is populated and positive. Only contested wins
-- with a coherent vote tally and an L2 voter count survive, and rows are dropped
-- when votes, the L2 voter count, or the projected support would be zero.
-- Uncontested winners, rows where votes exceed the reported total (unreliable
-- partial loads), and rows with no L2 match are also excluded.
-- Sources: votes_received is DDHQ via candidacy_stage; the office (br_position_id),
-- number_of_seats, and total_votes_cast come from election_stage (BallotReady-first
-- for the total); icp_voter_count is the L2 district voter count from the positions
-- mart. The office is read from election_stage, not the elected-officials roster
-- (elected_official_terms), so every general winner is covered, not only those
-- resolved into an elected-official record. number_of_seats flags multi-seat races,
-- where one winner's share of the race total is structurally low.
-- projected_registered_supporters applies the vote share to icp_voter_count. It is
-- an INTERIM PLACEHOLDER for a future constituents-based figure; it assumes
-- non-voters split like voters, so it is for display, not analysis.
-- Two sources are unioned at the position grain:
-- 1. The civics general-win path above, which is scoped to elections from 2026
-- onward (the repo-wide HubSpot->BallotReady candidate-provenance cutover).
-- 2. A 2025 DDHQ supplement (see the ddhq_2025 CTEs) for offices product
-- elected officials hold. Those officials mostly won in the Nov 2025
-- off-year local elections, which DDHQ reports but the 2026 civics scope
-- excludes; DDHQ results are independent of the HubSpot/BR candidate
-- cutover, so reading them here does not cross that boundary. The civics
-- path wins when an office is covered by both.
with
    general_winning_stages as (
        select gp_election_stage_id, votes_received, election_stage_date
        from {{ ref("candidacy_stage") }}
        where is_winner and lower(election_stage) like '%general%'
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
            number_of_seats,
            votes_received,
            total_votes_cast,
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
    -- holder's election rather than an older high-turnout one; within that
    -- election, multi-seat offices have several co-winners, so the top winner by
    -- votes is kept (gp_election_stage_id breaks any remaining tie for
    -- determinism). Drops rows whose projected support rounds to zero (a tiny
    -- district with a low share) before ranking.
    per_position as (
        select
            br_position_id,
            number_of_seats,
            votes_received,
            total_votes_cast,
            icp_voter_count,
            projected_registered_supporters
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

    -- ===== 2025 DDHQ supplement (offices product elected officials hold) =====
    -- Offices product elected officials hold, resolved to a BR position id via
    -- elected_office -> organization.position_id (the Election API position UUID),
    -- with the office-name match keys (see the office_match_keys macro).
    product_offices_raw as (
        select distinct pos.br_database_id as br_position_id, pos.state, pos.name
        from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }} as eo
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_organization") }} as org
            on eo.organization_slug = org.slug
        inner join
            {{ ref("m_election_api__position") }} as pos on org.position_id = pos.id
        where pos.br_database_id is not null
    ),

    product_offices as (
        select br_position_id, state, {{ office_match_keys("name") }}
        from product_offices_raw
    ),

    -- DDHQ general results (2025 onward) aggregated to the race: total votes
    -- cast, the winning votes, seat count, and the office-name match keys. Any
    -- general stage (incl. special/runoff) is kept; the 2025 floor on the civics
    -- path means most of these are 2025, but in-window 2026 races the main path
    -- missed are caught here too (the per_position dedup keeps it from
    -- double-counting offices the civics path already covers).
    ddhq_results_raw as (
        select
            ddhq_race_id,
            max(state_postal_code) as state,
            max(official_office_name) as official_office_name,
            max(election_date) as election_date,
            sum(try_cast(votes as bigint)) as total_votes_cast,
            -- the winner's votes; fall back to the top vote-getter when DDHQ did
            -- not flag a winner (the top vote-getter is the winner)
            coalesce(
                max(case when is_winner then try_cast(votes as bigint) end),
                max(try_cast(votes as bigint))
            ) as votes_received,
            max(try_cast(number_of_seats_in_election as int)) as number_of_seats
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where
            lower(election_stage) like '%general%'
            and election_date >= date '2025-01-01'
        group by ddhq_race_id
    ),

    ddhq_results as (
        select
            ddhq_race_id,
            state,
            election_date,
            total_votes_cast,
            votes_received,
            number_of_seats,
            {{ office_match_keys("official_office_name") }}
        from ddhq_results_raw
    ),

    -- Match a product office to its DDHQ race when, in the same state, the
    -- distinctive locality tokens are identical, the office category agrees, and
    -- the seat designator is null-safe equal. This normalization (via
    -- office_match_keys) catches the cross-source name variants -- City of X / X
    -- City, Twp / Township, Councilor / Council, numbered seats, at-large -- while
    -- keeping different localities and office types apart. icp_voter_count is the
    -- BR position's own L2 count. One DDHQ race per office; prefer one with votes,
    -- then the largest.
    ddhq_matched as (
        select
            po.br_position_id,
            d.number_of_seats,
            d.votes_received,
            d.total_votes_cast,
            pos.icp_voter_count
        from product_offices as po
        inner join
            ddhq_results as d
            on d.state = po.state
            and size(po.locality_key) >= 1
            and po.locality_key = d.locality_key
            and po.office_category = d.office_category
            -- seat must agree; an unnumbered BR office is treated as compatible
            -- with a DDHQ "at-large" race (the whole-body seat), but never with a
            -- specific numbered seat.
            and (
                po.seat_designator <=> d.seat_designator
                or (po.seat_designator is null and d.seat_designator = 'atlarge')
                or (d.seat_designator is null and po.seat_designator = 'atlarge')
            )
        left join
            {{ ref("positions") }} as pos
            on pos.br_position_database_id = po.br_position_id
        qualify
            row_number() over (
                partition by po.br_position_id
                order by
                    case when d.votes_received > 0 then 0 else 1 end,
                    d.election_date desc nulls last,
                    d.total_votes_cast desc,
                    d.ddhq_race_id
            )
            = 1
    ),

    per_position_ddhq as (
        select
            br_position_id,
            number_of_seats,
            votes_received,
            total_votes_cast,
            icp_voter_count,
            cast(
                round(
                    icp_voter_count * votes_received / cast(total_votes_cast as double)
                ) as bigint
            ) as projected_registered_supporters
        from ddhq_matched
        where
            votes_received > 0
            and total_votes_cast > 0
            and votes_received <= total_votes_cast
            and icp_voter_count > 0
            -- the civics general-win path wins when an office is covered by both
            and br_position_id not in (
                select br_position_id from per_position where br_position_id is not null
            )
    ),

    -- Union the two sources at the position grain before attaching the UUID.
    combined as (
        select
            br_position_id,
            number_of_seats,
            votes_received,
            total_votes_cast,
            icp_voter_count,
            projected_registered_supporters
        from per_position
        union all
        select
            br_position_id,
            number_of_seats,
            votes_received,
            total_votes_cast,
            icp_voter_count,
            projected_registered_supporters
        from per_position_ddhq
        where projected_registered_supporters > 0
    ),

    -- Fan out to one row per gp-api elected_office instance. Each office maps to
    -- a single position via organization.position_id -> Position.id; the
    -- position's support numbers are attached, so co-holders of a multi-seat
    -- office share them. The join keys are unique (organization.slug,
    -- Position.id, combined.br_position_id), so there is no fan-out beyond the
    -- one row per elected_office_id. Offices whose position has no support row
    -- are dropped.
    office_support as (
        select
            eo.id as elected_office_id,
            c.votes_received as support_constituents,
            c.icp_voter_count as total_constituents
        from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }} as eo
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_organization") }} as org
            on eo.organization_slug = org.slug
        inner join
            {{ ref("m_election_api__position") }} as pos on org.position_id = pos.id
        inner join combined as c on c.br_position_id = pos.br_database_id
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
