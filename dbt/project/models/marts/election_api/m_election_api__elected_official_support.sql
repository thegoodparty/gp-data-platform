{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "elected_official_support"],
    )
}}

-- Election API elected_official_support table.
-- Grain: one row per position (br_position_id), from that office's most recent
-- GENERAL election win. This is the position-grained, public-API version of the
-- civics elected_official_support model: the per-person gp_elected_official_id is
-- intentionally dropped (the election API is public and must not carry per-user
-- ids), and multi-seat offices, which have several winners, are collapsed to the
-- single top winner by votes. The support figure for a multi-seat office
-- therefore reflects one winner; number_of_seats is retained so the consumer can
-- label or adjust it.
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
            and number_of_seats is not null
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

    -- Attach the Election API position UUID (Position.id) so consumers can join
    -- on the same key the product uses (organization.position_id resolves to
    -- this UUID). br_database_id is unique in the position model, so this is a
    -- 1:1 lookup; the inner join drops any office with no Election API position
    -- row, which could not be exposed through the API anyway.
    with_position as (
        select
            pos.id as position_id,
            pp.br_position_id,
            pp.number_of_seats,
            pp.votes_received,
            pp.total_votes_cast,
            pp.icp_voter_count,
            pp.projected_registered_supporters
        from per_position as pp
        inner join
            {{ ref("m_election_api__position") }} as pos
            on pp.br_position_id = pos.br_database_id
    )

select
    {{ generate_salted_uuid(fields=["with_position.br_position_id"]) }} as id,
    {% if is_incremental() %} coalesce(existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at,
    with_position.position_id,
    with_position.br_position_id,
    with_position.number_of_seats,
    with_position.votes_received,
    with_position.total_votes_cast,
    with_position.icp_voter_count,
    with_position.projected_registered_supporters
from with_position
{% if is_incremental() %}
    left join
        {{ this }} as existing
        on {{ generate_salted_uuid(fields=["with_position.br_position_id"]) }}
        = existing.id
{% endif %}
