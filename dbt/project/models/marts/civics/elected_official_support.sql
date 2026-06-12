-- Civics mart elected_official_support table.
-- Grain: one row per (elected official, office), keyed by gp_elected_official_id
-- and br_position_id, from that office's most recent GENERAL election win.
-- Primaries are excluded (a primary win does not confer office); 'general' covers
-- general, general special, general runoff, and general special runoff. A person
-- with multiple offices gets one row per office; same-office re-elections collapse
-- to the most recent win.
-- Population: every column is populated and positive. Only contested wins with a
-- coherent vote tally and an L2 voter count survive, and rows are dropped when
-- votes, the L2 voter count, or the projected support would be zero. Uncontested
-- winners, rows where votes exceed the reported total (unreliable partial loads),
-- and rows with no L2 match are also excluded.
-- Sources: votes_received is DDHQ via candidacy_stage; total_votes_cast is
-- BallotReady-first via election_stage; icp_voter_count is the L2 district voter
-- count from the positions mart. number_of_seats flags multi-seat races (~47%),
-- where one winner's share of the race total is structurally low.
-- projected_registered_supporters applies the vote share to icp_voter_count. It is
-- an INTERIM PLACEHOLDER for a future constituents-based figure; it assumes
-- non-voters split like voters, so it is for display, not analysis.
-- total_votes_cast, votes_received, and br_candidacy_id are cast here (not in
-- staging) because the upstream mart columns have no shared staging ancestor.
with
    general_winning_stages as (
        select
            br_candidacy_id, gp_election_stage_id, votes_received, election_stage_date
        from {{ ref("candidacy_stage") }}
        where is_winner and lower(election_stage) like '%general%'
    ),

    official_office_wins as (
        select
            eot.gp_elected_official_id,
            eot.br_position_id,
            ws.votes_received,
            ws.gp_election_stage_id,
            ws.election_stage_date
        from {{ ref("elected_official_terms") }} as eot
        inner join
            general_winning_stages as ws
            on cast(eot.br_candidacy_id as string) = ws.br_candidacy_id
    ),

    latest_win_per_office as (
        select *
        from official_office_wins
        qualify
            row_number() over (
                partition by gp_elected_official_id, br_position_id
                order by election_stage_date desc nulls last, gp_election_stage_id desc
            )
            = 1
    ),

    joined as (
        select
            lw.gp_elected_official_id,
            lw.br_position_id,
            es.number_of_seats,
            try_cast(lw.votes_received as bigint) as votes_received,
            try_cast(
                nullif(es.total_votes_cast, 'uncontested') as bigint
            ) as total_votes_cast,
            pos.icp_voter_count
        from latest_win_per_office as lw
        inner join
            {{ ref("election_stage") }} as es
            on lw.gp_election_stage_id = es.gp_election_stage_id
        left join
            {{ ref("positions") }} as pos
            on lw.br_position_id = pos.br_position_database_id
    ),

    scored as (
        select
            gp_elected_official_id,
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
        from joined
        where
            votes_received > 0
            and total_votes_cast > 0
            and votes_received <= total_votes_cast
            and icp_voter_count > 0
            and number_of_seats is not null
    )

-- Drop rows whose projected support rounds to zero (a tiny district with a low
-- share); a zero support number is not meaningful for the banner. Combined with
-- the votes_received > 0 and icp_voter_count > 0 guards above, every column is
-- guaranteed positive.
select
    gp_elected_official_id,
    br_position_id,
    number_of_seats,
    votes_received,
    total_votes_cast,
    icp_voter_count,
    projected_registered_supporters
from scored
where projected_registered_supporters > 0
