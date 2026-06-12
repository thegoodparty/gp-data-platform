-- Civics mart elected_official_support table.
-- Grain: one row per (elected official, office), keyed by gp_elected_official_id
-- with br_position_id, from that office's most recent GENERAL election win.
-- Primaries are excluded; a primary win does not confer office. 'general' covers
-- general, general special, general runoff, and general special runoff stages. An
-- official who holds more than one distinct office gets one row per office;
-- re-elections to the same office collapse to the most recent win.
-- Population: every column is populated. Only contested wins with a coherent vote
-- tally and an L2 registered-voter count survive. Uncontested winners (no numeric
-- total) are excluded, as are rows where votes exceed the reported total (a
-- BallotReady partial load on a very recent election, treated as unreliable and
-- dropped whole) and rows with no L2 district match.
-- votes_received is DDHQ-canonical via candidacy_stage; total_votes_cast is
-- BallotReady-first (DDHQ fallback) via election_stage. Where both vendors carry a
-- total they are identical, so the BR-first total is used per repo standard.
-- registered_voter_count is the L2 district registered-voter count from
-- int__icp_offices, joined on br_position_id.
-- projected_registered_supporters applies the official's general-election vote
-- share to that registered-voter base. It is an INTERIM PLACEHOLDER for a future
-- constituents-based support figure: when a real constituents count exists, the
-- same vote share will be applied to it instead. The estimate assumes non-voters
-- would split the same way as voters. Fine for a display figure, not for analysis.
-- number_of_seats flags multi-seat / at-large races (about 47% of rows). In those,
-- one winner's votes over the race total is a structurally low share because the
-- electorate is split across several winners, so the support fraction is not
-- comparable to a single-seat race. The consumer should use this to label or
-- adjust. Note the two distinct recent-load cases: rows where votes exceed the
-- reported total are dropped by the where clause (incoherent partial loads), but
-- rows that are merely low yet coherent (votes <= total, a real winner whose
-- recorded tally is small while counts finish loading) are kept and may show very
-- low support fractions.
-- total_votes_cast and votes_received are parsed and cast here, not in staging,
-- because election_stage intentionally keeps total_votes_cast a string that
-- carries the 'uncontested' sentinel, and candidacy_stage keeps votes_received a
-- string; both are mart-layer tables with no shared staging ancestor where the
-- parse could live once. Same reason br_candidacy_id is cast here: it is int on
-- elected_official_terms but string on candidacy_stage.
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
            icp.voter_count as registered_voter_count
        from latest_win_per_office as lw
        inner join
            {{ ref("election_stage") }} as es
            on lw.gp_election_stage_id = es.gp_election_stage_id
        left join
            {{ ref("int__icp_offices") }} as icp
            on lw.br_position_id = icp.br_database_position_id
    )

select
    gp_elected_official_id,
    br_position_id,
    number_of_seats,
    votes_received,
    total_votes_cast,
    registered_voter_count,
    cast(
        round(
            registered_voter_count * votes_received / cast(total_votes_cast as double)
        ) as bigint
    ) as projected_registered_supporters
from joined
where
    votes_received is not null
    and total_votes_cast is not null
    and total_votes_cast > 0
    and votes_received <= total_votes_cast
    and registered_voter_count is not null
    and number_of_seats is not null
