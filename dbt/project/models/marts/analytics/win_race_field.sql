-- Every candidate in every 2026 race where a Win user ran, users and not. One
-- row per candidacy-stage. Lets the post-election analysis compare Win users
-- with the rest of their own field, and set each result against the odds the
-- race gave.
--
-- Context columns are all knowable before the race: incumbency, how often the
-- person ran and won before, how close the same office's last race was, and
-- election timing. Nothing here is something the product could influence.
with
    stages as (
        select
            gp_election_stage_id,
            election_date,
            stage_type,
            is_primary,
            is_runoff,
            br_position_id,
            office_level,
            office_type,
            partisan_type,
            number_of_seats,
            -- The civics mart carries vote counts as strings.
            try_cast(total_votes_cast as bigint) as total_votes_cast
        from {{ ref("election_stage") }}
    ),

    candidacy_stages as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            gp_person_id,
            candidate_name,
            candidate_party,
            gp_api_campaign_id,
            election_stage_date,
            is_winner,
            election_result,
            try_cast(votes_received as bigint) as votes_received,
            is_uncontested
        from {{ ref("candidacy_stage") }}
    ),

    win_races as (
        select distinct gp_election_stage_id
        from candidacy_stages
        where gp_api_campaign_id is not null and year(election_stage_date) = 2026
    ),

    campaign_users as (
        select id as campaign_id, user_id
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where not coalesce(is_demo, false)
    ),

    candidacies as (
        select gp_candidacy_id, is_incumbent, is_open_seat from {{ ref("candidacy") }}
    ),

    -- Each person's record before this stage, from every stage we hold.
    person_history as (
        select
            cs.gp_candidacy_stage_id,
            count(distinct prior.gp_candidacy_id) as prior_candidacies,
            count(
                distinct case when prior.is_winner then prior.gp_candidacy_id end
            ) as prior_wins
        from candidacy_stages as cs
        inner join win_races as w on w.gp_election_stage_id = cs.gp_election_stage_id
        left join
            candidacy_stages as prior
            on prior.gp_person_id = cs.gp_person_id
            and prior.election_stage_date < cs.election_stage_date
            -- Earlier stages of the same candidacy are this race, not a prior one.
            and prior.gp_candidacy_id != cs.gp_candidacy_id
        group by cs.gp_candidacy_stage_id
    ),

    -- The same office's most recent earlier race of the same stage type, and
    -- how close it was: the gap in vote share between the last seat won and
    -- the first seat lost.
    prior_stage as (
        select
            s.gp_election_stage_id,
            p.gp_election_stage_id as prior_election_stage_id,
            p.number_of_seats as prior_seats,
            p.total_votes_cast as prior_total_votes
        from stages as s
        inner join win_races as w on w.gp_election_stage_id = s.gp_election_stage_id
        inner join
            stages as p
            on p.br_position_id = s.br_position_id
            and p.stage_type = s.stage_type
            and p.election_date < s.election_date
        qualify
            row_number() over (
                partition by s.gp_election_stage_id order by p.election_date desc
            )
            = 1
    ),

    prior_ranked as (
        select
            ps.gp_election_stage_id,
            ps.prior_seats,
            cs.votes_received / nullif(ps.prior_total_votes, 0) as vote_share,
            row_number() over (
                partition by ps.gp_election_stage_id order by cs.votes_received desc
            ) as finish
        from prior_stage as ps
        inner join
            candidacy_stages as cs
            on cs.gp_election_stage_id = ps.prior_election_stage_id
        where cs.votes_received is not null
    ),

    prior_margin as (
        select
            gp_election_stage_id,
            max(case when finish = coalesce(prior_seats, 1) then vote_share end) - max(
                case when finish = coalesce(prior_seats, 1) + 1 then vote_share end
            ) as prior_race_margin
        from prior_ranked
        group by gp_election_stage_id
    ),

    field_size as (
        select
            gp_election_stage_id,
            count(*) as candidates_in_race,
            count_if(is_winner) as winners_in_race,
            count_if(not is_winner) as losers_in_race
        from candidacy_stages
        group by gp_election_stage_id
    )

select
    cs.gp_candidacy_stage_id,
    cs.gp_election_stage_id,
    cs.gp_candidacy_id,
    cs.gp_person_id,
    cs.candidate_name,
    cs.candidate_party,
    cu.user_id,
    cs.gp_api_campaign_id is not null as is_win_user,

    -- The race
    s.election_date,
    s.stage_type,
    s.is_primary,
    s.is_runoff,
    s.office_level,
    s.office_type,
    s.partisan_type,
    s.number_of_seats,
    fs.candidates_in_race,
    -- Some multi-seat races are recorded with one seat. Once decided, the
    -- winners show how many seats there were, a fact fixed before the vote.
    greatest(coalesce(s.number_of_seats, 1), fs.winners_in_race) as seats_in_race,
    -- A recorded loss means a contest even when civics is missing the opponents.
    fs.candidates_in_race > seats_in_race or fs.losers_in_race > 0 as is_contested,
    -- The share of candidates who win if seats were handed out at random.
    -- Unknown when civics is missing opponents: a loss with no more
    -- candidates than seats.
    case
        when fs.candidates_in_race > seats_in_race or fs.losers_in_race = 0
        then least(seats_in_race / fs.candidates_in_race, 1.0)
    end as baseline_win_chance,
    -- On-cycle Novembers bring different turnout than off-cycle dates.
    month(s.election_date) = 11 and not s.is_primary as is_november_general,
    cs.is_uncontested,

    -- The candidate, before the race
    c.is_incumbent,
    c.is_open_seat,
    coalesce(ph.prior_candidacies, 0) as prior_candidacies,
    coalesce(ph.prior_wins, 0) as prior_wins,
    pm.prior_race_margin,

    -- The result
    cs.election_result,
    cs.is_winner,
    cs.votes_received,
    cs.votes_received / nullif(s.total_votes_cast, 0) as vote_share,
    current_timestamp() as built_at
from candidacy_stages as cs
inner join win_races as w on w.gp_election_stage_id = cs.gp_election_stage_id
inner join stages as s on s.gp_election_stage_id = cs.gp_election_stage_id
inner join field_size as fs on fs.gp_election_stage_id = cs.gp_election_stage_id
left join campaign_users as cu on cu.campaign_id = cs.gp_api_campaign_id
left join candidacies as c on c.gp_candidacy_id = cs.gp_candidacy_id
left join person_history as ph on ph.gp_candidacy_stage_id = cs.gp_candidacy_stage_id
left join prior_margin as pm on pm.gp_election_stage_id = cs.gp_election_stage_id
