-- Person-level incumbency for a Win signup, at user grain.
--
-- Two readings of "incumbent" ship side by side because they answer different
-- questions and are knowable at different times:
--
-- 1. The inc_* block: is this person holding ANY office at the moment they
-- signed up. Gated to terms that had already begun at signup, which is the
-- whole point -- a term beginning after signup is frequently the term won
-- in the very election a signup-anchored model is trying to predict, so
-- counting it would feed the outcome back in as an input.
-- 2. is_incumbent_at_election: the governed office-first flag, which asks
-- whether this signup holds the SEAT THEY ARE RUNNING FOR on election day.
-- Strictly later knowledge, so it is labelled non-point-in-time and must
-- not enter a model anchored at signup.
--
-- The inc_* match is on (state, upper first name, upper last name) against the
-- BR term mart. Common names collide. The collision count is deliberately not
-- promoted here: name commonness leaks into a corroboration label built by
-- name-matched entity resolution. It lives in the project's diagnostics.
with
    -- Output spine. A user with campaigns in two states contributes two match
    -- keys below but still gets exactly one row here.
    users as (
        select u.user_id, min(cast(u.user_created_at as date)) as signup_date
        from {{ ref("users_win_candidacy") }} as u
        where
            u.is_latest_version
            and not coalesce(u.is_demo, false)
            and u.user_first_name is not null
            and u.user_last_name is not null
            and u.campaign_state is not null
        group by u.user_id
    ),

    -- Match keys: every (name, declared state) a user presented.
    match_keys as (
        select distinct
            s.user_id,
            s.signup_date,
            upper(trim(u.user_first_name)) as first_name,
            upper(trim(u.user_last_name)) as last_name,
            u.campaign_state
        from {{ ref("users_win_candidacy") }} as u
        inner join users as s on s.user_id = u.user_id
        where
            u.is_latest_version
            and not coalesce(u.is_demo, false)
            and u.user_first_name is not null
            and u.user_last_name is not null
            and u.campaign_state is not null
    ),

    -- Every position this user declared, across their campaigns. Holding the
    -- seat they are running for is the strongest reading, so any declared
    -- position counts.
    declared_positions as (
        select distinct
            u.user_id, cast(u.ballotready_position_id as bigint) as br_position_id
        from {{ ref("users_win_candidacy") }} as u
        where
            u.is_latest_version
            and not coalesce(u.is_demo, false)
            and u.ballotready_position_id is not null
    ),

    terms as (
        select
            upper(trim(t.first_name)) as first_name,
            upper(trim(t.last_name)) as last_name,
            t.state,
            t.br_position_id,
            t.br_office_holder_id,
            t.term_start_date,
            t.term_end_date
        from {{ ref("elected_official_terms") }} as t
        where
            t.first_name is not null and t.last_name is not null and t.state is not null
    ),

    matched as (
        select
            p.user_id,
            count(distinct t.br_office_holder_id) as n_offices,
            min(t.term_start_date) as earliest_term_start,
            max(t.term_end_date) as latest_term_end,
            max(
                case when d.user_id is not null then 1 else 0 end
            ) as holds_same_position,
            -- A term with no recorded end reads as not-serving here, matching
            -- the definition this feature was accepted under. The governed
            -- election-day flag below reads the same null as still-serving.
            max(
                case when t.term_end_date >= p.signup_date then 1 else 0 end
            ) as serving_at_signup
        from match_keys as p
        inner join
            terms as t
            on t.first_name = p.first_name
            and t.last_name = p.last_name
            and t.state = p.campaign_state
            -- The point-in-time gate.
            and t.term_start_date <= p.signup_date
        left join
            declared_positions as d
            on d.user_id = p.user_id
            and d.br_position_id = t.br_position_id
        group by p.user_id
    ),

    -- The governed office-first flag, campaign grain, reduced to the same
    -- campaign the rest of the feature layer describes.
    governed as (
        select u.user_id, inc.is_incumbent, inc.seat_key_source
        from {{ ref("users_win_candidacy") }} as u
        inner join
            {{ ref("int__civics_campaign_incumbency") }} as inc
            on u.campaign_id = inc.campaign_id
            and u.election_date = inc.election_date
        where u.is_latest_version and not coalesce(u.is_demo, false)
        qualify
            row_number() over (
                partition by u.user_id
                order by u.election_date desc nulls last, u.campaign_id desc
            )
            = 1
    )

select
    p.user_id,
    p.signup_date,

    cast(m.user_id is not null as int) as inc_is_officeholder_at_signup,
    cast(coalesce(m.serving_at_signup, 0) as double) as inc_serving_at_signup,
    cast(coalesce(m.holds_same_position, 0) as double) as inc_holds_same_position,
    -- A genuine zero, not a sentinel: the join is exhaustive over the term
    -- mart, so no match means no holder record with this name in this state.
    cast(coalesce(m.n_offices, 0) as double) as inc_n_offices,
    cast(datediff(p.signup_date, m.earliest_term_start) as double)
    / 365.25 as inc_years_in_office_at_signup,
    cast(datediff(m.latest_term_end, p.signup_date) as double) as inc_days_to_term_end,
    cast(
        datediff(m.latest_term_end, p.signup_date) between 0 and 365 as double
    ) as inc_term_ends_within_1y,

    -- Not point-in-time at signup. Three-valued, as the governed model emits:
    -- true / false / null when no term covers election day or the seat does
    -- not resolve.
    g.is_incumbent as is_incumbent_at_election,
    g.seat_key_source as incumbency_seat_key_source
from users as p
left join matched as m on m.user_id = p.user_id
left join governed as g on g.user_id = p.user_id
