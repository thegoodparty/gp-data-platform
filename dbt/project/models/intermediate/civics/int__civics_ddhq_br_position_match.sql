-- DDHQ race -> BallotReady position, by office identity rather than by race.
-- One row per 2026+ DDHQ election-stage key with exactly one BR position in its
-- state carrying the same office key (office_match_keys: locality tokens,
-- office category, seat). Covers races BR has the office for but no race on
-- that date, which race-level ER cannot reach, and gives election-stage ER a
-- position anchor for the ones it misses. A key matching two or more positions
-- is left out: for an identity crosswalk a missed match is safer than a wrong
-- one.
--
-- Seats: numbered seats must be equal. An unnumbered or at-large DDHQ race is
-- compatible only with an unnumbered or at-large position, since it cannot say
-- which numbered seat it is.
with
    ddhq_races as (
        select
            {{ generate_ddhq_election_stage_key("ddhq_race_id", "election_stage") }}
            as ddhq_election_stage_key,
            any_value(state_postal_code) as state,
            any_value(official_office_name) as official_office_name
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
        where
            election_date >= '2026-01-01'
            and state_postal_code is not null
            and official_office_name is not null
        group by 1
    ),

    ddhq_keyed as (
        select *, {{ office_match_keys("official_office_name") }} from ddhq_races
    ),

    br_positions as (
        select database_id as br_position_id, upper(state) as state, name
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
        where name is not null and state is not null
    ),

    br_keyed as (select *, {{ office_match_keys("name") }} from br_positions),

    -- at-large reads as unnumbered on both sides.
    ddhq_seated as (
        select
            *,
            case
                when seat_designator = 'atlarge' then null else seat_designator
            end as seat
        from ddhq_keyed
        where size(locality_key) > 0
    ),

    br_seated as (
        select
            *,
            case
                when seat_designator = 'atlarge' then null else seat_designator
            end as seat
        from br_keyed
        where size(locality_key) > 0
    ),

    candidates as (
        select
            d.ddhq_election_stage_key,
            d.official_office_name as ddhq_office_name,
            b.br_position_id,
            b.name as br_position_name
        from ddhq_seated as d
        inner join
            br_seated as b
            on d.state = b.state
            and d.locality_key = b.locality_key
            and d.office_category = b.office_category
            and d.seat <=> b.seat
    )

select
    ddhq_election_stage_key,
    any_value(ddhq_office_name) as ddhq_office_name,
    any_value(br_position_id) as br_position_id,
    any_value(br_position_name) as br_position_name
from candidates
group by ddhq_election_stage_key
having count(distinct br_position_id) = 1
