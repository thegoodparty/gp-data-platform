{{ config(materialized="view", tags=["intermediate", "hubspot", "ballotready"]) }}
-- this file calculates whether races are contested or not by combining BallotReady
-- and Hubspot upstream data
-- count of ballotready candidates per contest
with
    brnumcands as (
        select
            br_contest_id as contest_id,
            number_of_seats_available,
            count(last_name) as numcands
        from {{ ref("int__ballotready_clean_candidacies") }}
        where official_office_name is not null and election_date is not null
        group by contest_id, number_of_seats_available
    ),

    -- count of hubspot candidates per contest
    hsnumcands as (
        select
            lower(
                concat_ws(
                    '__',
                    regexp_replace(
                        regexp_replace(trim(properties_official_office_name), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    ),
                    regexp_replace(
                        regexp_replace(trim(properties_election_date), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    )
                )
            ) as contest_id,
            count(properties_lastname) as numcands,
            cast(
                properties_number_of_seats_available as int
            ) as number_of_seats_available
        from {{ ref("int__hubspot_ytd_candidacies") }}
        where
            properties_official_office_name is not null
            and properties_election_date is not null
        group by contest_id, number_of_seats_available
    ),

    -- merged counts of candidates per contest,
    -- note in the following code to ensure a single value per contest_id the
    -- number_of_seats variable will be taken from HS > BR
    unioned as (
        select contest_id, numcands, number_of_seats_available, 2 as source_priority
        from brnumcands
        union all
        select contest_id, numcands, number_of_seats_available, 1 as source_priority
        from hsnumcands
        where contest_id in (select contest_id from brnumcands)
    ),
    ranked as (
        select
            *,
            row_number() over (
                partition by contest_id order by source_priority
            ) as row_num
        from unioned
    ),
    seat_source as (
        select contest_id, number_of_seats_available from ranked where row_num = 1
    ),
    counted as (
        select contest_id, sum(numcands) as numcands from unioned group by contest_id
    ),
    totalnumcands as (
        select c.contest_id, c.numcands, s.number_of_seats_available
        from counted c
        left join seat_source s using (contest_id)
    ),

    -- final step determine which contest are contested/uncontested
    uncontested as (
        select
            contest_id,
            case
                when numcands > number_of_seats_available
                then 'Contested'
                when numcands <= number_of_seats_available
                then 'Uncontested'
                else ''
            end as uncontested,
            numcands,
            number_of_seats_available
        from totalnumcands
    )

select *
from uncontested
