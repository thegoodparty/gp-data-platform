{{
    config(
        materialized="view",
        tags=["intermediate", "hubspot","ballotready"]
    )
}}
-- this file calculates whether races are contested or not by combining BallotReady and Hubspot upstream data


-- count of ballotready candidates per contest
WITH brnumcands AS (
    SELECT 
       br_contest_id AS contest_id,
       number_of_seats_available,
       COUNT(last_name) AS numcands
    FROM {{ ref('int__ballotready_cleancandidacies') }}
    WHERE
        official_office_name is not null and 
        election_date is not null 
    GROUP BY  contest_id, number_of_seats_available
),

-- count of hubspot candidates per contest 
hsnumcands AS (
    SELECT
        lower(concat_ws('__',
            regexp_replace(regexp_replace(trim(properties_official_office_name), ' ', '-'), '[^a-zA-Z0-9-]', ''),
            regexp_replace(regexp_replace(trim(properties_election_date), ' ', '-'), '[^a-zA-Z0-9-]', '')
        )) as contest_id,
        COUNT(properties_lastname) AS numcands,
        CAST(properties_number_of_seats_available AS INT) AS number_of_seats_available
    FROM {{ ref('int__hubspot_ytd_candidacies') }}
    WHERE
        properties_official_office_name IS NOT NULL AND
        properties_election_date IS NOT NULL 
    GROUP BY contest_id, number_of_seats_available
),


-- merged counts of candidates per contest,
-- note in the following code to ensure a single value per contest_id the number_of_seats variable will be taken from HS > BR 
unioned AS (
    SELECT
        contest_id,
        numcands,
        number_of_seats_available,
        2 AS source_priority
    FROM brnumcands
    UNION ALL
    SELECT
        contest_id,
        numcands,
        number_of_seats_available,
        1 AS source_priority
    FROM hsnumcands
    WHERE contest_id IN (SELECT contest_id FROM brnumcands)
),
ranked AS (
    SELECT *,
         ROW_NUMBER() OVER (PARTITION BY contest_id ORDER BY source_priority) AS row_num
    FROM unioned
),
seat_source AS (
    SELECT contest_id, number_of_seats_available
    FROM ranked
    WHERE row_num = 1
),
counted AS (
    SELECT contest_id, SUM(numcands) AS numcands
    FROM unioned
    GROUP BY contest_id
),
totalnumcands as (
    SELECT
        c.contest_id,
        c.numcands,
        s.number_of_seats_available
    FROM counted c
    LEFT JOIN seat_source s USING (contest_id)
),

-- final step determine which contest are contested/uncontested
uncontested as (
    SELECT
        contest_id,
        CASE
            WHEN numcands > number_of_seats_available THEN 'Contested'
            WHEN numcands <= number_of_seats_available THEN 'Uncontested'
            ELSE ''
        END AS uncontested,
        numcands,
        number_of_seats_available
    FROM totalnumcands
)

select * from uncontested
