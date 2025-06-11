-- Creates the final set of BallotReady candidacies to be uploaded to HubSpot 

--Add in the contested results 
with br_with_contest as (
    SELECT 
        br.* except (number_of_seats_available),
        cs.uncontested,
        cs.numcands as number_of_candidates,
        cs.number_of_seats_available
    FROM {{ ref('int__ballotready_cleancandidacies') }} br
    LEFT JOIN {{ ref('int__candidacies_contest_final_counts') }} cs
        ON br.br_contest_id = cs.contest_id
),

--Filter in for net new results 
br_new_candidacies as (
    SELECT 
        t1.*
    FROM br_with_contest t1
    WHERE 
        t1.br_candidate_code NOT IN (SELECT hs_candidate_code FROM {{ ref('int__hubspot_candidacy_codes') }})
),

--write formatted new batch data to a table 
br_new_candidacies_final as (
SELECT
    CAST(election_date AS DATE) AS election_date,
    official_office_name,
    candidate_office,
    office_level,
    CAST(candidate_id_tier AS INT) AS candidate_id_tier,
    CAST(number_of_seats_available AS INT) AS number_of_seats_available,
    election_type,
    party_affiliation,
    party_list,
    first_name,
    middle_name,
    last_name,
    state,
    phone,
    email,
    city,
    CASE  
        WHEN district LIKE '%, %' THEN LEFT(district, POSITION(', ' IN district) - 1)
        ELSE district
    END AS district,
    office_type,
    type,
    contact_owner,
    owner_name,
    candidate_id_source,
    uncontested, 
    number_of_candidates,
    current_timestamp() AS created_at
FROM br_new_candidacies
)
select * from br_new_candidacies_final
