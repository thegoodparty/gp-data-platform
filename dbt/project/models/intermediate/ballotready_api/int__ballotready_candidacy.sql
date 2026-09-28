{{ config(materialized="view") }}

-- A projection over staging, not a copy: staging already did the parsing
-- and casting. This int__ layer is the stable interface other models are
-- written against; the contract in int__ballotready_sql.yaml enforces that
-- its name and schema stay put.
select
    candidate_database_id,
    created_at,
    database_id,
    election_database_id,
    endorsements,
    id,
    is_certified,
    is_hidden,
    parties,
    position_database_id,
    race_database_id,
    result,
    stances,
    updated_at,
    withdrawn,
    feed_extracted_at
from {{ ref("stg_airflow_source__ballotready_candidacy_raw") }}
