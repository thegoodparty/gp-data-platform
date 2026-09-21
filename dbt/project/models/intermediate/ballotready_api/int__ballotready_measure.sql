{{ config(materialized="view") }}

-- Projection over staging; the contract in int__ballotready_sql.yaml keeps the schema
-- stable
-- for consumers.
select
    database_id,
    id,
    name,
    title,
    summary,
    text,
    pro_snippet,
    con_snippet,
    state,
    geo_id,
    mtfcc,
    has_unknown_boundaries,
    election_database_id,
    election_day,
    election_name,
    issue_database_id,
    party_database_id,
    arguments,
    endorsements,
    created_at,
    updated_at,
    extracted_at,
    dag_run_id
from {{ ref("stg_airflow_source__ballotready_measure_raw") }}
