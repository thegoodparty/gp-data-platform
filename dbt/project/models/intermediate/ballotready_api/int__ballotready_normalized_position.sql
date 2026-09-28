{{ config(materialized="view") }}

-- A projection over staging, not a copy: staging already did the parsing
-- and casting. This int__ layer is the stable interface other models are
-- written against; the contract in int__ballotready_sql.yaml enforces that
-- its name and schema stay put.
select database_id, description, id, issues, mtfcc, name, created_at, updated_at
from {{ ref("stg_airflow_source__ballotready_normalized_position_raw") }}
