{{ config(materialized="view") }}

-- A projection over staging, not a copy. Staging already did the parsing and casting;
-- this exists so the 23-plus downstream refs to int__ballotready_geofence keep working
-- unchanged. The contract in int__ballotready_sql.yaml is what guarantees they do.
select created_at, database_id, geo_id, id, mtfcc, updated_at, valid_from, valid_to
from {{ ref("stg_airflow_source__ballotready_geofence_raw") }}
