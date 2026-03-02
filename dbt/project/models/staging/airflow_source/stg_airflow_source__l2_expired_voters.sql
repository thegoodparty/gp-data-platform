with source as (select * from {{ source("airflow_source", "l2_expired_voters") }})

select *
from source
