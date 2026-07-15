{{ config(materialized="view") }}

select *
from {{ ref("int__amplitude_event_catalog") }}
