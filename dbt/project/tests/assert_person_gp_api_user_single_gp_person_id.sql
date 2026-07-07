-- A gp_api user id must map to at most one gp_person_id.
with
    gp_api as (
        select substring_index(record_key, '|', -1) as gp_api_user_id, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'gp_api'
    )

select gp_api_user_id, count(distinct gp_person_id) as distinct_ids
from gp_api
group by gp_api_user_id
having count(distinct gp_person_id) > 1
