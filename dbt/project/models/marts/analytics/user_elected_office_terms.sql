{{ config(materialized="view") }}

/*
    The term dates of each gp-api user who is a matched elected official, for
    the Serve clock on the User lookup page. Officeholder terms are public
    record; nothing else from the civics mart is carried over.
*/
select gp_api_user_id as user_id, term_start_date, term_end_date
from {{ ref("elected_officials") }}
where gp_api_user_id is not null
