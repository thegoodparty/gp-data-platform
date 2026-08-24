-- Unfiltered pass-through of the L2 nationwide voter file for the gp-api
-- application, which queries this mart directly over SQL. Carries the full L2
-- record, PII included, so read access is scoped to the mart_gp_api_readers
-- group rather than all data users.
select * from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
