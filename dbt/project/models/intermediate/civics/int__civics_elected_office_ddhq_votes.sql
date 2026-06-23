{{ config(materialized="table", tags=["civics", "ddhq"]) }}

-- DDHQ general-winner votes resolved to each gp-api elected office via the
-- matcha elected_official entity resolution. The DDHQ winner record that shares
-- a cluster_id with the gp_api office carries the official's OWN winning votes
-- (ddhq_votes); this attaches them to the office. One row per
-- gp_api_elected_office_id (the official's most recent winning cycle).
-- This is the single place the cluster -> DDHQ-votes link lives; civics
-- elected_official_terms surfaces it, and the election_api support score reads
-- it from there (with this model as the gp_api-only supplement for offices that
-- have no BallotReady term to ride on the BR-spine terms mart).
with cluster as (select * from {{ ref("stg_er_source__clustered_elected_officials") }})

select g.gp_api_elected_office_id, d.ddhq_votes as ddhq_winning_votes
from cluster as g
inner join
    cluster as d
    on g.cluster_id = d.cluster_id
    and d.source_name = 'ddhq'
    and d.ddhq_votes is not null
where g.source_name = 'gp_api' and g.gp_api_elected_office_id is not null
qualify
    row_number() over (
        partition by g.gp_api_elected_office_id
        order by d.term_start_date desc nulls last, d.ddhq_votes desc
    )
    = 1
