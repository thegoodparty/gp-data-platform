-- DDHQ general-winner votes matched to each elected official via the matcha
-- elected_official entity resolution. A DDHQ winner shares a Splink cluster with
-- the official's BallotReady term and/or gp-api office; this attaches that
-- winner's OWN votes (ddhq_votes) to those records. Surnames must match within
-- the cluster, which guards against the multi-person clusters that ER false
-- positives can produce (the cluster can contain more than one real person).
--
-- Long format: one row per matched record, keyed by EITHER br_office_holder_id
-- (BR term) OR gp_api_elected_office_id (gp-api office). BR rows feed
-- elected_official_terms (so every official, not just gp-api ones, can carry
-- DDHQ votes); gp-api rows feed the election_api support score, including
-- gp_api-only offices that have no BR term on the BR-spine terms mart.
with
    clustered as (
        select * from {{ ref("stg_er_source__clustered_elected_officials") }}
    ),

    ddhq as (
        select cluster_id, lower(trim(last_name)) as last_name, ddhq_votes
        from clustered
        where source_name = 'ddhq' and ddhq_votes is not null
    ),

    -- DDHQ votes attached to each BR term in the same cluster + surname.
    br_matched as (
        select
            c.br_office_holder_id,
            cast(null as string) as gp_api_elected_office_id,
            d.ddhq_votes as ddhq_winning_votes
        from clustered as c
        inner join
            ddhq as d
            on c.cluster_id = d.cluster_id
            and lower(trim(c.last_name)) = d.last_name
        where
            c.source_name = 'ballotready_techspeed'
            and c.br_office_holder_id is not null
        qualify
            row_number() over (
                partition by c.br_office_holder_id order by d.ddhq_votes desc
            )
            = 1
    ),

    -- DDHQ votes attached to each gp-api office in the same cluster + surname.
    gp_matched as (
        select
            cast(null as int) as br_office_holder_id,
            c.gp_api_elected_office_id,
            d.ddhq_votes as ddhq_winning_votes
        from clustered as c
        inner join
            ddhq as d
            on c.cluster_id = d.cluster_id
            and lower(trim(c.last_name)) = d.last_name
        where c.source_name = 'gp_api' and c.gp_api_elected_office_id is not null
        qualify
            row_number() over (
                partition by c.gp_api_elected_office_id order by d.ddhq_votes desc
            )
            = 1
    )

select br_office_holder_id, gp_api_elected_office_id, ddhq_winning_votes
from br_matched
union all
select br_office_holder_id, gp_api_elected_office_id, ddhq_winning_votes
from gp_matched
