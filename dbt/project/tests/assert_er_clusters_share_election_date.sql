-- Cluster integrity at candidacy_stage grain.
--
-- Each cluster from the candidacy-stage entity resolution pipeline must
-- represent a single (candidate, race, stage) triple, and the stage is keyed
-- by election_date. If a cluster contains records from multiple election
-- dates, the matcher transitively merged a candidate's primary, runoff, or
-- general candidacies into one cluster — a false positive that the dbt code
-- relies on never happening (the marts use the cluster_id-derived
-- gp_candidacy_stage_id as a stage-unique key when joining BR/TS/DDHQ).
--
-- Each row this test returns is a cluster_id that violates the invariant.
select cluster_id, count(distinct election_date) as n_distinct_dates
from {{ source("er_source", "clustered_candidacy_stages") }}
where election_date is not null
group by cluster_id
having count(distinct election_date) > 1
