-- Cluster integrity at candidacy_stage grain.
--
-- Each cluster from the candidacy-stage entity resolution pipeline must
-- represent a single (candidate, race, stage) triple. matcha matches
-- election_date within a 10-day window (ELECTION_DATE_WINDOW_DAYS in
-- matcha/scripts/configs/candidacy.py, the contract's source of truth,
-- mirrored by the er_election_date_window_days var; update both together), so
-- one stage reported a few days apart across sources lands in one cluster,
-- while distinct stages of a race (primary, runoff, general) sit three or
-- more weeks apart and must never merge. A date spread wider than the window
-- means the matcher transitively merged distinct stages, a false positive
-- the dbt code relies on never happening (the marts use the cluster-derived
-- gp_candidacy_stage_id as a stage-unique key when joining BR/TS/DDHQ).
--
-- Stage labels are deliberately not compared: sources label the same stage
-- inconsistently, so the date window is the stage-identity contract.
--
-- Each row this test returns is a cluster_id that violates the invariant.
select
    cluster_id,
    count(distinct election_date) as n_distinct_dates,
    datediff(
        max(cast(election_date as date)), min(cast(election_date as date))
    ) as spread_days
from {{ source("er_source", "clustered_candidacy_stages") }}
where election_date is not null
group by cluster_id
having
    datediff(max(cast(election_date as date)), min(cast(election_date as date)))
    > {{ var("er_election_date_window_days") }}
