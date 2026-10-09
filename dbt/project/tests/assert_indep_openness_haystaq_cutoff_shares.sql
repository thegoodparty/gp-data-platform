-- The frozen Haystaq cutoffs should flag 40-41% of each state's voters who have a
-- score. Not exactly 40%: the score is an integer and about 1% of a state sits on the
-- cutoff, all of them flagged. A new Haystaq vintage can drift a state out of the band;
-- that is a prompt to re-cut deliberately, not an error, hence warn.
{{ config(severity="warn") }}

with
    scored as (
        select
            l2.state_postal_code as state,
            l2.hs_partisanship_moderate_third_party_support >= cutoffs.cutoff as flagged
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }} as l2
        inner join
            {{ ref("indep_openness_haystaq_cutoff") }} as cutoffs
            on l2.state_postal_code = cutoffs.state
        where l2.hs_partisanship_moderate_third_party_support is not null
    ),
    shares as (
        select state, avg(case when flagged then 1.0 else 0.0 end) as share_flagged
        from scored
        group by 1
    )

select state, share_flagged
from shares
where share_flagged not between 0.40 and 0.41
