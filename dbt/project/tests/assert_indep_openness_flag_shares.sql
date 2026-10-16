-- The research notebook sets each state's three-way cut (single_*, multi_*) at the
-- highest level that still flags at least 10% of every state house district of
-- 1,000+ voters, so each of those columns flags at least ~10% of the state.
-- Head-to-head cuts (donly_*, ronly_*) are a fixed probability with no floor.
-- A column flagging no one or everyone in a state points at a cut that was not
-- built for these scores. New registrants flagged against the frozen cuts drift
-- shares between rescores, hence warn rather than error.
{{ config(severity="warn") }}

with
    flags as (
        select state, scenario, flag
        from
            {{ ref("int__indep_openness_voter_scores") }} unpivot (
                flag for scenario in (
                    indep_openness_single_inc,
                    indep_openness_single_open,
                    indep_openness_multi_inc,
                    indep_openness_multi_open,
                    indep_openness_donly_inc,
                    indep_openness_donly_open,
                    indep_openness_ronly_inc,
                    indep_openness_ronly_open,
                    indep_openness_donly_multi,
                    indep_openness_ronly_multi
                )
            )
    ),
    shares as (
        select state, scenario, avg(case when flag then 1.0 else 0.0 end) as share_true
        from flags
        group by 1, 2
    )

select state, scenario, share_true
from shares
where
    share_true in (0, 1)
    or (
        scenario in (
            'indep_openness_single_inc',
            'indep_openness_single_open',
            'indep_openness_multi_inc',
            'indep_openness_multi_open'
        )
        and share_true < 0.09
    )
