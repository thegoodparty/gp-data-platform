-- Each flag should be true for 40% of its state's voters in every scenario column,
-- because the cutoff is the state's 60th percentile of the same scores. It is an
-- approximate percentile, so allow half a point. New registrants flagged against the
-- frozen cutoff drift this between rescores, which is expected, hence warn rather
-- than error.
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
where abs(share_true - 0.4) > 0.005
