{{ config(severity="error") }}

-- Fixture test of the census_acs value macros themselves, over literal rows:
-- every documented sentinel on both the estimate and margin side, the
-- controlled-zero margin, deliberate pass-through of wrong-side sentinels
-- (they must survive to fail the staged range walls loudly, never become
-- silent nulls), null propagation through a bracket combination, and one
-- exact root-sum-of-squares case (3,4,12 -> 13; with a controlled 0 term:
-- 3,0,4 -> 5).
with
    scalar_cases as (
        select
            case_name,
            expected_estimate,
            expected_margin,
            {{ census_acs_estimate("raw_value") }} as estimate_result,
            {{ census_acs_moe("raw_value") }} as margin_result
        from
            (
                values
                    ('insufficient_sample', '-999999999', null, null),
                    ('not_applicable', '-888888888', null, null),
                    ('estimate_not_computable', '-666666666', null, -666666666.0),
                    ('controlled', '-555555555', -555555555, 0.0),
                    ('open_ended_median', '-333333333', -333333333, null),
                    ('insufficient_for_margin', '-222222222', -222222222, null),
                    ('ordinary_value', '12345', 12345, 12345.0),
                    ('zero_value', '0', 0, 0.0)
            ) as t(case_name, raw_value, expected_estimate, expected_margin)
    ),

    scalar_violations as (
        select
            case_name as violation,
            concat(
                'estimate=',
                coalesce(cast(estimate_result as string), 'null'),
                ' margin=',
                coalesce(cast(margin_result as string), 'null')
            ) as detail
        from scalar_cases
        where
            estimate_result is distinct from expected_estimate
            or margin_result is distinct from expected_margin
    ),

    composite_cases as (
        select
            case_name,
            expected_sum,
            expected_rss,
            {{ census_acs_estimate_sum(["c1", "c2", "c3"]) }} as sum_result,
            {{ census_acs_moe_rss(["m1", "m2", "m3"]) }} as rss_result
        from
            (
                values
                    ('all_present', '100', '200', '300', '3', '4', '12', 600, 13.0),
                    (
                        'controlled_zero_term',
                        '100',
                        '200',
                        '300',
                        '3',
                        '-555555555',
                        '4',
                        600,
                        5.0
                    ),
                    (
                        'suppressed_component_nulls_the_bracket',
                        '100',
                        '-999999999',
                        '300',
                        '3',
                        '-999999999',
                        '12',
                        null,
                        null
                    )
            ) as t(case_name, c1, c2, c3, m1, m2, m3, expected_sum, expected_rss)
    ),

    composite_violations as (
        select
            case_name as violation,
            concat(
                'sum=',
                coalesce(cast(sum_result as string), 'null'),
                ' rss=',
                coalesce(cast(rss_result as string), 'null')
            ) as detail
        from composite_cases
        where
            sum_result is distinct from expected_sum
            or coalesce(
                abs(rss_result - expected_rss) > 1e-9,
                (rss_result is null) != (expected_rss is null)
            )
    )

select *
from scalar_violations
union all
select *
from composite_violations
