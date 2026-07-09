-- DATA-2015: int__voter_turnout_lgbm_inference builds ONE opp_{year} opportunity flag
-- per year from EITHER AnyElection_{year} OR OtherElection_{year} rows. That collapse
-- is prefix-safe only because the opportunity source obeys the L2 parity invariant:
-- AnyElection rows are odd-year-only and OtherElection rows are even-year-only
-- (verified across ~2.9M rows on 2026-07-06). A load that violates parity would
-- silently change vote-history eligibility, so fail loudly here instead.
-- Also asserts NH/VT rows carry non-NULL Precinct values: the inference model joins
-- on a ward-coalesce expression for NH/VT, so NULL precincts in this table would
-- silently zero all NH/VT OtherElection opportunity flags.
-- The table is loaded outside dbt (promoted alongside the model versions), so it is
-- addressed directly; the schema mirrors the model's PROD default
-- (voter_turnout_precincts_schema = model_predictions).
-- depends_on: {{ ref("int__voter_turnout_lgbm_inference") }}
with
    parity_violations as (
        select election_year_str, count(*) as rows
        from goodparty_data_catalog.model_predictions.turnout_historical_precincts
        where
            (
                election_year_str rlike '^AnyElection_[0-9]{4}$'
                and pmod(
                    cast(regexp_extract(election_year_str, '([0-9]{4})$', 1) as int), 2
                )
                = 0
            )
            or (
                election_year_str rlike '^OtherElection_[0-9]{4}$'
                and pmod(
                    cast(regexp_extract(election_year_str, '([0-9]{4})$', 1) as int), 2
                )
                = 1
            )
        group by election_year_str
    ),

    nh_vt_null_precincts as (
        select count(*) as null_precinct_rows
        from goodparty_data_catalog.model_predictions.turnout_historical_precincts
        where state in ('NH', 'VT') and precinct is null
    )

select election_year_str as violation, cast(rows as string) as detail
from parity_violations
union all
select 'NH_VT_NULL_PRECINCT' as violation, cast(null_precinct_rows as string) as detail
from nh_vt_null_precincts
where null_precinct_rows > 0
