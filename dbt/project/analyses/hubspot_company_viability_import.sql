-- Viability 2.0 -> HubSpot Company import file (DATA-1976).
--
-- A dbt *analysis*: compiled and schema-validated, never materialized. Run the
-- compiled SQL ad hoc, export the result as CSV, and import it into HubSpot
-- Companies matching on Record ID, mapping `Viability 2.0` to the existing
-- `viability_2_0` company property.
--
-- SCOPE, per the DATA-1976 decisions: the score only, and the Company object
-- only. The five model INPUTS (is_incumbent, open_seat, partisan_contest,
-- seats_available, number_of_opponents) are deliberately NOT written. Three of
-- them are properties gp-api reads back OUT of HubSpot via webhook (see the
-- IncomingProperty enum in omni's `crm.types.ts`), so writing them from here
-- would loop them into the product campaign and overwrite the product-owned
-- inputs that feed the model. Product owns the inputs; Databricks owns the score.
--
-- Delivery is a manual import. An Airflow DAG pushing this same result set
-- through the HubSpot batch-update API is the follow-up that gives it a daily
-- cadence; keeping the changed-only filter below means that DAG can read this
-- query unchanged.
--
-- COMPILE AGAINST PROD. The refs resolve to whatever target you compile with, so
-- a dev compile silently reads your personal schema and can hand ops a file built
-- from stale relations. Check the schema names in the compiled SQL before running.
--
-- Two things worth knowing about the shape of the data:
-- - `hubspot_company_ids` is a JSON array STRING, not a Spark array, and is not
-- single-element: up to 11 ids sit on one candidacy. Empty is the literal '[]'.
-- - A company id can be carried by several candidacies, because the 2025 HubSpot
-- archive reuses ids across cycles. As of 2026-09-01, 610 of 30,360 ids map to
-- more than one candidacy and for 413 of those the candidacies disagree on the
-- score, so the winner has to be deterministic rather than an arbitrary pick:
-- the live cycle beats an archived one.
with
    exploded as (
        select
            explode(from_json(hubspot_company_ids, 'array<string>')) as record_id,
            gp_candidacy_id,
            viability_score,
            updated_at,
            -- nearest UPCOMING stage date; null once every stage is in the past
            least(
                case
                    when primary_election_date >= current_date()
                    then primary_election_date
                end,
                case
                    when general_election_date >= current_date()
                    then general_election_date
                end
            ) as next_election_date,
            greatest(
                primary_election_date, general_election_date
            ) as latest_election_date
        from {{ ref("candidacy_scored") }}
        where
            viability_score is not null
            and hubspot_company_ids is not null
            and trim(hubspot_company_ids) not in ('', '[]')
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by record_id
                order by
                    case when next_election_date is null then 1 else 0 end,
                    next_election_date asc,
                    latest_election_date desc,
                    updated_at desc,
                    gp_candidacy_id
            ) as rn
        from exploded
        where record_id is not null and trim(record_id) <> ''
    ),

    ours as (select record_id, viability_score from ranked where rn = 1),

    -- Current stored values, for the changed-only filter. Same continuous 0-5
    -- scale at 2dp on both sides, so the comparison needs no rescaling -- only a
    -- round() to keep float representation from reporting a change that is not one.
    theirs as (
        select
            cast(id as string) as record_id,
            try_cast(viability_score as double) as hs_score
        from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
    )

-- INNER join, never LEFT: a Record ID absent from the HubSpot companies ingest no
-- longer exists in HubSpot (deleted, or newer than the last sync) and would only
-- error on import. 32 rows were dropped this way on 2026-09-01.
select o.record_id as `Record ID`, o.viability_score as `Viability 2.0`
from ours as o
join theirs as t on o.record_id = t.record_id
where t.hs_score is null or round(t.hs_score, 2) <> round(o.viability_score, 2)
order by o.record_id
