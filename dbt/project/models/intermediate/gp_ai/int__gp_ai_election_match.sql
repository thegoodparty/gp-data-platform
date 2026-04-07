{{
    config(
        materialized="table",
        auto_liquid_cluster=true,
        tags=["intermediate", "gp_ai", "election_match"],
    )
}}

-- DEPRECATED (DATA-1834): Frozen snapshot of DDHQ-HubSpot AI election match results.
-- Previously a Python model that fetched parquet from S3 via gp-ai API.
-- Now sources from the Dec 2, 2025 manual snapshot — the same data that was
-- already in production (no API run ever succeeded; fallback was always active).
-- Splink modelling replaces this pipeline going forward.
with
    snapshot as (
        select *, 'manual_run' as run_id
        from {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251202") }}
    ),

    deduped as (
        select *
        from snapshot
        qualify
            row_number() over (
                partition by gp_candidacy_id, election_date, election_type
                order by row_index desc
            )
            = 1
    )

select *
from deduped
