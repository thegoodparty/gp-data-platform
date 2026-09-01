{{ config(materialized="view", tblproperties={}) }}

-- Viability 2.0 -> HubSpot Company import feed (DATA-1976). Companion to
-- `candidacy_hubspot`: that feed CREATES contacts already carrying a score, this
-- one backfills the score onto companies that exist. Match on Record ID.
--
-- Score only. The five model inputs are deliberately not written: gp-api reads
-- incumbent and number_of_opponents back OUT of HubSpot via webhook (omni
-- `crm.types.ts`), so writing them would overwrite the inputs feeding the model.
--
-- A view, against the schema's table default: ops reads it immediately before an
-- import and wants current scores. Runs in under a second, so the sibling feeds'
-- reason for materializing (heavy anti-joins re-run by every test) does not apply.
-- Empty tblproperties: the schema's Delta column mapping is meaningless on a view.
with
    exploded as (
        select
            -- JSON array string, not a Spark array, and not single-element
            explode(from_json(hubspot_company_ids, 'array<string>')) as record_id,
            gp_candidacy_id,
            viability_score,
            updated_at,
            -- null once every stage is in the past
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

    -- The 2025 HubSpot archive reuses company ids across cycles, so one id can be
    -- carried by several candidacies that disagree on the score. Live cycle wins.
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

    theirs as (
        select
            cast(id as string) as record_id,
            try_cast(viability_score as double) as hs_score
        from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
    )

-- INNER join: a Record ID missing from the ingest no longer exists in HubSpot and
-- would only error on import. round() so float noise is not read as a change.
select
    o.record_id as `Record ID`,
    o.viability_score as viability_2_0,
    t.hs_score as hubspot_current_value,
    current_timestamp() as added_to_mart_at
from ours as o
join theirs as t on o.record_id = t.record_id
where t.hs_score is null or round(t.hs_score, 2) <> round(o.viability_score, 2)
