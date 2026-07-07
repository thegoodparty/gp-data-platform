-- DDHQ candidate person projection: one row per DDHQ candidate_id. candidate_id
-- is per-race/per-cycle rather than a durable person id: it can span several
-- races within a cycle and is occasionally reused across people, so cross-cycle
-- person identity is resolved by the probabilistic person layer, not here. DDHQ
-- has no native created field, so first_seen_at falls back to the Airbyte
-- extraction time.
with
    ddhq as (
        select
            cast(candidate_id as string) as ddhq_candidate_id,
            lower(trim(candidate_first_name)) as first_name,
            lower(trim(candidate_last_name)) as last_name,
            nullif(lower(trim(candidate_middle_name)), '') as middle_name,
            upper(trim(state_postal_code)) as state,
            nullif(lower(trim(official_office_name)), '') as office_name,
            cast(ddhq_race_id as string) as race_id,
            cast(candidate_id as string)
            || '_'
            || cast(ddhq_race_id as string) as candidacy_source_id,
            election_date,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where candidate_id is not null
    ),

    representative as (
        select ddhq_candidate_id, first_name, last_name, middle_name, state
        from ddhq
        qualify
            row_number() over (
                partition by ddhq_candidate_id
                order by election_date desc nulls last, race_id desc
            )
            = 1
    ),

    aggregated as (
        select
            ddhq_candidate_id,
            cast(min(_airbyte_extracted_at) as timestamp) as first_seen_at,
            array_sort(collect_set(candidacy_source_id)) as candidacy_source_ids,
            array_sort(collect_set(race_id)) as race_ids,
            array_sort(collect_set(office_name)) as office_names,
            array_sort(collect_set(election_date)) as election_dates
        from ddhq
        group by ddhq_candidate_id
    )

select
    'ddhq' as source_name,
    r.ddhq_candidate_id as source_id,
    cast(null as string) as br_candidate_id,
    cast(null as string) as ts_officeholder_id,
    cast(null as string) as techspeed_candidate_code,
    r.ddhq_candidate_id,
    r.first_name,
    r.last_name,
    r.middle_name,
    nullif(
        trim(concat_ws(' ', r.first_name, r.middle_name, r.last_name)), ''
    ) as full_name,
    cast(null as string) as email,
    cast(null as string) as phone,
    r.state,
    cast(null as date) as birth_date,
    cast(null as string) as website_url,
    cast(null as string) as facebook_url,
    cast(null as string) as twitter_url,
    cast(null as string) as linkedin_url,
    a.candidacy_source_ids,
    a.race_ids,
    a.office_names,
    a.election_dates,
    a.first_seen_at
from representative as r
join aggregated as a using (ddhq_candidate_id)
