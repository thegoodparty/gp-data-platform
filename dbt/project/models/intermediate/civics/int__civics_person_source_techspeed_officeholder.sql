-- TechSpeed office-holder person projection: one row per ts_officeholder_id.
-- The deterministic link to a BallotReady person (ts_officeholder_id ==
-- br_office_holder_id) is carried as br_candidate_id, but suppressed for the
-- ids flagged reused across different people (the existing reuse guard in
-- int__civics_elected_official_canonical_ids). Reused ids still project as
-- office-holder persons, just without a false deterministic BR link. All-time;
-- first_seen_at is the earliest parsed date_processed.
with
    ts as (
        select
            cast(ts_officeholder_id as string) as ts_officeholder_id,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            upper(trim(state)) as state,
            nullif(lower(trim(email)), '') as email,
            nullif(trim(phone), '') as phone,
            nullif(lower(trim(office_name)), '') as office_name,
            date_processed_date as processed_date,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        where ts_officeholder_id is not null
    ),

    crosswalk as (
        select
            cast(ts_officeholder_id as string) as ts_officeholder_id,
            cast(br_candidate_id as string) as br_candidate_id,
            ts_officeholder_id_is_reused
        from {{ ref("int__civics_elected_official_canonical_ids") }}
    ),

    representative as (
        select ts_officeholder_id, first_name, last_name, state
        from ts
        qualify
            row_number() over (
                partition by ts_officeholder_id
                order by processed_date desc nulls last, _airbyte_extracted_at desc
            )
            = 1
    ),

    aggregated as (
        select
            ts_officeholder_id,
            cast(
                min(
                    coalesce(cast(processed_date as timestamp), _airbyte_extracted_at)
                ) as timestamp
            ) as first_seen_at,
            max(email) as email,
            max(phone) as phone,
            array_sort(collect_set(office_name)) as office_names,
            -- Office-holder rows carry no candidacy or race ids; typed empties.
            collect_set(cast(null as string)) as candidacy_source_ids,
            collect_set(cast(null as string)) as race_ids,
            collect_set(cast(null as date)) as election_dates
        from ts
        group by ts_officeholder_id
    )

select
    'techspeed_officeholder' as source_name,
    r.ts_officeholder_id as source_id,
    case
        when not coalesce(x.ts_officeholder_id_is_reused, false) then x.br_candidate_id
    end as br_candidate_id,
    r.ts_officeholder_id,
    cast(null as string) as techspeed_candidate_code,
    cast(null as string) as ddhq_candidate_id,
    r.first_name,
    r.last_name,
    cast(null as string) as middle_name,
    nullif(trim(concat_ws(' ', r.first_name, r.last_name)), '') as full_name,
    a.email,
    a.phone,
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
join aggregated as a using (ts_officeholder_id)
left join crosswalk as x using (ts_officeholder_id)
