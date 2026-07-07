-- TechSpeed candidate person projection: one row per candidate identity code.
-- TechSpeed exposes no native candidate id, so identity keys off
-- generate_candidate_code (the same 5-arg form the candidacy-stage models use),
-- which keeps candidacy_source_ids joinable to the candidacy-stage layer. The
-- code varies by office and city, so a person running for multiple offices
-- resolves to several codes here. first_seen_at is the earliest date_processed
-- (parsed inline; candidate staging keeps it raw).
with
    ts as (
        select
            {{
                generate_candidate_code(
                    "first_name", "last_name", "state", "office_type", "city"
                )
            }} as techspeed_candidate_code,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            upper(trim(state_postal_code)) as state,
            nullif(lower(trim(email)), '') as email,
            nullif(trim(phone), '') as phone,
            birth_date_parsed as birth_date,
            nullif(trim(website_url), '') as website_url,
            nullif(trim(facebook_url), '') as facebook_url,
            nullif(trim(linkedin_url), '') as linkedin_url,
            nullif(lower(trim(official_office_name)), '') as office_name,
            cast(br_race_id as string) as race_id,
            election_date,
            case
                when primary_election_date_parsed is not null
                then 'primary'
                else 'general'
            end as election_stage,
            coalesce(
                try_cast(date_processed as date),
                try_to_date(date_processed, 'MM/dd/yyyy'),
                try_to_date(date_processed, 'M/d/yyyy')
            ) as processed_date,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    coded as (select * from ts where techspeed_candidate_code is not null),

    representative as (
        select techspeed_candidate_code, first_name, last_name, state
        from coded
        qualify
            row_number() over (
                partition by techspeed_candidate_code
                order by processed_date desc nulls last, _airbyte_extracted_at desc
            )
            = 1
    ),

    aggregated as (
        select
            techspeed_candidate_code,
            cast(
                min(
                    coalesce(cast(processed_date as timestamp), _airbyte_extracted_at)
                ) as timestamp
            ) as first_seen_at,
            max(email) as email,
            max(phone) as phone,
            max(birth_date) as birth_date,
            max(website_url) as website_url,
            max(facebook_url) as facebook_url,
            max(linkedin_url) as linkedin_url,
            array_sort(
                collect_set(techspeed_candidate_code || '__' || election_stage)
            ) as candidacy_source_ids,
            array_sort(collect_set(race_id)) as race_ids,
            array_sort(collect_set(office_name)) as office_names,
            array_sort(collect_set(election_date)) as election_dates
        from coded
        group by techspeed_candidate_code
    )

select
    'techspeed_candidate' as source_name,
    r.techspeed_candidate_code as source_id,
    cast(null as string) as br_candidate_id,
    cast(null as string) as ts_officeholder_id,
    r.techspeed_candidate_code,
    cast(null as string) as ddhq_candidate_id,
    r.first_name,
    r.last_name,
    cast(null as string) as middle_name,
    nullif(trim(concat_ws(' ', r.first_name, r.last_name)), '') as full_name,
    a.email,
    a.phone,
    r.state,
    a.birth_date,
    a.website_url,
    a.facebook_url,
    -- TechSpeed stores twitter/instagram as handles, not URLs; left null here.
    cast(null as string) as twitter_url,
    a.linkedin_url,
    a.candidacy_source_ids,
    a.race_ids,
    a.office_names,
    a.election_dates,
    a.first_seen_at
from representative as r
join aggregated as a using (techspeed_candidate_code)
