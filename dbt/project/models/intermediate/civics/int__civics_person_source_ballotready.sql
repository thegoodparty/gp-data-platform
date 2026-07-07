-- BallotReady person projection: one row per br_candidate_id, the native BR
-- person id. BallotReady has no first-class person record in the S3 feeds, so
-- identity is reconstructed from a person's candidacy (candidacies_v3) and
-- office-holder (office_holders_v3) rows. first_seen_at is the earliest created
-- timestamp across both.
--
-- No election-day filter: a person's older candidacies and terms are useful
-- signal when resolving them across sources.
with
    candidacy_members as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            nullif(lower(trim(middle_name)), '') as middle_name,
            upper(trim(state)) as state,
            nullif(lower(trim(email)), '') as email,
            nullif(trim(phone), '') as phone,
            cast(null as string) as website_url,
            cast(null as string) as facebook_url,
            cast(null as string) as twitter_url,
            cast(null as string) as linkedin_url,
            cast(br_candidacy_id as string) as candidacy_source_id,
            cast(br_race_id as string) as race_id,
            nullif(trim(position_name), '') as office_name,
            election_day as election_date,
            coalesce(candidacy_created_at, _airbyte_extracted_at) as created_at,
            coalesce(
                candidacy_updated_at, candidacy_created_at, _airbyte_extracted_at
            ) as sort_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
    ),

    officeholder_members as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            nullif(lower(trim(middle_name)), '') as middle_name,
            upper(trim(state)) as state,
            nullif(lower(trim(email)), '') as email,
            nullif(trim(phone), '') as phone,
            nullif(trim(website_url), '') as website_url,
            nullif(trim(facebook_url), '') as facebook_url,
            nullif(trim(twitter_url), '') as twitter_url,
            nullif(trim(linkedin_url), '') as linkedin_url,
            cast(br_candidacy_id as string) as candidacy_source_id,
            cast(null as string) as race_id,
            nullif(trim(coalesce(position_name, office_title)), '') as office_name,
            cast(null as date) as election_date,
            coalesce(office_holder_created_at, _airbyte_extracted_at) as created_at,
            coalesce(
                office_holder_updated_at,
                office_holder_created_at,
                _airbyte_extracted_at
            ) as sort_at
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
    ),

    members as (
        select *
        from candidacy_members
        union all
        select *
        from officeholder_members
    ),

    -- Coherent name/state from the most recently updated member row.
    representative as (
        select br_candidate_id, first_name, last_name, middle_name, state
        from members
        qualify
            row_number() over (partition by br_candidate_id order by sort_at desc) = 1
    ),

    aggregated as (
        select
            br_candidate_id,
            cast(min(created_at) as timestamp) as first_seen_at,
            -- Best non-null contact / social across all of a person's rows;
            -- socials only appear on office-holder rows.
            max(email) as email,
            max(phone) as phone,
            max(website_url) as website_url,
            max(facebook_url) as facebook_url,
            max(twitter_url) as twitter_url,
            max(linkedin_url) as linkedin_url,
            array_sort(collect_set(candidacy_source_id)) as candidacy_source_ids,
            array_sort(collect_set(race_id)) as race_ids,
            array_sort(collect_set(office_name)) as office_names,
            array_sort(collect_set(election_date)) as election_dates
        from members
        group by br_candidate_id
    )

select
    'ballotready' as source_name,
    r.br_candidate_id as source_id,
    r.br_candidate_id,
    cast(null as string) as ts_officeholder_id,
    cast(null as string) as techspeed_candidate_code,
    cast(null as string) as ddhq_candidate_id,
    r.first_name,
    r.last_name,
    r.middle_name,
    nullif(
        trim(concat_ws(' ', r.first_name, r.middle_name, r.last_name)), ''
    ) as full_name,
    a.email,
    a.phone,
    r.state,
    cast(null as date) as birth_date,
    a.website_url,
    a.facebook_url,
    a.twitter_url,
    a.linkedin_url,
    a.candidacy_source_ids,
    a.race_ids,
    a.office_names,
    a.election_dates,
    a.first_seen_at
from representative as r
join aggregated as a using (br_candidate_id)
