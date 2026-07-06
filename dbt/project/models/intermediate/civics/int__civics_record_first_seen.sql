-- One row per source-native person identifier with the earliest timestamp at
-- which that person is evidenced in the source. The person layer mints a stable
-- gp_person_id from the earliest member of each cluster (canonical-person plan,
-- decision 4), so this model decides which member wins. Fully re-derivable and
-- safe under full refresh: for a fixed set of source rows the min is stable.
--
-- first_seen_at prefers each source's native created timestamp, falling back to
-- Airbyte extraction time where none exists (DDHQ). The plan's middle
-- "source-file date" tier is omitted: pre-work found native created fields are
-- ~100% populated for every source except DDHQ, so it would cover nothing.
--
-- BallotReady has no first-class person record in the S3 feeds; a BR person is
-- reconstructed from their candidacy and office-holder rows, so first_seen is
-- the min created timestamp across both. TechSpeed candidates and office
-- holders are distinct native id spaces, so they get distinct source_names.
--
-- ingested_at (min Airbyte extraction time) exists only for the sanity test
-- first_seen_at <= ingested_at. HubSpot staging drops _airbyte_extracted_at, so
-- it is null there.
with
    ballotready_rows as (
        select
            cast(br_candidate_id as string) as source_id,
            candidacy_created_at as created_at,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null

        union all

        select
            cast(br_candidate_id as string) as source_id,
            office_holder_created_at as created_at,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
    ),

    ballotready as (
        select
            'ballotready' as source_name,
            source_id,
            cast(
                min(coalesce(created_at, _airbyte_extracted_at)) as timestamp
            ) as first_seen_at,
            cast(min(_airbyte_extracted_at) as timestamp) as ingested_at
        from ballotready_rows
        group by source_id
    ),

    gp_api as (
        select
            'gp_api' as source_name,
            cast(id as string) as source_id,
            cast(
                min(coalesce(created_at, _airbyte_extracted_at)) as timestamp
            ) as first_seen_at,
            cast(min(_airbyte_extracted_at) as timestamp) as ingested_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        where id is not null
        group by id
    ),

    hubspot as (
        select
            'hubspot' as source_name,
            cast(id as string) as source_id,
            cast(min(contact_created_at) as timestamp) as first_seen_at,
            cast(null as timestamp) as ingested_at
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        where id is not null and contact_created_at is not null
        group by id
    ),

    techspeed_candidate_rows as (
        select
            {{
                generate_candidate_code(
                    "first_name", "last_name", "state", "office_type", "city"
                )
            }} as source_id,
            coalesce(
                try_cast(date_processed as date),
                try_to_date(date_processed, 'MM/dd/yyyy'),
                try_to_date(date_processed, 'M/d/yyyy')
            ) as created_date,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    techspeed_candidate as (
        select
            'techspeed_candidate' as source_name,
            source_id,
            cast(
                min(
                    coalesce(cast(created_date as timestamp), _airbyte_extracted_at)
                ) as timestamp
            ) as first_seen_at,
            cast(min(_airbyte_extracted_at) as timestamp) as ingested_at
        from techspeed_candidate_rows
        where source_id is not null
        group by source_id
    ),

    techspeed_officeholder as (
        select
            'techspeed_officeholder' as source_name,
            cast(ts_officeholder_id as string) as source_id,
            cast(
                min(
                    coalesce(
                        cast(date_processed_date as timestamp), _airbyte_extracted_at
                    )
                ) as timestamp
            ) as first_seen_at,
            cast(min(_airbyte_extracted_at) as timestamp) as ingested_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        where ts_officeholder_id is not null
        group by ts_officeholder_id
    ),

    ddhq as (
        select
            'ddhq' as source_name,
            cast(candidate_id as string) as source_id,
            cast(min(_airbyte_extracted_at) as timestamp) as first_seen_at,
            cast(min(_airbyte_extracted_at) as timestamp) as ingested_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where candidate_id is not null
        group by candidate_id
    )

select source_name, source_id, first_seen_at, ingested_at
from ballotready
union all
select source_name, source_id, first_seen_at, ingested_at
from gp_api
union all
select source_name, source_id, first_seen_at, ingested_at
from hubspot
union all
select source_name, source_id, first_seen_at, ingested_at
from techspeed_candidate
union all
select source_name, source_id, first_seen_at, ingested_at
from techspeed_officeholder
union all
select source_name, source_id, first_seen_at, ingested_at
from ddhq
