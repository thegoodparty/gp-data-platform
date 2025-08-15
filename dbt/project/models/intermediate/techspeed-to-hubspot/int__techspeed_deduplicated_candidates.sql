{{ config(materialized="view", tags=["techspeed", "deduplication"]) }}

with
    cleaned_candidates as (
        select * from {{ ref("int__techspeed_cleaned_candidates") }}
    ),

    -- Get current HubSpot contacts for deduplication
    hubspot_contacts as (
        select
            properties_firstname,
            properties_lastname,
            properties_state,
            properties_office_type,
            properties_type,
            properties_product_user,
            properties_election_date
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        where
            (
                properties_type like '%Self-Filer Lead%'
                or properties_product_user = 'yes'
            )
            and properties_election_date
            between date_trunc('year', current_date()) and date_trunc(
                'year', add_months(current_date(), 12)
            )
            - interval 1 day
    ),

    -- Generate candidate codes for existing HubSpot contacts
    hubspot_candidate_codes as (
        select
            {{
                generate_candidate_code(
                    "properties_firstname",
                    "properties_lastname",
                    "properties_state",
                    "properties_office_type",
                )
            }} as hs_candidate_code
        from hubspot_contacts
        where
            properties_firstname is not null
            and properties_lastname is not null
            and properties_state is not null
            and properties_office_type is not null
    ),

    -- Add candidate codes to TechSpeed data and standardize office types
    techspeed_with_codes as (
        select
            *,
            -- Use existing macro to standardize office types
            {{ map_ballotready_office_type("candidate_office") }}
            as standardized_office_type,

            -- Generate candidate code for deduplication
            {{
                generate_candidate_code(
                    "first_name",
                    "last_name",
                    "state",
                    map_ballotready_office_type("candidate_office"),
                )
            }} as ts_candidate_code

        from cleaned_candidates
    ),

    -- Identify candidates already in HubSpot
    deduplicated_candidates as (
        select
            *,
            case
                when
                    ts_candidate_code
                    in (select hs_candidate_code from hubspot_candidate_codes)
                then 'in_hubspot'
                else upload_exclusion_reason
            end as updated_exclusion_reason
        from techspeed_with_codes
    ),

    -- Add consolidated election date and fix known date issues
    candidates_with_dates as (
        select
            *,
            -- Use General election date if available, else Primary
            coalesce(general_election_date, primary_election_date) as election_date,

            -- One-time fix for incorrect date
            case
                when general_election_date = '2025-10-07'
                then '2025-11-04'
                else general_election_date
            end as corrected_general_election_date

        from deduplicated_candidates
    ),

    -- Filter to future elections only
    future_elections_only as (
        select
            * except (updated_exclusion_reason),
            case
                when try_cast(election_date as date) <= current_date()
                then 'past_election'
                else updated_exclusion_reason
            end as final_exclusion_reason
        from candidates_with_dates
    )

select *
from future_elections_only
