{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'hubspot_record_id',
        merge_update_condition = "DBT_INTERNAL_SOURCE.updated_at > DBT_INTERNAL_DEST.updated_at",
        auto_liquid_cluster=true, 
        tags=["intermediate", "hubspot","mart"]
    ) 
}}

with hs_companies as (
    select 
        'gp_candidacy_id-tbd' as gp_candidacy_id,
        'candidacy_id-tbd' as candidacy_id,
        'gp_user_id-tbd' as gp_user_id,
        'gp_contest_id-tbd' as gp_constest_id,
        t1.id as hubspot_record_id,

        t1.properties_candidate_name as full_name,
        t1.properties_candidate_email as email,
        t1.properties_phone as phone_number,
        t1.properties_website as website_url,
        t1.properties_linkedin_company_page as linkedin_url,
        t1.properties_twitterhandle as twitter_handle,
        t1.properties_facebook_url as facebook_url,
        t1.properties_address as street_address,

        t1.properties_official_office_name as official_office_name,
        t1.properties_candidate_office as candidate_office, 
        t1.properties_office_level as office_level,
        t1.properties_office_type as office_type,
        t1.properties_candidate_party as party_affiliation,
        t1.properties_partisan_np as is_partisan,

        t1.properties_state as state,
        t1.properties_city as city,
        t1.properties_candidate_district as district,
        t1.properties_candidates_seats as seat,

        t1.properties_filing_deadline as filing_deadline,
        t1.properties_primary_date as primary_election_date,
        t1.properties_election_date as general_election_date,
        t1.properties_runoff_date as runoff_election_date,

        t1.properties_incumbent as is_incumbent,
        t1.properties_uncontested as is_uncontested, 
        t1.properties_number_of_opponents as number_of_opponents,
        t1.properties_open_seat_ as is_open_seat,
        t1.properties_general_election_result as candidacy_result,
        -1 as population, --from contacts table

        t1.createdAt as created_at,
        t1.updatedAt as updated_at
  
-- from {{ ref("stg_airbyte_source__hubspot_api_companies") }} t1
from goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_companies t1
), 
hs_contacts as (
    -- this is a placeholder for when the contacts data can be joined 
   select
        t1.*, 
        'hubspot' as source_name, --from contacts table
        '' as first_name, --from contacts table
        '' as last_name, --from contacts table
        to_date('1900-01-01', 'yyyy-MM-dd') as birth_date, --from contacts table
        '' as instagram_handle --from contacts table

   from hs_companies t1 
)
select * from hs_companies