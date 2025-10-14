{{
    config(
        materialized="incremental",
        unique_key="techspeed_candidate_code",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        tags=["intermediate", "techspeed", "hubspot"],
    )
}}

with
    techspeed_candidates_w_hubspot as (
        select
            candidate_id_source,
            first_name,
            last_name,
            candidate_type,
            email,
            techspeed_candidate_code,
            phone,
            candidate_id_tier,
            party,
            website_url,
            linkedin_url,
            instagram_handle,
            twitter_handle,
            facebook_url,
            birth_date,
            street_address,
            postal_code,
            district,
            city,
            state,
            official_office_name,
            candidate_office,
            office_type,
            office_level,
            filing_deadline,
            primary_election_date,
            general_election_date,
            election_date,
            election_type,
            uncontested,
            number_of_candidates,
            number_of_seats_available,
            open_seat,
            partisan,
            population,
            ballotready_race_id,
            type,
            contact_owner,
            owner_name,
            case
                when
                    techspeed_candidate_code in (
                        select hubspot_candidate_code
                        from {{ ref("int__hubspot_candidacy_codes") }}
                    )
                then 'in_hubspot'
                else uploaded
            end as uploaded,
            _airbyte_extracted_at,
            _ab_source_file_url
        from {{ ref("int__techspeed_candidates_clean") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    )

select
    candidate_id_source,
    first_name,
    last_name,
    candidate_type,
    email,
    techspeed_candidate_code,
    phone,
    candidate_id_tier,
    party,
    website_url,
    linkedin_url,
    instagram_handle,
    twitter_handle,
    facebook_url,
    birth_date,
    street_address,
    postal_code,
    district,
    city,
    state,
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    filing_deadline,
    primary_election_date,
    general_election_date,
    election_date,
    election_type,
    uncontested,
    number_of_candidates,
    number_of_seats_available,
    open_seat,
    partisan,
    population,
    ballotready_race_id,
    type,
    contact_owner,
    owner_name,
    uploaded,
    _airbyte_extracted_at,
    _ab_source_file_url
from techspeed_candidates_w_hubspot
