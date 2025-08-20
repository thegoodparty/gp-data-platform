{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

-- TODO:
-- move all transformations to the intermerdiate model name cleaned
-- include office type transformation in the above data model
with
    techspeed_candidates_w_hubspot as (
        select
            candidate_id_source,
            first_name,
            suggested_last as last_name,
            candidate_type,
            email,
            case
                when first_name is null
                then null
                when last_name is null
                then null
                when state is null
                then null
                when office_type is null
                then null
                else
                    lower(
                        concat_ws(
                            '__',
                            regexp_replace(
                                regexp_replace(trim(first_name), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            ),
                            regexp_replace(
                                regexp_replace(trim(last_name), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            ),
                            regexp_replace(
                                regexp_replace(trim(state), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            ),
                            regexp_replace(
                                regexp_replace(trim(office_type), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            )
                        )
                    )
            end as techspeed_candidate_code,
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
            -- one time fix
            case
                when general_election_date = '2025-10-07'
                then '2025-11-04'
                else general_election_date
            end as general_election_date,
            coalesce(general_election_date, primary_election_date) as election_date,
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
                    ts_candidate_code in (
                        select hubspot_candidate_code
                        from {{ ref("int__hubspot_candidate_codes") }}
                    )
                then 'in_hubspot'
                else uploaded
            end as uploaded,
            _airbyte_extracted_at
        from {{ ref("int__techspeed_candidates_clean_last_name") }}
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
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        district, 'District |Dist\. #?|Subdistrict |Ward ', ''
                    ),
                    '(st|nd|rd|th) Congressional District',
                    ''
                ),
                '[-#]',
                ''
            ),
            ' District$',
            ''
        )
    ) as district,
    case
        when city like '%,%' then left(city, position(',' in city) - 1) else city
    end as city,
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
        when try_cast(election_date as date) <= current_date
        then 'past_election'
        else uploaded
    end as uploaded,
    _airbyte_extracted_at
from techspeed_candidates_w_hubspot
