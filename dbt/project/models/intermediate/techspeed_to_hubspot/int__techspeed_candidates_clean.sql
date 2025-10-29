{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="techspeed_candidate_code",
        on_schema_change="append_new_columns",
        tags=["intermediate", "techspeed", "hubspot"],
    )
}}

with
    candidates_wo_suffix as (
        select
            case
                when candidate_id_source = 'ts_found_race_net_new'
                then 'TS NET NEW'
                else candidate_id_source
            end as candidate_id_source,
            first_name,
            last_name,
            candidate_type,
            email,
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
                when city like '%,%'
                then left(city, position(',' in city) - 1)
                else city
            end as city,
            state,
            official_office_name,
            candidate_office,
            case
                when lower(office_type) = 'city mayor'
                then 'Mayor'
                when lower(office_type) = 'city council member'
                then 'City Council'
                when lower(office_type) = 'township trustee'
                then 'Town Council'
                when lower(office_type) = 'village council'
                then 'Town Council'
                when lower(office_type) = 'city commissioner'
                then 'City Council'
                when lower(office_type) = 'services director'
                then 'Other'
                when lower(office_type) = 'town commission'
                then 'Town Council'
                else office_type
            end as office_type,
            office_level,
            filing_deadline,
            primary_election_date,
            general_election_date,
            coalesce(general_election_date, primary_election_date) as election_date,
            election_type,
            uncontested,
            number_of_candidates,
            number_of_seats_available,
            open_seat,
            partisan,
            case
                when (population = '' or population is null)
                then null
                else cast(trim(regexp_replace(population, '[^0-9]', '')) as integer)
            end as population,
            ballotready_race_id,
            type,
            contact_owner,
            owner_name,
            case
                when try_cast(general_election_date as date) <= current_date
                then 'past_election'
                else 'future_election'
            end as uploaded,
            _ab_source_file_url,
            _airbyte_extracted_at,
            {{ remove_techspeed_name_suffixes("last_name") }} as last_name_no_suffix
        from {{ ref("int__techspeed_candidates") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    ),
    candidates_w_clean_comma as (
        select
            * except (last_name_no_suffix),
            -- Remove trailing commas
            regexp_replace(last_name_no_suffix, ',$', '') as last_name_no_comma
        from candidates_wo_suffix
    ),
    candidates_w_clean_space as (
        select
            * except (last_name_no_comma),
            case
                when
                    length(last_name_no_comma) >= 2
                    and substring(last_name_no_comma, length(last_name_no_comma) - 1, 1)
                    = ' '
                then substring(last_name_no_comma, 1, length(last_name_no_comma) - 2)
                else last_name_no_comma
            end as last_name_trimmed
        from candidates_w_clean_comma
    ),
    candidates_w_clean_initials as (
        select
            * except (last_name_trimmed),
            -- Handle middle initial with period but no space
            case
                when
                    length(last_name_trimmed) >= 3
                    and substring(last_name_trimmed, 2, 1) = '.'
                    and substring(last_name_trimmed, 3, 1) != ' '
                then substring(last_name_trimmed, 3)
                else last_name_trimmed
            end as last_name_clean
        from candidates_w_clean_space
    ),
    candidates_w_extracted_last_name as (
        select
            * except (last_name_clean),
            -- Extract suggested last name (everything after last space)
            case
                when last_name_clean like '% %'
                then
                    case
                        -- Handle special prefixes that should be kept
                        when last_name_clean like '%De La %'
                        then
                            'De La ' || substring(
                                last_name_clean,
                                position('De La ' in last_name_clean) + 7
                            )
                        when last_name_clean like '%Van %'
                        then
                            'Van ' || substring(
                                last_name_clean, position('Van ' in last_name_clean) + 4
                            )
                        when last_name_clean like '% van %'
                        then
                            ' van ' || substring(
                                last_name_clean,
                                position(' van ' in last_name_clean) + 5
                            )
                        when last_name_clean like '%Le %'
                        then
                            'Le ' || substring(
                                last_name_clean, position('Le ' in last_name_clean) + 3
                            )
                        else
                            -- Default: take everything after the last space
                            substring(
                                last_name_clean,
                                length(last_name_clean)
                                - position(' ' in reverse(last_name_clean))
                                + 2
                            )
                    end
                else last_name_clean
            end as suggested_last
        from candidates_w_clean_initials
    ),
    candidates_deduped_on_name_state_office_type as (
        select
            *,
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
                    {{
                        generate_candidate_code(
                            "first_name",
                            "suggested_last",
                            "state",
                            "office_type",
                            "city",
                        )
                    }}
            end as techspeed_candidate_code
        from candidates_w_extracted_last_name
        where
            -- Filter out records with null or empty required fields for candidate code
            trim(first_name) is not null
            and trim(first_name) <> ''
            and trim(last_name) is not null
            and trim(last_name) <> ''
            and trim(state) is not null
            and trim(state) <> ''
            and trim(city) is not null
            and trim(city) <> ''
            and trim(office_type) is not null
            and trim(office_type) <> ''
        qualify
            -- qualify over the techspeed_candidate_code, and order by the source file
            -- url in ascending order. This is to ensure that we only keep the record
            -- the first time it appears so we don't overwrite the record with
            -- a later one
            row_number() over (
                partition by techspeed_candidate_code order by _ab_source_file_url asc
            )
            = 1
    ),
    -- deduplicate on phone number, and order by the source file url
    -- in ascending order. This is to ensure that we only keep the record the
    -- first time it appears so we don't overwrite the record with a later one
    candidates_deduped_on_phone as (
        select *
        from candidates_deduped_on_name_state_office_type
        qualify
            row_number() over (partition by phone order by _ab_source_file_url asc) = 1
    )

select
    candidate_id_source,
    first_name,
    suggested_last as last_name,
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
    _ab_source_file_url,
    _airbyte_extracted_at
from candidates_deduped_on_phone
