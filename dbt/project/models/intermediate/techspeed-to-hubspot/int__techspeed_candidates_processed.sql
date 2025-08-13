{{
    config(
        materialized="incremental",
        unique_key="ts_candidate_code",
        on_schema_change="sync_all_columns",
        tags=["techspeed", "storage", "incremental"],
        post_hook="insert into {{ ref('int__techspeed_uploaded_files') }} (source_file_url, uploaded_at, processing_date) select distinct _ab_source_file_url, current_timestamp(), current_date() from {{ this }} where final_exclusion_reason is null",
    )
}}

with
    deduplicated_candidates as (
        select * from {{ ref("int__techspeed_deduplicated_candidates") }}
    ),

    -- Clean district names
    district_cleaning as (
        select
            * except (district),
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                district, 'District |Dist\\. #?|Subdistrict |Ward ', ''
                            ),
                            '(st|nd|rd|th) Congressional District',
                            ''
                        ),
                        '[-#]',
                        ''
                    ),
                    ' District',
                    ''
                )
            ) as district
        from deduplicated_candidates
    ),

    -- Clean city names (remove everything after comma)
    city_cleaning as (
        select
            * except (city),
            case
                when city like '%,%'
                then left(city, position(',' in city) - 1)
                else city
            end as city
        from district_cleaning
    ),

    {% if is_incremental() %}
        -- Get existing records for timestamp preservation
        existing_records as (
            select ts_candidate_code, created_at, uploaded_at from {{ this }}
        ),
    {% endif %}

    -- Add UUID and timestamps
    with_metadata as (
        select
            -- Generate UUID based on cleaned candidate components including election
            -- year
            {{
                generate_salted_uuid(
                    [
                        'lower(trim(regexp_replace(first_name, "[^a-zA-Z0-9]", "")))',
                        'lower(trim(regexp_replace(last_name, "[^a-zA-Z0-9]", "")))',
                        "lower(trim(state))",
                        'lower(trim(regexp_replace(standardized_office_type, "[^a-zA-Z0-9]", "")))',
                        "cast(year(try_cast(election_date as date)) as string)",
                    ]
                )
            }}
            as candidate_uuid,

            -- All original columns in snake_case
            candidate_id_source,
            first_name,
            last_name,
            candidate_type,
            party,
            email,
            phone,
            candidate_id_tier,
            website_url,
            linkedin_url,
            instagram_handle,
            twitter_handle,
            facebook_url,
            birth_date,
            street_address,
            state,

            -- Zero-pad postal codes to 5 digits
            case
                when postal_code is null
                then null
                else right(concat('00000', cast(postal_code as string)), 5)
            end as postal_code,

            district,
            city,
            population,
            official_office_name,
            initcap(trim(candidate_office)) as candidate_office,
            standardized_office_type,
            office_level,
            filing_deadline,
            ballotready_race_id,
            -- Parse date fields that might be in M/D/YYYY format
            try_cast(primary_election_date as date) as primary_election_date,
            try_cast(
                corrected_general_election_date as date
            ) as corrected_general_election_date,
            try_cast(election_date as date) as election_date,
            election_type,
            uncontested,
            number_of_candidates,
            number_of_seats_available,
            open_seat,
            partisan,
            type,
            contact_owner,
            owner_name,
            final_exclusion_reason,
            city_cleaning.ts_candidate_code,
            _ab_source_file_url,

            -- Timestamps - handle incremental logic
            {% if is_incremental() %}
                coalesce(existing.created_at, current_timestamp()) as created_at,
                current_timestamp() as updated_at,
                coalesce(existing.uploaded_at, cast(null as timestamp)) as uploaded_at
            {% else %}
                current_timestamp() as created_at,
                current_timestamp() as updated_at,
                cast(null as timestamp) as uploaded_at
            {% endif %}

        from city_cleaning
        {% if is_incremental() %}
            left join
                existing_records existing
                on city_cleaning.ts_candidate_code = existing.ts_candidate_code
        {% endif %}
    ),

    -- Deduplicate by candidate code (keep most recent by source file)
    deduplicated_final as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by ts_candidate_code order by _ab_source_file_url desc
                    ) as rn
                from with_metadata
            ) ranked
        where rn = 1
    )

select * except (rn)
from deduplicated_final
