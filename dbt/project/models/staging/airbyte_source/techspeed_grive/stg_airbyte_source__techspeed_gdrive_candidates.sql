with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    renamed as (
        select
            -- Airbyte metadata
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,

            -- Candidate identity
            trim(first_name) as first_name,
            -- Strip middle-initial pollution from last_name.
            -- Three regex_replace passes after remove_name_suffixes:
            -- (1) strip a trailing comma left behind by suffix removal
            -- ("Smith, Jr." -> "Smith," -> "Smith"); (2) strip one-or-more
            -- leading initial tokens ("A.", "A. ", "A ", "M.J. ", "E L ")
            -- that look like middle initials but were merged with the
            -- surname in TS source data; (3) strip a trailing " X" pattern.
            -- Lookarounds prevent over-stripping legitimate compound
            -- surnames ("De La Cruz", "Da Silva", "St. John", "AB Smith"),
            -- which have no period or space between the leading cap(s)
            -- and the next character.
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        {{ remove_name_suffixes("trim(last_name)") }}, ',$', ''
                    ),
                    '^([A-Z][.] ?|[A-Z] )+(?=[A-Za-z])',
                    ''
                ),
                '(?<=[A-Za-z]) [A-Z]$',
                ''
            ) as last_name,
            trim(regexp_replace(src.state, '[^A-Za-z ]', '')) as state,
            coalesce(
                cs.state_cleaned_postal_code,
                trim(regexp_replace(src.state, '[^A-Za-z ]', ''))
            ) as state_postal_code,
            nullif(trim(email), '') as email,
            {{ clean_phone_number("phone") }} as phone,
            date_of_birth_mmddyyyy as birth_date,
            -- Same slash-normalize + non-zero-padded handling as the election
            -- dates above: TechSpeed is the same source, so birth dates arrive both
            -- as 6-2-1985 and as yyyy/MM/dd (1953/02/01). Normalize slashes to
            -- dashes, then parse ISO (try_cast) and month-first M-d-yyyy / M-d-yy.
            coalesce(
                try_cast(replace(date_of_birth_mmddyyyy, '/', '-') as date),
                try_to_date(replace(date_of_birth_mmddyyyy, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace(date_of_birth_mmddyyyy, '/', '-'), 'M-d-yy')
            ) as birth_date_parsed,
            nullif(trim(street_address), '') as street_address,
            postal_code,

            -- Office / position
            trim(office_name) as official_office_name,
            trim(office_normalized) as candidate_office,
            office_type,
            office_level,
            trim(district_name) as district,
            trim(normalized_location) as city,
            county_municipality,

            -- Election dates (raw strings with slash→dash normalization)
            replace(primary_election_date, '/', '-') as primary_election_date,
            replace(general_election_date, '/', '-') as general_election_date,
            replace(filing_deadline, '/', '-') as filing_deadline,
            -- Parsed DATE columns. TechSpeed delivers some dates without
            -- zero-padding (for example 6-2-2026), so the patterns use the
            -- single-letter tokens M and d, which accept one or two digits and
            -- therefore parse both padded (06-02-2026) and non-padded values.
            -- The earlier MM-dd patterns required two digits and silently
            -- dropped the non-padded dates to NULL.
            coalesce(
                try_cast(replace(primary_election_date, '/', '-') as date),
                try_to_date(replace(primary_election_date, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace(primary_election_date, '/', '-'), 'M-d-yy')
            ) as primary_election_date_parsed,
            coalesce(
                try_cast(replace(general_election_date, '/', '-') as date),
                try_to_date(replace(general_election_date, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace(general_election_date, '/', '-'), 'M-d-yy')
            ) as general_election_date_parsed,
            case
                when
                    year(
                        coalesce(
                            try_cast(replace(filing_deadline, '/', '-') as date),
                            try_to_date(replace(filing_deadline, '/', '-'), 'M-d-yyyy'),
                            try_to_date(replace(filing_deadline, '/', '-'), 'M-d-yy')
                        )
                    )
                    between 1900 and 2050
                then
                    coalesce(
                        try_cast(replace(filing_deadline, '/', '-') as date),
                        try_to_date(replace(filing_deadline, '/', '-'), 'M-d-yyyy'),
                        try_to_date(replace(filing_deadline, '/', '-'), 'M-d-yy')
                    )
            end as filing_deadline_parsed,
            -- Coalesced election date (general preferred, fallback to primary)
            coalesce(
                try_cast(replace(general_election_date, '/', '-') as date),
                try_to_date(replace(general_election_date, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace(general_election_date, '/', '-'), 'M-d-yy'),
                try_cast(replace(primary_election_date, '/', '-') as date),
                try_to_date(replace(primary_election_date, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace(primary_election_date, '/', '-'), 'M-d-yy')
            ) as election_date,
            election_result,

            -- Race metadata (booleans via cast_to_boolean)
            party,
            {{ cast_to_boolean("partisan", ["partisan"], ["nonpartisan"]) }}
            as is_partisan,
            {{ cast_to_boolean("is_incumbent") }} as is_incumbent,
            {{ cast_to_boolean("is_primary") }} as is_primary,
            {{ cast_to_boolean("is_uncontested") }} as is_uncontested,
            {{ cast_to_boolean("open_seat") }} as is_open_seat,
            is_veteran,
            try_cast(
                trim(regexp_replace(cast(population as string), '[^0-9]', '')) as int
            ) as population,
            number_candidates as number_of_candidates,
            try_cast(seats_available as int) as seats_available,
            ballotready_race_id as br_race_id,

            -- Source tracking
            candidate_id_source,
            candidate_id_tier,
            ts_found_race_net_new,
            ts_found_candidate_net_new,
            ts_status,
            ts_comment,

            -- Contact sourcing
            phone_clean,
            phone_source,
            phone_type_select_1,
            email_source,
            source_url,
            date_processed,
            -- Deliveries mix ISO and US date formats (same parse rule as the
            -- officeholders staging).
            coalesce(
                try_cast(date_processed as date),
                try_to_date(date_processed, 'MM/dd/yyyy'),
                try_to_date(date_processed, 'M/d/yyyy')
            ) as date_processed_date,

            -- Social / web
            nullif(trim(website_url), '') as website_url,
            nullif(trim(facebook_url), '') as facebook_url,
            nullif(trim(linkedin_url), '') as linkedin_url,
            nullif(trim(twitter_handle), '') as twitter_handle,
            nullif(trim(instagram_handle), '') as instagram_handle,

            -- Airbyte source file metadata
            _ab_source_file_url,
            _ab_source_file_last_modified

        from source as src
        left join
            clean_states as cs
            on upper(trim(regexp_replace(src.state, '[^A-Za-z ]', '')))
            = upper(trim(cs.state_raw))
    ),

    invalid as (
        select _airbyte_raw_id
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates_invalid") }}
    )

select *
from renamed
where
    _airbyte_raw_id
    not in (select _airbyte_raw_id from invalid where _airbyte_raw_id is not null)
