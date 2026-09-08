with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    -- Delivery files disagree on date component order. Most write month-first
    -- (11-3-2026), but some write day-first (25-08-2026), and a day-first value
    -- whose day is <= 12 parses month-first without error, landing silently on
    -- the wrong month. The order therefore has to be decided per file, reading
    -- all three date columns, since a file holds to one convention throughout
    -- and several tip their hand only on the filing deadline.
    file_dates as (
        select _ab_source_file_url, replace(primary_election_date, '/', '-') as d
        from source
        union all
        select _ab_source_file_url, replace(general_election_date, '/', '-')
        from source
        union all
        select _ab_source_file_url, replace(filing_deadline, '/', '-')
        from source
    ),

    file_date_parts as (
        select
            _ab_source_file_url,
            try_cast(
                regexp_extract(d, '^([0-9]{1,2})-([0-9]{1,2})-[0-9]{2,4}$', 1) as int
            ) as first_part,
            try_cast(
                regexp_extract(d, '^([0-9]{1,2})-([0-9]{1,2})-[0-9]{2,4}$', 2) as int
            ) as second_part
        from file_dates
    ),

    -- A file reads as day-first only on uncontradicted evidence: some date has a
    -- first component above 12 (impossible as a month) and none has a second
    -- component above 12 (which would prove month-first). Files with neither
    -- signal keep the month-first default.
    file_date_order as (
        select
            _ab_source_file_url,
            coalesce(max(first_part) > 12, false)
            and not coalesce(max(second_part) > 12, false) as is_day_first
        from file_date_parts
        group by _ab_source_file_url
    ),

    source_with_date_order as (
        select src.*, coalesce(fdo.is_day_first, false) as is_day_first_file
        from source as src
        left join
            file_date_order as fdo on fdo._ab_source_file_url = src._ab_source_file_url
    ),

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
            -- Generational suffix, captured from the raw surname before the
            -- stripping above discards it (Jr/Sr distinguish father and son).
            upper(
                nullif(
                    regexp_extract(
                        trim(src.last_name),
                        '(?i)(?:^|[ ,])(jr|sr|ii|iii|iv|v)\\.?\\s*$',
                        1
                    ),
                    ''
                )
            ) as name_suffix,
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
            -- Which component order this delivery file uses, exposed so a
            -- surprising parse can be traced back to the file-level decision.
            src.is_day_first_file,
            -- Parsed DATE columns.
            {{ parse_techspeed_date("primary_election_date", "src.is_day_first_file") }}
            as primary_election_date_parsed,
            {{ parse_techspeed_date("general_election_date", "src.is_day_first_file") }}
            as general_election_date_parsed,
            case
                when
                    year(
                        {{
                            parse_techspeed_date(
                                "filing_deadline", "src.is_day_first_file"
                            )
                        }}
                    )
                    between 1900 and 2050
                then
                    {{
                        parse_techspeed_date(
                            "filing_deadline", "src.is_day_first_file"
                        )
                    }}
            end as filing_deadline_parsed,
            -- Coalesced election date (general preferred, fallback to primary)
            coalesce(
                {{
                    parse_techspeed_date(
                        "general_election_date", "src.is_day_first_file"
                    )
                }},
                {{
                    parse_techspeed_date(
                        "primary_election_date", "src.is_day_first_file"
                    )
                }}
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

        from source_with_date_order as src
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
    and {{ dsar_not_suppressed("email", "email") }}
    and {{ dsar_not_suppressed("phone_clean", "phone") }}
