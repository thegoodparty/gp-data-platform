with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_officeholders") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("_ab_source_file_url") }},
            {{ adapter.quote("_ab_source_file_last_modified") }},
            {{ adapter.quote("office_holder_id") }} as ts_officeholder_id,
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("email_source") }},
            {{ adapter.quote("phone") }},
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace({{ adapter.quote("phone") }}, '-', ''), '_', ''
                            ),
                            '[',
                            ''
                        ),
                        ']',
                        ''
                    ),
                    '(',
                    ''
                ),
                ')',
                ''
            ) as phone_clean,
            {{ adapter.quote("phone_source") }},
            {{ adapter.quote("city") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("postal_code") }},
            {{ adapter.quote("street_address") }},
            {{ adapter.quote("county_municipality") }},
            {{ adapter.quote("district_name") }},
            {{ adapter.quote("office_name") }},
            {{ adapter.quote("office_type") }},
            {{ adapter.quote("office_level") }},
            {{ adapter.quote("office_normalized") }},
            {{ adapter.quote("position_id") }},
            {{ adapter.quote("normalized_position_id") }},
            {{ adapter.quote("normalized_location") }},
            {{ adapter.quote("level") }},
            cast({{ adapter.quote("tier") }} as int) as tier,
            {{ adapter.quote("party") }},
            {{ adapter.quote("partisan") }},
            {{ adapter.quote("is_incumbent") }} as is_incumbent_raw,
            case
                when
                    lower(trim({{ adapter.quote("is_incumbent") }}))
                    in ('true', 'yes', '1')
                then true
                when
                    lower(trim({{ adapter.quote("is_incumbent") }}))
                    in ('false', 'no', '0')
                then false
            end as is_incumbent,
            {{ adapter.quote("is_uncontested") }} as is_uncontested_raw,
            case
                when
                    lower(trim({{ adapter.quote("is_uncontested") }}))
                    in ('true', 'yes', '1')
                then true
                when
                    lower(trim({{ adapter.quote("is_uncontested") }}))
                    in ('false', 'no', '0')
                then false
            end as is_uncontested,
            {{ adapter.quote("seats_available") }},
            {{ adapter.quote("general_election_day") }},
            coalesce(
                try_cast({{ adapter.quote("general_election_day") }} as date),
                try_to_date({{ adapter.quote("general_election_day") }}, 'MM/dd/yyyy'),
                try_to_date({{ adapter.quote("general_election_day") }}, 'M/d/yyyy')
            ) as general_election_date,
            {{ adapter.quote("primary_election_day") }},
            coalesce(
                try_cast({{ adapter.quote("primary_election_day") }} as date),
                try_to_date({{ adapter.quote("primary_election_day") }}, 'MM/dd/yyyy'),
                try_to_date({{ adapter.quote("primary_election_day") }}, 'M/d/yyyy')
            ) as primary_election_date,
            {{ adapter.quote("filing_deadline") }},
            coalesce(
                try_cast({{ adapter.quote("filing_deadline") }} as date),
                try_to_date({{ adapter.quote("filing_deadline") }}, 'MM/dd/yyyy'),
                try_to_date({{ adapter.quote("filing_deadline") }}, 'M/d/yyyy')
            ) as filing_deadline_date,
            {{ adapter.quote("date_processed") }},
            coalesce(
                try_cast({{ adapter.quote("date_processed") }} as date),
                try_to_date({{ adapter.quote("date_processed") }}, 'MM/dd/yyyy'),
                try_to_date({{ adapter.quote("date_processed") }}, 'M/d/yyyy')
            ) as date_processed_date,
            {{ adapter.quote("running_for_re_election_2025") }},
            {{ adapter.quote("running_for_re_election_2026") }},
            {{ adapter.quote("url_for_running_for_re_election_2025") }},
            {{ adapter.quote("url_for_running_for_re_election_2026") }},
            {{ adapter.quote("ts_status") }},
            {{ adapter.quote("ts_comment") }}

        from source
    )
select *
from renamed
