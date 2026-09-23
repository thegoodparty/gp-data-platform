-- A delivery file that carries both day-first evidence (a first part above 12)
-- and month-first evidence (a second part above 12) is internally mixed. Values
-- with evidence parse by their own evidence; only the ambiguous ones (both parts
-- 12 or under) follow the file-level call, so a mixed file is worth a look but
-- no longer misparses. The first such file arrived 2026-09-22.
{{ config(severity="warn") }}

with
    file_dates as (
        select _ab_source_file_url, primary_election_date as d
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
        union all
        select _ab_source_file_url, general_election_date
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
        union all
        select _ab_source_file_url, filing_deadline
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    date_parts as (
        select
            _ab_source_file_url,
            try_cast(
                regexp_extract(d, '^([0-9]{1,2})-([0-9]{1,2})-[0-9]{2,4}$', 1) as int
            ) as first_part,
            try_cast(
                regexp_extract(d, '^([0-9]{1,2})-([0-9]{1,2})-[0-9]{2,4}$', 2) as int
            ) as second_part
        from file_dates
    )

select
    _ab_source_file_url,
    max(first_part) as max_first_part,
    max(second_part) as max_second_part
from date_parts
group by _ab_source_file_url
having max(first_part) > 12 and max(second_part) > 12
