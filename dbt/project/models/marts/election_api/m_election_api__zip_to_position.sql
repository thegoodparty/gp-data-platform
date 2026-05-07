{{
    config(
        materialized="table",
        tags=["mart", "election_api", "officepicker"],
    )
}}

with
    future_elections as (
        select
            election_date,
            election_year,
            state,
            office_level,
            office_type,
            city,
            district,
            is_judicial,
            br_position_database_id
        from {{ ref("election") }}
        where
            election_date > current_date()
            and election_date <= current_date() + interval 2 years
    ),

    zip_to_position as (
        select
            zip_code,
            br_database_id,
            max(voters_in_zip) as voters_in_zip,
            sum(voters_in_zip_district) as voters_in_zip_district,
            sum(voters_in_zip_district) * 1.0 / max(voters_in_zip)
                as pct_districtzip_to_zip
        from {{ ref("int__zip_code_to_br_office") }}
        where br_database_id is not null
        group by zip_code, br_database_id
    ),

    positions as (
        select id as position_id, name, br_database_id
        from {{ ref("m_election_api__position") }}
        where district_id is not null
    ),

    officepicker as (
        select
            pos.position_id,
            pos.name,
            zips.zip_code,
            elec.election_year,
            case
                when elec.is_judicial then 'Judicial' else elec.office_level
            end as display_office_level,
            elec.office_type,
            elec.state,
            elec.city,
            elec.district,
            elec.election_date,
            elec.br_position_database_id as br_database_id,
            zips.voters_in_zip,
            zips.voters_in_zip_district,
            zips.pct_districtzip_to_zip
        from future_elections as elec
        left join
            zip_to_position as zips
            on zips.br_database_id = elec.br_position_database_id
        inner join positions as pos on pos.br_database_id = elec.br_position_database_id
    )

select *
from officepicker
