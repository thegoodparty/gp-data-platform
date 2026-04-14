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
            official_office_name,
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
        select zip_code, district_name, br_database_id
        from {{ ref("int__zip_code_to_br_office") }}
    ),

    positions as (
        select id as position_id, br_database_id
        from {{ ref("m_election_api__position") }}
    ),

    officepicker as (
        select
            pos.position_id,
            elec.official_office_name as display_name,
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
            elec.br_position_database_id
        from future_elections as elec
        left join
            zip_to_position as zips
            on zips.br_database_id = elec.br_position_database_id
        left join positions as pos on pos.br_database_id = elec.br_position_database_id
    )

select *
from officepicker
