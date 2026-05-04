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
        select zip_code, br_database_id from {{ ref("int__zip_code_to_br_office") }}
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
                when elec.is_judicial
                then 'Judicial'
                -- Normalize case mismatches in upstream office_level (rare
                -- BR/TS rows surface as LOCAL / local / COUNTY etc. instead
                -- of the title-cased canonical values).
                else initcap(lower(elec.office_level))
            end as display_office_level,
            elec.office_type,
            elec.state,
            elec.city,
            elec.district,
            elec.election_date,
            elec.br_position_database_id as br_database_id
        from future_elections as elec
        left join
            zip_to_position as zips
            on zips.br_database_id = elec.br_position_database_id
        inner join positions as pos on pos.br_database_id = elec.br_position_database_id
        -- Election mart can carry multiple rows per (br_position_database_id,
        -- election_date) due to upstream duplication in BR/TS sources (same
        -- race-position-date appearing under different gp_election_ids).
        -- Pick the row with the most populated descriptive fields per
        -- (zip, position, date) so the unique-combination test holds.
        qualify
            row_number() over (
                partition by zips.zip_code, pos.position_id, elec.election_date
                order by
                    case when elec.office_level is not null then 0 else 1 end,
                    case when elec.state is not null then 0 else 1 end,
                    case when elec.district is not null then 0 else 1 end,
                    elec.election_year desc
            )
            = 1
    )

select *
from officepicker
