with
    future_elections as (
        -- Some (br_position_database_id, election_date) pairs have multiple
        -- gp_election_ids when Splink misses clustering BR/TS rows for the
        -- same position+date; pick one canonical row to prevent fan-out below.
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
        qualify
            row_number() over (
                partition by br_position_database_id, election_date
                order by
                    case
                        when array_contains(source_systems, 'ballotready')
                        then 1
                        when array_contains(source_systems, 'techspeed')
                        then 2
                        when array_contains(source_systems, 'ddhq')
                        then 3
                        else 4
                    end,
                    gp_election_id
            )
            = 1
    ),

    -- voters_in_zip is documented as invariant per (zip, state, district_type)
    -- upstream, but here we group by (zip, br_database_id). any_value() is
    -- safe only if voters_in_zip is also constant within each (zip,
    -- br_database_id) group — confirmed empirically (0 divergent groups out
    -- of ~2.25M) and guarded going forward by
    -- tests/assert_zip_to_br_office_voters_in_zip_invariant.sql.
    zip_to_position as (
        select
            zip_code,
            br_database_id,
            any_value(voters_in_zip) as voters_in_zip,
            sum(voters_in_zip_district) as voters_in_zip_district,
            -- Guard the division: an unexpected zero/null denominator emits
            -- null rather than a divide-by-zero or silently wrong value.
            -- voters_in_zip is upstream-tested as not_null and >= 1, so the
            -- else branch should never fire today; the guard exists so a
            -- regression in those guarantees fails loudly downstream.
            case
                when any_value(voters_in_zip) > 0
                then sum(voters_in_zip_district) * 1.0 / any_value(voters_in_zip)
                else null
            end as pct_districtzip_to_zip
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
            elec.br_position_database_id as br_database_id,
            zips.voters_in_zip,
            zips.voters_in_zip_district,
            zips.pct_districtzip_to_zip
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
