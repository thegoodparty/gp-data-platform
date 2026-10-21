with
    -- One election per position supplies the display attributes: the next
    -- upcoming one, else the most recent past one. Positions are listed
    -- whether or not they have a race in any window; election-api bounds
    -- which races it offers off the Race table.
    elections as (
        select
            election_date,
            state,
            office_level,
            office_type,
            city,
            district,
            is_judicial,
            br_position_database_id,
            source_systems,
            gp_election_id,
            election_date > current_date() as is_upcoming
        from {{ ref("election") }}
        where br_position_database_id is not null
    ),

    representative_elections as (
        select *
        from elections
        qualify
            row_number() over (
                partition by br_position_database_id
                order by
                    is_upcoming desc,
                    case when is_upcoming then election_date end asc,
                    election_date desc,
                    -- Splink can leave BR/TS rows for one position and date
                    -- under separate gp_election_ids; prefer the BR row.
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
        -- br_database_id back to numeric: the election-api mart publishes it
        -- as string, but the election feed joins on the numeric BR id
        select
            id as position_id,
            name,
            state,
            cast(br_database_id as bigint) as br_database_id
        from {{ ref("m_election_api__position") }}
        where district_id is not null
    ),

    officepicker as (
        select
            pos.position_id,
            pos.name,
            zips.zip_code,
            -- Next known election only; null when the position has none
            -- upcoming.
            case when elec.is_upcoming then elec.election_date end as election_date,
            case
                when elec.is_upcoming then year(elec.election_date)
            end as election_year,
            -- Positions with no election row at all fall back to the same
            -- BallotReady position derivations the election mart uses.
            case
                when coalesce(elec.is_judicial, br_pos.is_judicial)
                then 'Judicial'
                -- Normalize case mismatches in upstream office_level (rare
                -- BR/TS rows surface as LOCAL / local / COUNTY etc. instead
                -- of the title-cased canonical values).
                else initcap(lower(coalesce(elec.office_level, br_pos.level)))
            end as display_office_level,
            coalesce(elec.office_type, pos_ot.office_type, 'Other') as office_type,
            coalesce(elec.state, pos.state) as state,
            coalesce(
                elec.city, {{ extract_city_from_office_name("br_pos.name") }}
            ) as city,
            coalesce(
                elec.district, {{ extract_district_geographic("br_pos.name") }}, ''
            ) as district,
            pos.br_database_id,
            zips.voters_in_zip,
            zips.voters_in_zip_district,
            zips.pct_districtzip_to_zip
        from positions as pos
        left join zip_to_position as zips on pos.br_database_id = zips.br_database_id
        left join
            representative_elections as elec
            on pos.br_database_id = elec.br_position_database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as br_pos
            on pos.br_database_id = br_pos.database_id
        left join
            {{ ref("int__civics_position_office_type") }} as pos_ot
            on pos.br_database_id = pos_ot.br_position_database_id
    )

-- id and the timestamps satisfy the election-api ZipToPosition Postgres
-- contract. The id is deterministic on the natural key, so it is stable across
-- rebuilds. The sync rebuilds the table every run, so created_at restates to
-- the build time rather than tracking a first-seen date.
select
    {{
        generate_salted_uuid(
            fields=["zip_code", "position_id"],
            salt="zip_to_position",
        )
    }} as id, current_timestamp() as created_at, current_timestamp() as updated_at, *
from officepicker
