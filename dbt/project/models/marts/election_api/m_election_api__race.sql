{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}


with
    -- Pre-aggregate civics.election_stage to one row per br_race_id. The
    -- mart has known duplicates on br_race_id (TS-found-race sentinels like
    -- 'ts_found_race_net...' plus a handful of numeric collisions); without
    -- this dedup the left join fans out the race grain.
    --
    -- For gp_election_id, prefer the BallotReady-sourced row when present:
    -- BR is the authoritative carrier of a numeric br_race_id (1 BR race ->
    -- 1 gp_election_id), so any_value across BR + TS collisions could
    -- attach a numeric race to the wrong election cycle. Fall back to any
    -- non-BR row only when no BR row exists for this br_race_id.
    stage_per_br_race as (
        select
            br_race_id,
            coalesce(
                max(
                    case
                        when array_contains(source_systems, 'ballotready')
                        then gp_election_id
                    end
                ),
                max(gp_election_id)
            ) as gp_election_id,
            max(number_of_seats) as number_of_seats
        from {{ ref("election_stage") }}
        where br_race_id is not null
        group by br_race_id
    ),

    -- One row per gp_election_id with the race-level civics attributes
    -- (is_partisan, office_type, official_office_name, office_level), which are
    -- position-grain and in practice invariant across candidacies within an
    -- election cycle. win_number is carried here too but has no live source:
    -- BallotReady supplies no win number, and the only values that exist come
    -- from the 2023-2025 HubSpot archive (int__civics_candidacy_2025), whose
    -- past elections fall well outside this mart's date window (a 2-month
    -- grace period at the oldest), so win_number is null for every row this
    -- mart emits, and consumers fall back
    -- to a computed estimate. Aggregate with any valid value; downstream Race
    -- rows that share a gp_election_id (i.e. multiple BR race stages for the
    -- same election cycle) will all carry the same values. Drop non-positive
    -- win_number sentinels (e.g. -1) an archive candidacy can attach to an
    -- in-window race after id re-keying; a sub-1 "votes to win" is never real.
    civics_race_attrs as (
        select
            gp_election_id,
            max(case when win_number >= 1 then win_number end) as win_number,
            bool_or(is_partisan) as is_partisan,
            max(office_type) as office_type,
            max(official_office_name) as official_office_name,
            max(office_level) as office_level
        from {{ ref("candidacy") }}
        where gp_election_id is not null
        group by gp_election_id
    )

select
    tbl_race.id,
    tbl_race.created_at,
    -- Bump updated_at when a filing-address override applies (BallotReady's
    -- own updated_at does not change), and otherwise ride the place row's
    -- updated_at: the place mart bumps it whenever a slug changes
    -- (disambiguation in either direction), so every dependent race clears
    -- the election-api write model's incremental gate (updated_at greater
    -- than the postgres max) in the same run and republishes its rebuilt
    -- slug and place_id. Comparing slugs instead would miss a place moving
    -- back from a suffixed slug to a clean one.
    case
        when filing_overrides.br_database_id is not null
        then current_timestamp()
        else greatest(tbl_race.updated_at, tbl_place.updated_at)
    end as updated_at,
    tbl_race.br_hash_id,
    tbl_race.br_database_id,
    tbl_race.election_date,
    tbl_race.state,
    tbl_race.position_level,
    tbl_race.position_geo_id as position_geoid,
    regexp_replace(
        tbl_race.normalized_position_name, '//', '-'
    ) as normalized_position_name,
    tbl_race.position_description,
    coalesce(
        filing_overrides.filing_office_address, tbl_race.filing_office_address
    ) as filing_office_address,
    tbl_race.filing_phone_number,
    tbl_race.paperwork_instructions,
    tbl_race.filing_requirements,
    tbl_race.is_runoff,
    tbl_race.is_primary,
    tbl_race.partisan_type,
    tbl_race.filing_date_start,
    tbl_race.filing_date_end,
    tbl_race.employment_type,
    tbl_race.eligibility_requirements,
    tbl_race.salary,
    tbl_race.sub_area_name,
    tbl_race.sub_area_value,
    tbl_race.frequency,
    tbl_race.place_id,
    -- Build the race slug from the place mart's (slug-disambiguated) place
    -- slug rather than the raw upstream place_name_slug, so the race slug
    -- always extends the place slug election-api actually serves. The
    -- '-ccd' strip on the position part preserves the previous derivation,
    -- which stripped it from the whole concatenated slug.
    -- Fall back to position_names when normalized_position_name is absent, and
    -- use concat_ws so a fully missing position (both null) degrades to just the
    -- place slug instead of nulling the whole value (concat returns null on any
    -- null arg). position_names is an array; element_at(.., 1) is its first
    -- entry (Databricks is 1-indexed).
    concat_ws(
        '/',
        tbl_place.slug,
        replace(
            {{
                slugify(
                    "coalesce(tbl_race.normalized_position_name, element_at(tbl_race.position_names, 1))"
                )
            }},
            '-ccd',
            ''
        )
    ) as slug,
    tbl_race.position_names,
    tbl_position.id as position_id,
    tbl_stage.number_of_seats,
    tbl_civics.win_number,
    tbl_civics.is_partisan,
    tbl_civics.office_type,
    tbl_civics.official_office_name,
    tbl_civics.office_level
from {{ ref("int__enhanced_race") }} as tbl_race
-- Inner join: a race whose place is absent from the place mart cannot be
-- served (Race.placeId must resolve), and this is also where the race picks
-- up the disambiguated place slug.
inner join
    {{ ref("m_election_api__place") }} as tbl_place on tbl_race.place_id = tbl_place.id
left join
    stage_per_br_race as tbl_stage
    on cast(tbl_race.br_database_id as string) = tbl_stage.br_race_id
left join
    civics_race_attrs as tbl_civics
    on tbl_stage.gp_election_id = tbl_civics.gp_election_id
left join
    {{ ref("m_election_api__position") }} as tbl_position
    on tbl_race.br_position_database_id = tbl_position.br_database_id
left join
    {{ ref("election_api_race_filing_address_overrides") }} as filing_overrides
    on tbl_race.br_database_id = filing_overrides.br_database_id
where
    -- serve races from 6 years past through 2 years out, so recently-passed and
    -- historical races stay queryable. This is the sole election_date window for
    -- the Race table; write__election_api_db.py loads whatever the mart emits
    tbl_race.election_date
    between current_date() - interval '6 years' and current_date() + interval '2 years'
    -- Race -> Position -> District -> ProjectedTurnout is the chain the API
    -- depends on; a Race with no matching Position can't serve the
    -- campaign-strategy-context endpoint (no projected_turnout, no district
    -- traversal). ~36/270k upstream races land with no Position FK match;
    -- drop them rather than ship rows that fail downstream.
    and tbl_position.id is not null
