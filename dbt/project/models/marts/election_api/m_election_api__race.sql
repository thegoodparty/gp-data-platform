{% set serving_window_predicate %}
tbl_race.election_date
between current_date() - interval '6 years' and current_date() + interval '2 years'
{% endset %}

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
    -- election cycle.
    -- win_number is carried here but has no live source: BallotReady
    -- supplies none, and the only values that ever existed come from
    -- 2023-2025 HubSpot-archive candidacies. Their races now fall inside
    -- the six-year window, but archive candidacies rarely resolve to a
    -- BallotReady race id, so in practice the column is null across the
    -- mart and consumers fall back to a computed estimate.
    -- Aggregate with any valid value; downstream Race rows that share a
    -- gp_election_id (i.e. multiple BR race stages for the same election
    -- cycle) will all carry the same values. Drop non-positive win_number
    -- sentinels (e.g. -1) an archive candidacy can attach to an in-window
    -- race after id re-keying; a sub-1 "votes to win" is never real.
    civics_race_attrs as (
        select
            gp_election_id,
            max(case when win_number >= 1 then win_number end) as win_number,
            bool_or(is_partisan) as is_partisan,
            max(office_type) as office_type,
            -- official_office_name is free text from each candidacy's own source, so
            -- a vendor row matched to the wrong race puts a second seat's office name
            -- in the group. Carry the whole set and resolve it against the race's own
            -- position name at the select; an arbitrary max here published one
            -- district's name on another district's race.
            collect_set(official_office_name) as official_office_names,
            max(office_level) as office_level
        from {{ ref("candidacy") }}
        where gp_election_id is not null
        group by gp_election_id
    ),

    -- Day type + projection key, derived together. The November rule is the
    -- fixed federal expression (Tuesday after the first Monday in November,
    -- even years); the state's primary day comes from the derived calendar,
    -- which excludes November-general collisions, so the two branches are
    -- disjoint. The state that picks the calendar row is the resolved
    -- district's (override-corrected) state where a district exists, the
    -- race's own state otherwise. Where a district exists that is the same
    -- state the projection join is keyed by, so tag and join can never
    -- disagree about whose primary day applies; a no-district race carries a
    -- NULL join tuple that never matches, so only its tag is served.
    -- model_election_code speaks the model-output vocabulary for
    -- the join; election_code lands the API's enum spelling on the race row.
    race_projection_key as (
        select
            tbl_race.id,
            tbl_district.state as district_state,
            tbl_district.l2_district_type,
            tbl_district.l2_district_name,
            case
                when
                    year(tbl_race.election_date) % 2 = 0
                    and cast(tbl_race.election_date as date)
                    = {{ november_general_election_day("year(tbl_race.election_date)") }}
                then 'General'
                when tbl_primary.state is not null
                then 'Primary'
                else 'Local_or_Municipal'
            end as model_election_code,
            case
                when model_election_code = 'Local_or_Municipal'
                then 'LocalOrMunicipal'
                else model_election_code
            end as election_code
        from {{ ref("int__enhanced_race") }} as tbl_race
        left join
            {{ ref("m_election_api__position") }} as tbl_position
            on cast(tbl_race.br_position_database_id as string)
            = tbl_position.br_database_id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_position.district_id = tbl_district.id
        left join
            {{ ref("int__election_calendar_primary_ballotready") }} as tbl_primary
            on coalesce(tbl_district.state, tbl_race.state) = tbl_primary.state
            and cast(tbl_race.election_date as date) = tbl_primary.election_date
        where
            -- The window is defined once at the top of the file, so the two
            -- uses cannot drift: the base relation is a view over the full
            -- race graph, and an unwindowed second traversal would roughly
            -- double the mart's dominant nightly work on a
            -- common-subexpression gamble
            {{ serving_window_predicate }}
    )

select
    tbl_race.id,
    tbl_race.created_at,
    -- Ride the greater of the race's own BR timestamp and the place row's
    -- updated_at (the place mart bumps it whenever a slug changes, so a
    -- dependent race republishes its rebuilt slug and place_id in the same
    -- build). Deliberately source-stable: a filing-address override changes
    -- row content without bumping updated_at, and civics-joined columns can
    -- also change without a bump. A bump implies the row changed; the
    -- reverse does not hold, and nothing downstream consumes this column as
    -- a change signal.
    greatest(tbl_race.updated_at, tbl_place.updated_at) as updated_at,
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
    coalesce(
        filing_date_overrides.filing_date_start, tbl_race.filing_date_start
    ) as filing_date_start,
    coalesce(
        filing_date_overrides.filing_date_end, tbl_race.filing_date_end
    ) as filing_date_end,
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
    -- Take the candidacy office name when it agrees with the race's own position
    -- name, or when the group is unanimous. A conflicting group with no agreeing
    -- value publishes nothing: consumers already read the office name off
    -- position_names, and another seat's name is worse than none. Match on the
    -- singular position_name — position_names aggregates every sibling seat
    -- sharing the position's geo_id, so its first element is often another
    -- district's name.
    case
        when array_contains(tbl_civics.official_office_names, tbl_race.position_name)
        then tbl_race.position_name
        when size(tbl_civics.official_office_names) = 1
        then element_at(tbl_civics.official_office_names, 1)
    end as official_office_name,
    tbl_civics.office_level,
    tbl_projection_key.election_code,
    -- Delivery contract: the Postgres projection columns are integers (same
    -- as the legacy served table); cast at the landing site so the loader
    -- never relies on implicit numeric coercion.
    cast(tbl_projection.ballots_projected as int) as projected_turnout,
    cast(tbl_projection.ballots_projected_lower as int) as projected_turnout_lower,
    cast(tbl_projection.ballots_projected_upper as int) as projected_turnout_upper,
    -- Freshness stamp: when the joined projection was produced. NULL exactly
    -- where the projection columns are NULL (no projection joined).
    tbl_projection.inference_at
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
    on cast(tbl_race.br_position_database_id as string) = tbl_position.br_database_id
left join
    {{ ref("election_api_race_filing_address_overrides") }} as filing_overrides
    on tbl_race.br_database_id = filing_overrides.br_database_id
-- Filing windows are set by statute for a whole class of offices in a state, so
-- the override is keyed at that grain rather than per race. BallotReady stores
-- them per race and can populate a whole state's worth from a stale template.
left join
    {{ ref("election_api_race_filing_date_overrides") }} as filing_date_overrides
    on tbl_race.state = filing_date_overrides.state
    and cast(tbl_race.election_date as date) = filing_date_overrides.election_date
    and tbl_race.position_level = filing_date_overrides.position_level
    and tbl_race.partisan_type = filing_date_overrides.partisan_type
inner join
    race_projection_key as tbl_projection_key on tbl_race.id = tbl_projection_key.id
-- One projection row per (district, year, day type): the model output is
-- unique on that key per model_version, and the build asserts a single
-- model_version. Years outside the model's horizon and districts it does
-- not cover simply do not join: those races carry NULL projections by
-- design (no fallback, nothing invented).
left join
    {{ ref("int__voter_turnout_inference") }} as tbl_projection
    on tbl_projection_key.district_state = tbl_projection.state
    and tbl_projection_key.l2_district_type = tbl_projection.district_type
    and tbl_projection_key.l2_district_name = tbl_projection.district_name
    and year(tbl_race.election_date) = tbl_projection.election_year
    and tbl_projection_key.model_election_code = tbl_projection.election_code
where
    -- serve races from 6 years past through 2 years out, so recently-passed and
    -- historical races stay queryable. The window is defined once at the top
    -- of the file. The nightly race sync's staged swap delivers whatever the
    -- mart emits
    {{ serving_window_predicate }}
    -- Race -> Position -> District -> ProjectedTurnout is the chain the API
    -- depends on; a Race with no matching Position can't serve the
    -- campaign-strategy-context endpoint (no projected_turnout, no district
    -- traversal). ~36/270k upstream races land with no Position FK match;
    -- drop them rather than ship rows that fail downstream.
    and tbl_position.id is not null
