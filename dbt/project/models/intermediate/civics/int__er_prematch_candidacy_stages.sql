{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady x TechSpeed x DDHQ candidacy-stages
-- Unions candidacy-stage records from all sources into a standardized schema
-- for Splink matching.
--
-- Grain: One row per source candidacy-stage record (candidate + office + election date)
-- Key: unique_id (source_name || '|' || source_id)
--
-- All upstream sources are already at the candidacy-stage grain:
-- - BallotReady staging: one row per candidate per race (election stage)
-- - TechSpeed clean: one row per candidate per election date
-- - DDHQ election results: one row per candidate per race
--
with
    -- Nickname aliases: aggregate nicknames per canonical name into an array
    -- so Splink can use ArrayIntersectLevel to detect nickname matches
    -- (e.g. robert ↔ bob).
    -- Produces an array like [`daniel`, `dan`, `danny`]
    nickname_aliases as (
        select
            name1, array_distinct(array_append(collect_list(name2), name1)) as aliases
        from {{ ref("nicknames") }}
        group by name1
    ),

    -- BallotReady staging: each row is a candidacy-stage (candidate x race)
    br_staging as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            election_day >= '2026-01-01'
            and first_name is not null
            and last_name is not null
            and state is not null
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    -- BallotReady API person data for richer contact info
    br_person as (select * from {{ ref("int__ballotready_person") }}),

    person_emails as (
        select
            database_id as person_database_id,
            get(filter(contacts, x -> x.email is not null), 0).email as api_email
        from br_person
        where database_id is not null
    ),

    ballotready_stages as (
        select
            'ballotready' as source_name,
            br.br_candidacy_id as source_id,
            lower(trim(br.first_name)) as first_name,
            lower(trim(br.last_name)) as last_name,
            br.state,
            {{ parse_party_affiliation("br.parties") }} as party,
            {{
                generate_candidate_office_from_position(
                    "br.position_name",
                    "br.normalized_position_name",
                )
            }} as candidate_office,
            case
                when lower(br.level) = 'city' then 'Local' else initcap(br.level)
            end as office_level,
            {{
                map_office_type(
                    generate_candidate_office_from_position(
                        "br.position_name",
                        "br.normalized_position_name",
                    )
                )
            }} as office_type,
            {{ extract_district_raw("br.position_name") }} as district_raw,
            {{ extract_district_identifier("br.position_name") }}
            as district_identifier,
            -- Single election date at candidacy-stage grain
            br.election_day as election_date,
            -- Derive election stage from is_primary / is_runoff flags
            case
                when br.is_primary
                then 'Primary'
                when br.is_runoff
                then 'Runoff'
                else 'General'
            end as election_stage,
            coalesce(br.email, pe.api_email) as email,
            br.phone as phone,
            cast(br.br_race_id as string) as br_race_id,
            br.position_name as official_office_name,
            br.br_candidacy_id,
            -- Derive seat_name from position_name (same regex as candidacy model)
            coalesce(
                regexp_extract(br.position_name, '[-, ] (?:Seat|Group) ([^,]+)'),
                regexp_extract(br.position_name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,
            br_position.partisan_type
        from br_staging as br
        left join br_position on br.br_position_id = br_position.database_id
        left join person_emails as pe on br.br_candidate_id = pe.person_database_id
    ),

    -- State name → 2-letter code mapping for TechSpeed and DDHQ
    clean_states as (select * from {{ ref("clean_states") }}),

    -- TechSpeed: sourced from staging to preserve candidacy-stage grain
    ts_staging as (
        select
            ts.*,
            coalesce(cs.state_cleaned_postal_code, ts.state) as state_code,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
        where
            coalesce(
                try_cast(general_election_date as date),
                try_to_date(general_election_date, 'MM-dd-yyyy'),
                try_to_date(general_election_date, 'MM/dd/yy'),
                try_cast(primary_election_date as date),
                try_to_date(primary_election_date, 'MM-dd-yyyy'),
                try_to_date(primary_election_date, 'MM/dd/yy')
            )
            >= '2026-01-01'
            and coalesce(
                try_cast(general_election_date as date),
                try_to_date(general_election_date, 'MM-dd-yyyy'),
                try_to_date(general_election_date, 'MM/dd/yy'),
                try_cast(primary_election_date as date),
                try_to_date(primary_election_date, 'MM-dd-yyyy'),
                try_to_date(primary_election_date, 'MM/dd/yy')
            )
            < '2050-01-01'
    ),

    techspeed_stages as (
        select
            'techspeed' as source_name,
            ts.techspeed_candidate_code as source_id,
            lower(trim(ts.first_name)) as first_name,
            lower(trim(ts.last_name)) as last_name,
            ts.state_code as state,
            ts.party,
            ts.candidate_office,
            ts.office_level,
            ts.office_type,
            ts.district as district_raw,
            try_cast(
                regexp_extract(ts.district, '([0-9]+)') as int
            ) as district_identifier,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as election_date,
            -- Map is_primary to election stage (was election_type from _clean)
            case
                when
                    upper(trim(cast(ts.is_primary as string)))
                    in ('YES', 'TRUE', 'PRIMARY')
                then 'Primary'
                else 'General'
            end as election_stage,
            ts.email,
            ts.phone,
            ts.br_race_id,
            ts.official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type
        from ts_staging as ts
    ),

    -- DDHQ election results: each row is a candidate x race (candidacy-stage)
    ddhq_staging as (
        select *
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where
            race_id is not null
            and candidate_id is not null
            and candidate is not null
            and trim(candidate) != ''
            -- Require splittable name (first + last)
            and size(split(trim(candidate), ' ')) >= 2
            and date is not null
            and date >= '2026-01-01'
    ),

    -- Compute candidate_office once so map_office_type can reference it
    -- without re-evaluating the full CASE expression
    ddhq_with_office as (
        select
            ddhq.*,
            coalesce(
                cs_two.state_cleaned_postal_code, cs_one.state_cleaned_postal_code
            ) as state,
            {{ parse_ddhq_candidate_office("ddhq.race_name") }} as candidate_office,
            -- Strip state prefix from race_name so office names align with
            -- BR/TS for Splink JW matching (e.g. "New York Senate District 5"
            -- → "Senate District 5")
            trim(
                case
                    when cs_two.state_cleaned_postal_code is not null
                    then
                        substring(
                            ddhq.race_name,
                            length(
                                concat(
                                    split(ddhq.race_name, ' ')[0],
                                    ' ',
                                    split(ddhq.race_name, ' ')[1]
                                )
                            )
                            + 2
                        )
                    when cs_one.state_cleaned_postal_code is not null
                    then
                        substring(
                            ddhq.race_name, length(split(ddhq.race_name, ' ')[0]) + 2
                        )
                end
            ) as official_office_name
        from ddhq_staging as ddhq
        left join
            clean_states as cs_one
            on upper(split(ddhq.race_name, ' ')[0]) = upper(trim(cs_one.state_raw))
        left join
            clean_states as cs_two
            on upper(
                concat(
                    split(ddhq.race_name, ' ')[0], ' ', split(ddhq.race_name, ' ')[1]
                )
            )
            = upper(trim(cs_two.state_raw))
        where
            coalesce(cs_two.state_cleaned_postal_code, cs_one.state_cleaned_postal_code)
            is not null
    ),

    ddhq_stages as (
        select
            'ddhq' as source_name,
            cast(d.candidate_id as string)
            || '_'
            || cast(d.race_id as string) as source_id,
            lower(
                trim(split({{ remove_name_suffixes("trim(d.candidate)") }}, ' ')[0])
            ) as first_name,
            lower(
                trim(
                    element_at(
                        split({{ remove_name_suffixes("trim(d.candidate)") }}, ' '), -1
                    )
                )
            ) as last_name,
            d.state,
            {{ parse_party_affiliation("d.candidate_party") }} as party,
            d.candidate_office,
            {{ parse_ddhq_office_level("d.race_name") }} as office_level,
            {{ map_office_type("d.candidate_office") }} as office_type,
            -- Extract district/ward from race_name
            -- Try keyword-prefixed first (Ward 3, District 5), then fall back
            -- to any trailing number (Board of Supervisors 19)
            coalesce(
                nullif(
                    regexp_extract(
                        d.race_name,
                        '(?i)(?:ward|district|seat|place|position|zone) ([^ ]+)$'
                    ),
                    ''
                ),
                nullif(regexp_extract(d.race_name, ' ([0-9]+)$'), '')
            ) as district_raw,
            coalesce(
                try_cast(
                    regexp_extract(
                        d.race_name,
                        '(?i)(?:ward|district|seat|place|position|zone) ([0-9]+)$'
                    ) as int
                ),
                try_cast(regexp_extract(d.race_name, ' ([0-9]+)$') as int)
            ) as district_identifier,
            d.date as election_date,
            case
                when lower(d.election_type) like '%runoff%'
                then 'Runoff'
                when lower(d.election_type) like '%primary%'
                then 'Primary'
                else 'General'
            end as election_stage,
            cast(null as string) as email,
            cast(null as string) as phone,
            cast(null as string) as br_race_id,
            d.official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type
        from ddhq_with_office as d
    ),

    unioned as (
        select *
        from ballotready_stages
        union all
        select *
        from techspeed_stages
        union all
        select *
        from ddhq_stages
    )

-- Splink treats empty strings as real values for exact matching, so convert
-- sparse columns to NULL where empty so missing data is handled correctly.
select
    source_name || '|' || source_id as unique_id,
    source_id,
    u.source_name,
    u.first_name,
    u.last_name,
    -- Array of first_name + all known nicknames for Splink ArrayIntersectLevel
    coalesce(na.aliases, array(u.first_name)) as first_name_aliases,
    u.state,
    party,
    candidate_office,
    office_level,
    office_type,
    nullif(district_raw, '') as district_raw,
    district_identifier,
    election_date,
    election_stage,
    nullif(email, '') as email,
    nullif(phone, '') as phone,
    try_cast(br_race_id as int) as br_race_id,
    nullif(lower(trim(official_office_name)), '') as official_office_name,
    nullif(br_candidacy_id, '') as br_candidacy_id,
    nullif(seat_name, '') as seat_name,
    partisan_type
from unioned as u
left join nickname_aliases as na on u.first_name = na.name1
