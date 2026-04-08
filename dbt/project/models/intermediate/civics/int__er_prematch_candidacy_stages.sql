{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady x TechSpeed x DDHQ x GP API
-- candidacy-stages. Unions candidacy-stage records from all sources into a
-- standardized schema for Splink matching.
--
-- Grain: One row per source candidacy-stage record (candidate + office + election date)
-- Key: unique_id (source_name || '|' || source_id)
--
-- All upstream sources are at the candidacy-stage grain:
-- - BallotReady staging: one row per candidate per race (election stage)
-- - TechSpeed staging: dates/state pre-parsed, unpivoted into primary/general
-- stage rows, deduped per candidate-stage
-- - DDHQ election results: one row per candidate per race
-- - GP API: campaigns mart fanned out by BR API race into one row per stage,
-- enriched with normalized position names and stage-specific dates
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

    -- Compute candidate_office once so map_office_type can reference it
    -- without re-evaluating the full CASE expression (same pattern as
    -- ddhq_with_office and gp_api_with_office below).
    br_with_office as (
        select
            br.*,
            br_position.partisan_type,
            -- BR S3 candidacies sometimes lack emails; fill gaps from the
            -- BR API person entity when available
            coalesce(br.email, pe.api_email) as _email,
            {{
                generate_candidate_office_from_position(
                    "br.position_name",
                    "br.normalized_position_name",
                )
            }} as candidate_office
        from br_staging as br
        left join br_position on br.br_position_id = br_position.database_id
        left join person_emails as pe on br.br_candidate_id = pe.person_database_id
    ),

    ballotready_stages as (
        select
            'ballotready' as source_name,
            br.br_candidacy_id as source_id,
            lower(trim(br.first_name)) as first_name,
            lower(trim(br.last_name)) as last_name,
            br.state,
            {{ parse_party_affiliation("br.parties") }} as party,
            br.candidate_office,
            case
                when lower(br.level) = 'city' then 'Local' else initcap(br.level)
            end as office_level,
            {{ map_office_type("br.candidate_office") }} as office_type,
            {{ extract_district_raw("br.position_name") }} as district_raw,
            {{ extract_district_identifier("br.position_name") }}
            as district_identifier,
            br.election_day as election_date,
            {{
                derive_election_stage(
                    "br.is_primary", "br.is_runoff", "br.election_name"
                )
            }} as election_stage,
            br._email as email,
            br.phone as phone,
            cast(br.br_race_id as string) as br_race_id,
            br.position_name as official_office_name,
            br.br_candidacy_id,
            coalesce(
                regexp_extract(br.position_name, '[-, ] (?:Seat|Group) ([^,]+)'),
                regexp_extract(br.position_name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,
            br.partisan_type
        from br_with_office as br
    ),

    -- TechSpeed: sourced from staging (dates and state already parsed),
    -- unpivoted into primary/general stage rows
    ts_staging as (
        select
            ts.* except (election_date),
            ts.state_postal_code as state_code,
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
    ),

    -- Determine stage type: primary takes priority over general (mirrors civics
    -- election_stage pattern). If both dates exist, candidate is at primary stage.
    ts_with_stage as (
        select
            *,
            case
                when primary_election_date_parsed is not null
                then 'Primary'
                else 'General'
            end as election_stage,
            case
                when primary_election_date_parsed is not null
                then primary_election_date_parsed
                else general_election_date_parsed
            end as election_date
        from ts_staging
        where
            coalesce(primary_election_date_parsed, general_election_date_parsed)
            >= '2026-01-01'
            and year(
                coalesce(primary_election_date_parsed, general_election_date_parsed)
            )
            between 1900 and 2050
    ),

    techspeed_stages as (
        select
            'techspeed' as source_name,
            -- Stage-grain source_id (candidate_code + stage) for uniqueness
            ts.techspeed_candidate_code
            || '__'
            || lower(ts.election_stage) as source_id,
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
            ts.election_date,
            ts.election_stage,
            ts.email,
            ts.phone,
            ts.br_race_id,
            ts.official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type
        from ts_with_stage as ts
        -- Dedupe: staging is not deduplicated, keep first appearance per
        -- candidate-stage to avoid duplicate source_ids
        qualify
            row_number() over (
                partition by techspeed_candidate_code, election_stage
                order by _ab_source_file_url asc
            )
            = 1
    ),

    -- DDHQ election results: each row is a candidate x race (candidacy-stage).
    -- State resolution, office derivation, and district extraction now come
    -- from the staging model.
    ddhq_staging as (
        select *
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where
            ddhq_race_id is not null
            and candidate_id is not null
            and candidate is not null
            and trim(candidate) != ''
            -- Require splittable name (first + last)
            and size(split(trim(candidate), ' ')) >= 2
            and candidate_first_name is not null
            and candidate_last_name is not null
            and election_date is not null
            and election_date >= '2026-01-01'
            and state_postal_code is not null
    ),

    ddhq_stages as (
        select
            'ddhq' as source_name,
            cast(d.candidate_id as string)
            || '_'
            || cast(d.ddhq_race_id as string) as source_id,
            lower(trim(d.candidate_first_name)) as first_name,
            lower(trim(d.candidate_last_name)) as last_name,
            d.state_postal_code as state,
            d.party_affiliation as party,
            d.candidate_office,
            d.office_level,
            d.office_type,
            d.district as district_raw,
            coalesce(
                try_cast(regexp_extract(d.district, '([0-9]+)') as int),
                try_cast(regexp_extract(d.race_name, ' ([0-9]+)$') as int)
            ) as district_identifier,
            d.election_date,
            -- DDHQ election_type contains both stage and special indicators
            -- (e.g. "Special Election Primary", "Special General Election").
            -- Derive boolean-like expressions from the string to reuse the
            -- shared derive_election_stage macro.
            {{
                derive_election_stage(
                    "lower(d.election_type) like '%primary%'",
                    "lower(d.election_type) like '%runoff%'",
                    "d.election_type",
                )
            }} as election_stage,
            cast(null as string) as email,
            cast(null as string) as phone,
            cast(null as string) as br_race_id,
            d.official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type
        from ddhq_staging as d
    ),

    -- GP API: campaign data from the GP platform (joins already resolved
    -- in the campaigns mart: campaign + user + organization + position).
    -- Fanned out by election stage via BR API race table so each campaign
    -- produces one row per stage (Primary, General, Runoff, Special variants).
    gp_api_campaigns as (
        select *
        from {{ ref("campaigns") }}
        where
            election_date >= '2026-01-01'
            and nullif(trim(campaign_state), '') is not null
            and not coalesce(is_demo, false)
            and is_latest_version
            and nullif(trim(user_first_name), '') is not null
            and nullif(trim(user_last_name), '') is not null
            and nullif(trim(campaign_office), '') is not null
    ),

    -- BR API race: position x stage with stage-specific election dates.
    -- Joined to BR election for the actual date per stage. Excludes
    -- disabled races and pre-2026 elections. Detects special elections
    -- via election name (see ballotready_stages comment for rationale).
    br_race as (
        select
            race.position.databaseid as br_position_id,
            race.database_id as br_race_id,
            {{
                derive_election_stage(
                    "race.is_primary", "race.is_runoff", "election.name"
                )
            }} as election_stage,
            election.election_day
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as election
            on race.election.databaseid = election.database_id
        where
            not coalesce(race.is_disabled, false)
            and election.election_day >= '2026-01-01'
    ),

    -- Compute candidate_office once so map_office_type can reference it
    -- without re-evaluating the full CASE expression (same pattern as
    -- ddhq_with_office above).
    gp_api_with_office as (
        select
            c.*,
            r.br_race_id,
            r.election_stage,
            r.election_day,
            brp.level as br_position_level,
            -- Use the same normalization as BallotReady when we have the
            -- normalized_position_name; fall back to raw campaign_office
            coalesce(
                {{
                    generate_candidate_office_from_position(
                        "c.campaign_office",
                        "c.normalized_position_name",
                    )
                }},
                initcap(trim(c.campaign_office))
            ) as candidate_office
        from gp_api_campaigns as c
        inner join
            br_race as r
            on c.ballotready_position_id = r.br_position_id
            and year(r.election_day) = year(c.election_date)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as brp
            on c.ballotready_position_id = brp.database_id
        -- Dedup: a position may have multiple races of the same stage type
        -- in the same year; pick the race closest to the campaign's date
        qualify
            row_number() over (
                partition by c.campaign_id, r.election_stage
                order by
                    abs(datediff(r.election_day, c.election_date)) asc,
                    r.br_race_id desc nulls last
            )
            = 1
    ),

    gp_api_stages as (
        select
            'gp_api' as source_name,
            cast(g.campaign_id as string)
            || '__'
            || replace(lower(g.election_stage), ' ', '_') as source_id,
            lower(trim(g.user_first_name)) as first_name,
            lower(trim(g.user_last_name)) as last_name,
            upper(trim(g.campaign_state)) as state,
            {{ parse_party_affiliation("g.campaign_party") }} as party,
            g.candidate_office,
            -- Prefer campaign election_level; fall back to BR position level
            case
                when lower(g.election_level) in ('city', 'local')
                then 'Local'
                when lower(g.election_level) = 'county'
                then 'County'
                when lower(g.election_level) = 'state'
                then 'State'
                when lower(g.election_level) = 'federal'
                then 'Federal'
                when lower(g.br_position_level) in ('city', 'local', 'township')
                then 'Local'
                when lower(g.br_position_level) in ('county', 'regional')
                then 'County'
                when lower(g.br_position_level) = 'state'
                then 'State'
                when lower(g.br_position_level) = 'federal'
                then 'Federal'
                else null
            end as office_level,
            {{ map_office_type("g.candidate_office") }} as office_type,
            {{ extract_district_raw("g.campaign_office") }} as district_raw,
            {{ extract_district_identifier("g.campaign_office") }}
            as district_identifier,
            g.election_day as election_date,
            g.election_stage,
            g.user_email as email,
            g.user_phone as phone,
            cast(g.br_race_id as string) as br_race_id,
            trim(g.campaign_office) as official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type
        from gp_api_with_office as g
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
        union all
        select *
        from gp_api_stages
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
