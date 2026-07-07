-- Entity Resolution prematch: unions BallotReady, TechSpeed, DDHQ, and GP API
-- candidacy-stage records into one standardized schema for Splink matching.
-- Grain: one row per source candidacy-stage; key unique_id = source_name|source_id.
with
    -- Nickname aliases: aggregate nicknames per canonical name into an array
    -- so Splink can use ArrayIntersectLevel to detect nickname matches
    -- (e.g. robert ↔ bob).
    -- Produces an array like [`daniel`, `dan`, `danny`]
    nickname_aliases as (
        -- Normalize the seed with the same alpha-only rule applied to
        -- first_name below, so both the join key and the alias array members
        -- are in normalized form. Without this, the 4 seed nicknames carrying
        -- punctuation (e.g. casey -> "k.c.", leroy -> "l.r.") would land
        -- un-normalized in the alias array and never intersect a normalized
        -- first_name ("kc") in Splink's ArrayIntersectLevel.
        select
            {{ first_name_normalized("name1") }} as name1,
            array_distinct(
                array_append(
                    collect_list({{ first_name_normalized("name2") }}),
                    {{ first_name_normalized("name1") }}
                )
            ) as aliases
        from {{ ref("nicknames") }}
        group by {{ first_name_normalized("name1") }}
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
            initcap(br.level) as office_level,
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
            br.partisan_type,
            -- first_seen_at for BR is computed in the person mint from native
            -- br_candidate_id (spans candidacies + terms), not here.
            cast(null as timestamp) as first_seen_at
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
            -- TS staging emits mixed-case values (e.g. 'COUNTY', 'LOCAL', 'local')
            -- alongside the dominant initcap form. Normalize here to mirror
            -- initcap(br.level) on ballotready_stages above and keep the unioned
            -- prematch column on a single canonical vocabulary.
            initcap(ts.office_level) as office_level,
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
            cast(null as string) as partisan_type,
            -- Earliest processing date on this row; airbyte extract fills gaps
            -- where date_processed did not parse. Feeds the person mint.
            coalesce(
                cast(
                    coalesce(
                        try_cast(ts.date_processed as date),
                        try_to_date(ts.date_processed, 'MM/dd/yyyy'),
                        try_to_date(ts.date_processed, 'M/d/yyyy')
                    ) as timestamp
                ),
                ts._airbyte_extracted_at
            ) as first_seen_at
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
            cast(null as string) as partisan_type,
            -- DDHQ has no native created field; the single master CSV shares one
            -- extract time, so this is deterministic and full-refresh safe.
            d._airbyte_extracted_at as first_seen_at
        from ddhq_staging as d
    ),

    -- GP API: campaign data from the GP platform (joins already resolved
    -- in the campaigns mart: campaign + user + organization + position).
    -- One row per latest-version campaign with a populated state, office,
    -- and user name.
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

    -- BR race spine for resolving election_stage by br_race_id. Excludes
    -- disabled races and pre-2026 elections.
    br_race as (
        select
            race.database_id as br_race_id,
            race.position.databaseid as br_position_id,
            {{
                derive_election_stage(
                    "race.is_primary", "race.is_runoff", "election.name"
                )
            }} as election_stage
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as election
            on race.election.databaseid = election.database_id
        where
            not coalesce(race.is_disabled, false)
            and election.election_day >= '2026-01-01'
    ),

    gp_api_with_office as (
        select
            c.*,
            brp.level as br_position_level,
            br.br_race_id,
            br.election_stage,
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
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as brp
            on c.ballotready_position_id = brp.database_id
        left join br_race as br on c.ballotready_race_id = br.br_race_id
    ),

    gp_api_stages as (
        select
            'gp_api' as source_name,
            cast(g.campaign_id as string) as source_id,
            lower(trim(g.user_first_name)) as first_name,
            lower(trim(g.user_last_name)) as last_name,
            upper(trim(g.campaign_state)) as state,
            {{ parse_party_affiliation("g.campaign_party") }} as party,
            g.candidate_office,
            -- Prefer BR's 7-bucket PositionLevel taxonomy when br_position_level
            -- is available. Mirrors initcap(br.level) on ballotready_stages
            -- above (and int__civics_elected_official_gp_api), so cross-source
            -- ExactMatch("office_level") works against BR/TS records that emit
            -- City/Township/Regional. For rows without a BR position FK, fall
            -- back to gp-api's native election_level (4-bucket: City/Local
            -- collapse to Local; cannot distinguish City/Town/Township at this
            -- granularity).
            case
                when g.br_position_level is not null
                then initcap(g.br_position_level)
                when lower(g.election_level) in ('city', 'local')
                then 'Local'
                when lower(g.election_level) = 'county'
                then 'County'
                when lower(g.election_level) = 'state'
                then 'State'
                when lower(g.election_level) = 'federal'
                then 'Federal'
                else null
            end as office_level,
            {{ map_office_type("g.candidate_office") }} as office_type,
            {{ extract_district_raw("g.campaign_office") }} as district_raw,
            {{ extract_district_identifier("g.campaign_office") }}
            as district_identifier,
            g.election_date,
            g.election_stage,
            g.user_email as email,
            g.user_phone as phone,
            cast(g.br_race_id as string) as br_race_id,
            trim(g.campaign_office) as official_office_name,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as seat_name,
            cast(null as string) as partisan_type,
            -- first_seen_at for gp_api is computed in the person mint from the
            -- user's created_at, not here (campaign grain differs from user).
            cast(null as timestamp) as first_seen_at
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
    {{ first_name_normalized("u.first_name") }} as first_name,
    u.last_name,
    -- Array of first_name + all known nicknames for Splink ArrayIntersectLevel.
    coalesce(
        na.aliases, array({{ first_name_normalized("u.first_name") }})
    ) as first_name_aliases,
    -- >=2-char first-name token array for Splink ArrayIntersectLevel: lets
    -- compound first names overlap on a shared token ("charles kirk" vs "charles")
    {{ first_name_tokens("u.first_name") }} as first_name_tokens,
    u.state,
    party,
    candidate_office,
    office_level,
    -- Normalize office_type into a clean candidacy-ER race-key discriminator.
    case
        when
            lower(trim(office_type)) in (
                'alderman',
                'alderperson',
                'town council',
                'town chair',
                'township council',
                'city commission',
                'city commissioner',
                'city council'
            )
        then 'City Council'
        -- Non-discriminating buckets -> NULL so the race-key clause wildcards.
        when lower(trim(office_type)) in ('other', 'local', 'county', 'state')
        then null
        else nullif(initcap(trim(office_type)), '')
    end as office_type,
    nullif(district_raw, '') as district_raw,
    district_identifier,
    election_date,
    election_stage,
    nullif(email, '') as email,
    nullif(phone, '') as phone,
    try_cast(br_race_id as int) as br_race_id,
    nullif(lower(trim(official_office_name)), '') as official_office_name,
    -- Normalized token array for Splink ArrayIntersectLevel office-overlap
    -- match. Captures locality + r-N school-district codes (e.g. DDHQ
    -- "lincoln county r-iv" → ["lincoln","r-4"], BR "winfield r-4 school
    -- board" → ["winfield","r-4"]) so cross-source naming variants intersect.
    {{ office_name_tokens("official_office_name") }} as official_office_name_tokens,
    nullif(br_candidacy_id, '') as br_candidacy_id,
    nullif(seat_name, '') as seat_name,
    partisan_type,
    u.first_seen_at
from unioned as u
left join
    nickname_aliases as na on {{ first_name_normalized("u.first_name") }} = na.name1
