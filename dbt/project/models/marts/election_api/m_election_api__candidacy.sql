{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}


with
    active_candidacy as (
        select
            {{ generate_salted_uuid(fields=["tbl_candidacy.id"], salt="ballotready") }}
            as id,
            tbl_candidacy.id as br_hash_id,
            tbl_candidacy.database_id as br_database_id,
            tbl_candidacy.candidate_database_id,
            -- person-crosswalk key; built here so the join block stays flat
            'ballotready|'
            || cast(tbl_candidacy.candidate_database_id as string) as person_record_key,
            tbl_candidacy.race_database_id,
            tbl_candidacy.created_at,
            tbl_candidacy.updated_at
        from {{ ref("int__ballotready_candidacy") }} as tbl_candidacy
        inner join
            {{ ref("m_election_api__race") }} as tbl_race
            on tbl_candidacy.race_database_id = tbl_race.br_database_id
    ),
    tbl_party as (
        select
            candidacy_id,
            case when size(parties) > 0 then parties[0].name else null end as party
        from {{ ref("int__ballotready_party") }}
    ),
    -- Resolve every BR candidate to its person mint via the crosswalk. Going
    -- through int__civics_candidate_ballotready alone drops a br_candidate_id
    -- that lost its person group's dedup (a BR-conflict cluster that merged >1
    -- br_candidate_id keeps only one row, keyed by the surviving id), leaving its
    -- candidacies with a null gp_candidate_id. The crosswalk is one row per
    -- record_key ('ballotready|<br_candidate_id>'), so every br_candidate_id
    -- resolves to its gp_person_id.
    person_xwalk as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where record_key like 'ballotready|%'
    ),
    -- Person-grain contact attributes. int__civics_candidate_ballotready is one
    -- row per person (gp_candidate_id = gp_person_id), so keying on the person
    -- also surfaces contact data for the merged-away br_candidate_ids above.
    civics_contact_by_person as (
        select gp_candidate_id, email, website_url
        from {{ ref("int__civics_candidate_ballotready") }}
    ),
    -- (gp_candidate_id, br_position_database_id) -> is_incumbent for the
    -- most recent election cycle. The civics candidacy source is a
    -- multi-year union (2025 archive + 2026+); a candidate who was the
    -- incumbent in a prior cycle but is a challenger now would otherwise
    -- show is_incumbent=TRUE under bool_or. max_by on general_election_date
    -- scopes to the current cycle's value.
    --
    -- Databricks max_by(value, order) returns NULL for the whole group
    -- when every `order` value is NULL. Some civics rows have no
    -- general_election_date (special-election candidates, certain 2025
    -- archive rows). Fall back to max_by on updated_at (populated on
    -- every candidacy row) so the most recently updated row wins
    -- within an all-null-date group — NOT max(is_incumbent), which on
    -- a Boolean is equivalent to bool_or and would re-introduce the
    -- stale-incumbent propagation this CTE was rewritten to prevent
    -- (e.g. a 2025 archive incumbent + 2026 special challenger both
    -- with null dates would collapse to TRUE under max).
    civics_candidacy_attrs as (
        select
            gp_candidate_id,
            br_position_database_id,
            coalesce(
                max_by(is_incumbent, general_election_date),
                max_by(is_incumbent, updated_at)
            ) as is_incumbent
        from {{ ref("candidacy") }}
        where gp_candidate_id is not null and br_position_database_id is not null
        group by gp_candidate_id, br_position_database_id
    ),
    enhanced_candidacy as (
        select
            tbl_candidacy.id,
            tbl_candidacy.br_database_id,
            tbl_candidacy.created_at,
            tbl_candidacy.updated_at,
            tbl_candidacy.race_database_id,
            tbl_party.party,
            tbl_person.first_name,
            tbl_person.last_name,
            case
                when size(tbl_person.images) > 0 then tbl_person.images[0].url else null
            end as image,
            tbl_person.bio_text as about,
            transform(tbl_person.urls, url -> url.url) as urls,
            tbl_place.state,
            tbl_place.name as place_name,
            tbl_int_race.position_name as position_name,
            tbl_mart_race.frequency as election_frequency,
            tbl_mart_race.salary,
            tbl_mart_race.normalized_position_name,
            tbl_mart_race.position_description,
            person_xwalk.gp_person_id as gp_candidate_id,
            civics_contact.email,
            civics_contact.website_url,
            tbl_civics_attrs.is_incumbent,
            concat(
                coalesce(tbl_person.first_name, ''),
                '-',
                coalesce(tbl_person.last_name, '')
            ) as first_last_name_slug,
            tbl_mart_race.id as race_id
        from active_candidacy as tbl_candidacy
        left join
            tbl_party as tbl_party
            on tbl_candidacy.br_database_id = tbl_party.candidacy_id
        left join
            {{ ref("int__ballotready_person") }} as tbl_person
            on tbl_candidacy.candidate_database_id = tbl_person.database_id
        left join
            {{ ref("m_election_api__race") }} as tbl_mart_race
            on tbl_candidacy.race_database_id = tbl_mart_race.br_database_id
        left join
            {{ ref("int__enhanced_race") }} as tbl_int_race
            on tbl_mart_race.br_database_id = tbl_int_race.br_database_id
        left join
            {{ ref("m_election_api__place") }} as tbl_place
            on tbl_mart_race.place_id = tbl_place.id
        -- BR candidate.database_id (= S3 br_candidate_id) -> person mint -> contact
        left join
            person_xwalk on tbl_candidacy.person_record_key = person_xwalk.record_key
        left join
            civics_contact_by_person as civics_contact
            on person_xwalk.gp_person_id = civics_contact.gp_candidate_id
        left join
            civics_candidacy_attrs as tbl_civics_attrs
            on person_xwalk.gp_person_id = tbl_civics_attrs.gp_candidate_id
            and tbl_int_race.br_position_database_id
            = tbl_civics_attrs.br_position_database_id
    ),
    -- Downstream consumer (LLM-driven onboarding campaign planner) needs every
    -- candidate in the race regardless of party; the prior major-party filter
    -- excluded Democrat / Republican / Conservative Party / Progressive / DCP
    -- and has been dropped.
    person_with_slug as (
        select
            id,
            br_database_id,
            created_at,
            updated_at,
            party,
            state,
            place_name,
            race_database_id,
            first_name,
            last_name,
            image,
            about,
            urls,
            election_frequency,
            salary,
            normalized_position_name,
            position_name,
            position_description,
            gp_candidate_id,
            email,
            website_url,
            is_incumbent,
            first_last_name_slug,
            case
                when position_name is not null
                then
                    concat(
                        {{ slugify("first_last_name_slug") }},
                        '/',
                        {{ slugify("position_name") }}
                    )
                else {{ slugify("first_last_name_slug") }}
            end as slug,
            race_id
        from enhanced_candidacy
        where race_id is not null and first_name is not null and last_name is not null
    ),
    -- Deterministic dedup: with the major-party filter dropped, two unrelated
    -- candidates can share the same first-last/position slug. Order by
    -- updated_at first, then break ties on br_database_id so the survivor
    -- is stable across runs.
    deduped_candidacy as (
        select *
        from person_with_slug
        qualify
            row_number() over (
                partition by slug order by updated_at desc, br_database_id desc
            )
            = 1
    )

select
    id,
    br_database_id,
    created_at,
    updated_at,
    party,
    state,
    place_name,
    race_database_id,
    first_name,
    last_name,
    image,
    about,
    urls,
    election_frequency,
    salary,
    normalized_position_name,
    position_name,
    position_description,
    gp_candidate_id,
    email,
    website_url,
    is_incumbent,
    slug,
    race_id
from deduped_candidacy
