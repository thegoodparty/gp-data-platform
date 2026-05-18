{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "candidacy"],
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
    -- Pre-dedupe int__civics_candidate_ballotready to one row per
    -- br_candidate_id. That model dedupes by gp_candidate_id, not
    -- br_candidate_id, so a single BR person whose S3 candidacy rows had
    -- inconsistent email/phone can produce multiple gp_candidate_ids and
    -- thus multiple rows with the same br_candidate_id. Without this dedup
    -- the join below fans out and the downstream slug-level QUALIFY picks a
    -- non-deterministic gp_candidate_id. max() on the UUID string is
    -- deterministic.
    civics_candidate_by_br as (
        select
            br_candidate_id,
            max(gp_candidate_id) as gp_candidate_id,
            max(email) as email
        from {{ ref("int__civics_candidate_ballotready") }}
        where br_candidate_id is not null
        group by br_candidate_id
    ),
    -- (gp_candidate_id, br_position_database_id) -> is_incumbent. Aggregates
    -- civics.candidacy to handle persons who run for the same position across
    -- cycles; takes any non-null is_incumbent (TS supplies it, ~51k populated;
    -- BR/DDHQ/gp_api don't carry it). Within a (candidate, position) pair,
    -- incumbency rarely flips, so any-value is safe.
    civics_candidacy_attrs as (
        select
            gp_candidate_id,
            br_position_database_id,
            bool_or(is_incumbent) as is_incumbent
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
            tbl_civics_candidate.gp_candidate_id,
            tbl_civics_candidate.email,
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
        -- BR candidate.database_id (= S3 br_candidate_id) -> canonical gp_candidate_id
        left join
            civics_candidate_by_br as tbl_civics_candidate
            on tbl_candidacy.candidate_database_id
            = tbl_civics_candidate.br_candidate_id
        left join
            civics_candidacy_attrs as tbl_civics_attrs
            on tbl_civics_candidate.gp_candidate_id = tbl_civics_attrs.gp_candidate_id
            and tbl_int_race.br_position_database_id
            = tbl_civics_attrs.br_position_database_id
    ),
    -- Downstream consumer (LLM-driven onboarding campaign planner) needs every
    -- candidate in the race regardless of party; the prior major-party filter
    -- excluded Democrat / Republican / Conservative Party / Progressive / DCP
    -- and has been dropped per DATA-1922.
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
    is_incumbent,
    slug,
    race_id
from deduped_candidacy
