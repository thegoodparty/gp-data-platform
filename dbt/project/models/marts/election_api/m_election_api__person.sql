-- Person spine for public /people profiles (election-api "Person" table).
-- Grain: one row per canonical person (id = gp_person_id, the id space
-- Candidacy.gp_candidate_id already publishes). Scoped to people with a
-- candidacy or office term and at least one name part (slug is NOT NULL in
-- the API). BallotReady-rich fields come from int__ballotready_person via the
-- unambiguous br_person_id, falling back to the office_holders_v3 feed:
-- int__ballotready_person is fetched candidacy-first, so elected officials
-- with no BR candidacy are absent from it, while the office feed carries
-- their name parts and links. bio/headshot/degrees/experiences exist only on
-- the API person object and stay null for them (known follow-up). GP-native
-- people keep all BR fields null.
with
    public_people as (
        select *
        from {{ ref("people") }}
        where
            (is_candidate or is_elected_official)
            and (first_name is not null or last_name is not null)
    ),

    br_person as (
        select
            database_id,
            bio_text,
            middle_name,
            nickname,
            suffix,
            full_name,
            get(
                filter(images, x -> x.type = 'default' and x.url is not null), 0
            ).url as headshot_url,
            get(
                filter(urls, x -> x.type = 'website' and x.url is not null), 0
            ).url as website_url,
            get(
                filter(urls, x -> x.type = 'linkedin' and x.url is not null), 0
            ).url as linkedin_url,
            get(
                filter(urls, x -> x.type = 'facebook' and x.url is not null), 0
            ).url as facebook_url,
            get(
                filter(urls, x -> x.type = 'twitter' and x.url is not null), 0
            ).url as twitter_url,
            get(
                filter(urls, x -> x.type = 'instagram' and x.url is not null), 0
            ).url as instagram_url,
            -- JSON strings (not native arrays): Spark's Postgres JDBC writer
            -- does not round-trip array<struct>; the writer casts to jsonb.
            case
                when size(degrees) > 0
                then
                    to_json(
                        transform(
                            degrees,
                            d -> struct(
                                d.degree as degree,
                                d.major as major,
                                d.school as school,
                                d.gradyear as `gradYear`
                            )
                        )
                    )
            end as degrees,
            case
                when size(experiences) > 0
                then
                    to_json(
                        transform(
                            experiences,
                            e -> struct(
                                e.title as title,
                                e.organization as organization,
                                e.start as start,
                                e.end as end,
                                e.type as type
                            )
                        )
                    )
            end as experiences
        from {{ ref("int__ballotready_person") }}
    ),

    -- Fallback person fields from the office feed, one row per BR person
    -- (latest term wins). The feed uses '' (not null) for absent name parts.
    office_holder_person as (
        select
            br_candidate_id,
            nullif(middle_name, '') as middle_name,
            nullif(nickname, '') as nickname,
            nullif(suffix, '') as suffix,
            website_url,
            linkedin_url,
            facebook_url,
            twitter_url,
            get(
                filter(urls, x -> x.type = 'instagram' and x.url is not null), 0
            ).url as instagram_url
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
        qualify
            row_number() over (
                partition by br_candidate_id order by office_holder_updated_at desc
            )
            = 1
    )

select
    people.gp_person_id as id,
    cast(people.br_person_id as int) as br_person_id,
    {{ slugify("concat_ws('-', people.first_name, people.last_name)") }} as slug,
    people.first_name,
    coalesce(br_person.middle_name, office_holder.middle_name) as middle_name,
    people.last_name,
    coalesce(br_person.nickname, office_holder.nickname) as nickname,
    coalesce(br_person.suffix, office_holder.suffix) as suffix,
    coalesce(
        br_person.full_name, trim(concat_ws(' ', people.first_name, people.last_name))
    ) as full_name,
    br_person.bio_text,
    br_person.headshot_url,
    coalesce(br_person.website_url, office_holder.website_url) as website_url,
    coalesce(br_person.linkedin_url, office_holder.linkedin_url) as linkedin_url,
    coalesce(br_person.facebook_url, office_holder.facebook_url) as facebook_url,
    coalesce(br_person.twitter_url, office_holder.twitter_url) as twitter_url,
    coalesce(br_person.instagram_url, office_holder.instagram_url) as instagram_url,
    people.email,
    people.phone,
    br_person.degrees,
    br_person.experiences,
    people.state
from public_people as people
left join br_person on cast(people.br_person_id as int) = br_person.database_id
left join
    office_holder_person as office_holder
    on cast(people.br_person_id as int) = office_holder.br_candidate_id
