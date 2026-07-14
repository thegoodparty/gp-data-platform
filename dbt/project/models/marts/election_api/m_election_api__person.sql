-- Person spine for public /people profiles (election-api "Person" table).
-- Grain: one row per canonical person (id = gp_person_id, the id space
-- Candidacy.gp_candidate_id already publishes). Scoped to people with a
-- candidacy or office term and at least one name part (slug is NOT NULL in
-- the API). BallotReady-rich fields come from int__ballotready_person via the
-- unambiguous br_person_id; GP-native people keep them null.
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
            -- JSON strings (not native arrays): the Airflow loader reads over
            -- databricks-sql-connector, which does not round-trip complex types.
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
    )

select
    people.gp_person_id as id,
    cast(people.br_person_id as int) as br_person_id,
    {{ slugify("concat_ws('-', people.first_name, people.last_name)") }} as slug,
    people.first_name,
    br_person.middle_name,
    people.last_name,
    br_person.nickname,
    br_person.suffix,
    coalesce(
        br_person.full_name, trim(concat_ws(' ', people.first_name, people.last_name))
    ) as full_name,
    br_person.bio_text,
    br_person.headshot_url,
    br_person.website_url,
    br_person.linkedin_url,
    br_person.facebook_url,
    br_person.twitter_url,
    br_person.instagram_url,
    people.email,
    people.phone,
    br_person.degrees,
    br_person.experiences,
    people.state
from public_people as people
left join br_person on cast(people.br_person_id as int) = br_person.database_id
