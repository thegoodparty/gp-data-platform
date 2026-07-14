{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}

-- election-api OfficeHolder spine (one row per BallotReady office-holder term).
-- Sourced from the civics elected_official_terms mart. person_id is required by
-- the API (FK to Person, ON DELETE CASCADE), so vacancy terms with no resolved
-- person are dropped. position_id is a nullable FK resolved through the
-- election-api Position mart on the BallotReady position id.
with
    terms as (select * from {{ ref("elected_official_terms") }}),

    positions as (
        select id as position_id, cast(br_position_id as string) as br_position_id
        from {{ ref("m_election_api__position") }}
    )

select
    terms.gp_elected_official_term_id as id,
    terms.created_at,
    terms.updated_at,
    terms.br_office_holder_id,

    -- Office identity
    terms.position_name,
    terms.normalized_position_name,
    terms.candidate_office as office_title,
    -- Prisma party_names is TEXT[]; the civics term carries one affiliation.
    array_compact(array(terms.party_affiliation)) as party_names,

    -- Term
    terms.term_start_date as start_at,
    terms.term_end_date as end_at,
    cast(null as string) as term_date_specificity,
    case
        when terms.is_vacant
        then false
        when
            terms.term_start_date is not null and terms.term_start_date > current_date()
        then false
        when terms.term_end_date is not null and terms.term_end_date < current_date()
        then false
        else true
    end as is_current,
    terms.is_appointed,
    terms.is_vacant,
    cast(null as int) as number_of_seats,
    cast(null as date) as next_election_date,

    -- Mailing address
    terms.mailing_address_line_1,
    terms.mailing_address_line_2,
    terms.mailing_city,
    terms.mailing_state,
    terms.mailing_zip,

    -- Contact + links
    terms.office_phone,
    terms.email as office_email,
    terms.website_url,
    terms.linkedin_url,
    terms.facebook_url,
    terms.twitter_url,
    get(
        filter(terms.urls, x -> x.type = 'instagram' and x.url is not null), 0
    ).url as instagram_url,

    -- Geography / district
    cast(null as string) as sub_area_name,
    cast(null as string) as sub_area_value,
    terms.state,
    terms.br_geo_id as geo_id,
    cast(null as string) as mtfcc,

    -- Relations
    terms.gp_person_id as person_id,
    positions.position_id
from terms
left join positions on cast(terms.br_position_id as string) = positions.br_position_id
where terms.gp_person_id is not null
