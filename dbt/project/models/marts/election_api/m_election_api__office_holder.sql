-- Office terms for public /people profiles (election-api "OfficeHolder" table).
-- Grain: one row per BallotReady office-holder term (id =
-- gp_elected_official_term_id). person_id is NOT NULL in the API, so the mart
-- inner-joins m_election_api__person: vacancies and no-name people drop here
-- rather than failing the FK at load time.
with
    terms as (
        select * from {{ ref("elected_official_terms") }} where gp_person_id is not null
    ),

    persons as (select id from {{ ref("m_election_api__person") }}),

    positions as (select id, br_database_id from {{ ref("m_election_api__position") }}),

    -- Term fields the civics mart doesn't carry; staging is unique per
    -- br_office_holder_id.
    office_holder_source as (
        select
            br_office_holder_id,
            -- the S3 feed uses '' (not null) for absent values on these
            nullif(office_title, '') as office_title,
            nullif(term_date_specificity, '') as term_date_specificity,
            number_of_seats,
            nullif(sub_area_name, '') as sub_area_name,
            nullif(sub_area_value, '') as sub_area_value,
            nullif(mtfcc, '') as mtfcc,
            -- JSON string (not a native array): the Airflow loader reads over
            -- databricks-sql-connector, which does not round-trip complex types.
            case when size(party_names) > 0 then to_json(party_names) end as party_names
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
    ),

    -- Earliest upcoming election on the same BR position: "next election" for
    -- the seat in the profile's About Office section.
    next_elections as (
        select br_position_id, min(election_date) as next_election_date
        from {{ ref("election_stage") }}
        where election_date >= current_date()
        group by br_position_id
    )

select
    terms.gp_elected_official_term_id as id,
    terms.br_office_holder_id,
    terms.position_name,
    terms.normalized_position_name,
    office_holder_source.office_title,
    office_holder_source.party_names,
    terms.term_start_date as start_at,
    terms.term_end_date as end_at,
    office_holder_source.term_date_specificity,
    case
        when terms.term_end_date < current_date()
        then false
        when terms.term_start_date > current_date()
        then false
        when terms.term_start_date is null and terms.term_end_date is null
        then cast(null as boolean)
        else true
    end as is_current,
    terms.is_appointed,
    terms.is_vacant,
    office_holder_source.number_of_seats,
    next_elections.next_election_date,
    terms.mailing_address_line_1,
    terms.mailing_address_line_2,
    terms.mailing_city,
    terms.mailing_state,
    terms.mailing_zip,
    terms.office_phone,
    terms.email as office_email,
    terms.website_url,
    terms.linkedin_url,
    terms.facebook_url,
    terms.twitter_url,
    get(
        filter(terms.urls, x -> x.type = 'instagram' and x.url is not null), 0
    ).url as instagram_url,
    office_holder_source.sub_area_name,
    office_holder_source.sub_area_value,
    terms.state,
    terms.br_geo_id as geo_id,
    office_holder_source.mtfcc,
    terms.gp_person_id as person_id,
    positions.id as position_id
from terms
inner join persons on terms.gp_person_id = persons.id
left join
    office_holder_source
    on terms.br_office_holder_id = office_holder_source.br_office_holder_id
left join positions on terms.br_position_id = positions.br_database_id
left join next_elections on terms.br_position_id = next_elections.br_position_id
