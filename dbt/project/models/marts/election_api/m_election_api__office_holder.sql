-- Office terms for public /people profiles (election-api "OfficeHolder" table).
-- Grain: one row per BallotReady office-holder term (id =
-- gp_elected_official_term_id), plus one row per hand-added term from the
-- office_holder_term_additions seed. person_id is NOT NULL in the API, so the
-- mart inner-joins m_election_api__person: vacancies and no-name people drop
-- here rather than failing the FK at load time.
{% set independent_lines %}
    ('Independent', 'Unaffiliated', 'No Party Preference', 'Petitioning Candidate')
{% endset %}

with
    persons as (select id from {{ ref("m_election_api__person") }}),

    -- br_database_id back to numeric: the election-api mart publishes it as
    -- string, but terms join on the numeric BR id
    positions as (
        select id, cast(br_database_id as bigint) as br_database_id
        from {{ ref("m_election_api__position") }}
    ),

    -- Term fields the civics mart doesn't carry; staging carries one row per
    -- br_office_holder_id (unique-tested), and the qualify guards the join
    -- against fan-out if the feed ever re-delivers a term.
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
            br_candidate_id,
            case when size(party_names) > 0 then party_names end as party_names
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        qualify
            row_number() over (
                partition by br_office_holder_id order by office_holder_updated_at desc
            )
            = 1
    ),

    -- BallotReady terms, with a hand-verified end date replacing the feed's
    -- where a term ended early and the feed still carries the full term.
    br_terms as (
        select
            terms.gp_elected_official_term_id,
            terms.br_office_holder_id,
            terms.br_position_id,
            terms.position_name,
            terms.normalized_position_name,
            office_holder_source.office_title,
            office_holder_source.party_names,
            terms.term_start_date,
            coalesce(end_override.end_at, terms.term_end_date) as term_end_date,
            office_holder_source.term_date_specificity,
            terms.is_appointed,
            terms.is_vacant,
            office_holder_source.number_of_seats,
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
            terms.gp_person_id,
            office_holder_source.br_candidate_id
        from {{ ref("elected_official_terms") }} as terms
        left join
            office_holder_source
            on terms.br_office_holder_id = office_holder_source.br_office_holder_id
        left join
            {{ ref("office_holder_term_end_overrides") }} as end_override
            on terms.br_office_holder_id = end_override.br_office_holder_id
    ),

    -- Seat attributes for a hand-added term, from the newest BallotReady term
    -- on the same position.
    seat_attributes as (
        select
            br_position_id,
            position_name,
            normalized_position_name,
            office_title,
            number_of_seats,
            sub_area_name,
            sub_area_value,
            state,
            geo_id,
            mtfcc
        from br_terms
        qualify
            row_number() over (
                partition by br_position_id
                order by term_start_date desc nulls last, br_office_holder_id desc
            )
            = 1
    ),

    -- Terms BallotReady's office-holder file is missing. Holder-specific
    -- contact fields stay null; the seat fields come from seat_attributes.
    added_terms as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "additions.gp_person_id",
                        "additions.br_position_id",
                        "additions.start_at",
                    ],
                    salt="office_holder_term_additions",
                )
            }} as gp_elected_official_term_id,
            cast(null as bigint) as br_office_holder_id,
            additions.br_position_id,
            seat.position_name,
            seat.normalized_position_name,
            seat.office_title,
            cast(null as array<string>) as party_names,
            additions.start_at as term_start_date,
            additions.end_at as term_end_date,
            cast(null as string) as term_date_specificity,
            additions.is_appointed,
            false as is_vacant,
            seat.number_of_seats,
            cast(null as string) as mailing_address_line_1,
            cast(null as string) as mailing_address_line_2,
            cast(null as string) as mailing_city,
            cast(null as string) as mailing_state,
            cast(null as string) as mailing_zip,
            cast(null as string) as office_phone,
            cast(null as string) as office_email,
            cast(null as string) as website_url,
            cast(null as string) as linkedin_url,
            cast(null as string) as facebook_url,
            cast(null as string) as twitter_url,
            cast(null as string) as instagram_url,
            seat.sub_area_name,
            seat.sub_area_value,
            seat.state,
            seat.geo_id,
            seat.mtfcc,
            additions.gp_person_id,
            cast(null as int) as br_candidate_id
        from {{ ref("office_holder_term_additions") }} as additions
        inner join
            seat_attributes as seat on additions.br_position_id = seat.br_position_id
    ),

    all_terms as (
        select *
        from br_terms
        union all
        select *
        from added_terms
    ),

    -- A seat with one holder can't have two current terms. The feed can keep a
    -- predecessor's term open (no end date) after a replacement is seated under a
    -- new office-holder id, so an open-ended term with an older start date
    -- loses. A term with an end date is never retired here: on staggered boards
    -- mislabeled as one seat, both dated terms are real. Ties on start date and
    -- multi-member seats are left alone.
    current_single_seat_terms as (
        select
            br_office_holder_id,
            term_start_date,
            term_end_date,
            max(term_start_date) over (partition by br_position_id) as newest_start_date
        from br_terms
        where
            number_of_seats = 1
            and not coalesce(is_vacant, false)
            and term_start_date <= current_date()
            and (term_end_date is null or term_end_date >= current_date())
    ),

    superseded_terms as (
        select br_office_holder_id
        from current_single_seat_terms
        where term_start_date < newest_start_date and term_end_date is null
    ),

    -- Each BallotReady run (candidate and election day), with every party line
    -- on every candidacy row that day: the candidacy mart keeps only
    -- parties[0], which hides the major line on fusion tickets.
    run_lines as (
        select
            br_candidate_id,
            election_day,
            array_distinct(
                flatten(
                    collect_list(regexp_extract_all(parties, '"name"=>"([^"]+)"', 1))
                )
            ) as run_parties
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null and election_day is not null
        group by br_candidate_id, election_day
    ),

    -- The affiliation a run states, and the lines that state it. A run with
    -- both major lines (cross-filing) is its own class, so it never reads as a
    -- switch. Nonpartisan, minor-party and town lines state no affiliation:
    -- they describe the seat or the ballot line, not the person.
    candidate_runs as (
        select
            br_candidate_id,
            election_day,
            case
                when
                    exists (run_parties, p -> p rlike '^Democratic')
                    and exists (run_parties, p -> p rlike '^Republican')
                then 'both_major'
                when exists (run_parties, p -> p rlike '^Democratic')
                then 'democratic'
                when exists (run_parties, p -> p rlike '^Republican')
                then 'republican'
                when exists (run_parties, p -> p in {{ independent_lines }})
                then 'independent'
            end as affiliation,
            case
                affiliation
                when 'democratic'
                then filter(run_parties, p -> p rlike '^Democratic')
                when 'republican'
                then filter(run_parties, p -> p rlike '^Republican')
                when 'independent'
                then filter(run_parties, p -> p in {{ independent_lines }})
            end as affiliation_lines
        from run_lines
    ),

    latest_affiliation as (
        select br_candidate_id, affiliation
        from candidate_runs
        where affiliation is not null
        qualify
            row_number() over (partition by br_candidate_id order by election_day desc)
            = 1
    ),

    -- The latest run made while the term was being served. Matching runs to
    -- the term they fall in, rather than to the current term, keeps the
    -- correction after the term ends (profiles fall back to the last term held)
    -- and leaves earlier terms with the party they were served under.
    term_runs as (
        select
            terms.gp_elected_official_term_id,
            terms.br_candidate_id,
            case
                when
                    exists (terms.party_names, p -> p rlike '^Democratic')
                    and exists (terms.party_names, p -> p rlike '^Republican')
                then 'both_major'
                when exists (terms.party_names, p -> p rlike '^Democratic')
                then 'democratic'
                when exists (terms.party_names, p -> p rlike '^Republican')
                then 'republican'
                when exists (terms.party_names, p -> p in {{ independent_lines }})
                then 'independent'
            end as office_affiliation,
            runs.affiliation,
            runs.affiliation_lines
        from br_terms as terms
        inner join
            candidate_runs as runs
            on terms.br_candidate_id = runs.br_candidate_id
            and runs.election_day >= terms.term_start_date
            and (
                terms.term_end_date is null or runs.election_day <= terms.term_end_date
            )
        where runs.affiliation is not null
        qualify
            row_number() over (
                partition by terms.gp_elected_official_term_id
                order by runs.election_day desc
            )
            = 1
    ),

    -- BallotReady's office-holder feed keeps the party a member was elected
    -- under after a mid-term switch, while its candidacy feed is current. A run
    -- during the term that states a different affiliation wins, in either
    -- direction, but only while it is still the member's latest stated
    -- affiliation: profiles read eligibility off the last term held, so a term
    -- must not carry an affiliation the member has since moved on from.
    party_corrections as (
        select term_runs.gp_elected_official_term_id, term_runs.affiliation_lines
        from term_runs
        inner join
            latest_affiliation
            on term_runs.br_candidate_id = latest_affiliation.br_candidate_id
        where
            term_runs.affiliation in ('democratic', 'republican', 'independent')
            and term_runs.affiliation = latest_affiliation.affiliation
            and term_runs.office_affiliation != term_runs.affiliation
            and not (
                term_runs.office_affiliation = 'both_major'
                and term_runs.affiliation != 'independent'
            )
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
    -- build timestamps: the table is swap-replaced wholesale each run
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    terms.br_office_holder_id,
    terms.position_name,
    terms.normalized_position_name,
    terms.office_title,
    -- A hand-checked party override wins over the feed and the correction.
    case
        when party_override.br_office_holder_id is not null
        then split(nullif(party_override.party_names, ''), '\\|')
        else coalesce(party_corrections.affiliation_lines, terms.party_names)
    end as party_names,
    terms.term_start_date as start_at,
    terms.term_end_date as end_at,
    terms.term_date_specificity,
    case
        when terms.term_end_date < current_date()
        then false
        when terms.term_start_date > current_date()
        then false
        when superseded_terms.br_office_holder_id is not null
        then false
        when terms.term_start_date is null and terms.term_end_date is null
        then cast(null as boolean)
        else true
    end as is_current,
    terms.is_appointed,
    terms.is_vacant,
    terms.number_of_seats,
    next_elections.next_election_date,
    terms.mailing_address_line_1,
    terms.mailing_address_line_2,
    terms.mailing_city,
    terms.mailing_state,
    terms.mailing_zip,
    terms.office_phone,
    terms.office_email,
    terms.website_url,
    terms.linkedin_url,
    terms.facebook_url,
    terms.twitter_url,
    terms.instagram_url,
    terms.sub_area_name,
    terms.sub_area_value,
    terms.state,
    terms.geo_id,
    terms.mtfcc,
    terms.gp_person_id as person_id,
    positions.id as position_id
from all_terms as terms
inner join persons on terms.gp_person_id = persons.id
left join
    superseded_terms on terms.br_office_holder_id = superseded_terms.br_office_holder_id
left join
    party_corrections
    on terms.gp_elected_official_term_id = party_corrections.gp_elected_official_term_id
left join
    {{ ref("office_holder_party_overrides") }} as party_override
    on terms.br_office_holder_id = party_override.br_office_holder_id
left join positions on terms.br_position_id = positions.br_database_id
left join next_elections on terms.br_position_id = next_elections.br_position_id
