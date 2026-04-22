-- Civics mart elected_officials table
-- Person-level rollup from elected_official_terms.
-- Excludes vacancy rows (br_candidate_id IS NULL, is_vacant=true).
-- Picks the latest term per person for position/geography fields.
--
-- Grain: One row per elected official (person).
with
    terms as (
        select *
        from {{ ref("elected_official_terms") }}
        where br_candidate_id is not null
    ),

    person_rollup as (
        select *
        from terms
        qualify
            row_number() over (
                partition by br_candidate_id
                order by
                    term_start_date desc nulls last,
                    term_end_date desc nulls last,
                    updated_at desc,
                    br_office_holder_id desc
            )
            = 1
    )

select
    -- PK (person-level UUID from br_candidate_id)
    {{
        generate_salted_uuid(
            fields=[
                "'elected_official'",
                "cast(person_rollup.br_candidate_id as string)",
            ]
        )
    }} as gp_elected_official_id,

    -- Source IDs
    person_rollup.br_candidate_id,

    -- Name
    person_rollup.first_name,
    person_rollup.last_name,
    person_rollup.middle_name,
    person_rollup.suffix,
    person_rollup.full_name,

    -- Contact
    person_rollup.phone,
    person_rollup.email,
    person_rollup.office_phone,
    person_rollup.central_phone,

    -- Latest position/office
    person_rollup.candidate_office,
    person_rollup.office_level,
    person_rollup.office_type,

    -- Geography (from latest term)
    person_rollup.state,
    person_rollup.city,
    person_rollup.district,

    -- Latest term dates
    person_rollup.term_start_date,
    person_rollup.term_end_date,

    -- Flags
    person_rollup.is_judicial,
    person_rollup.is_incumbent,
    person_rollup.is_win_icp,
    person_rollup.is_serve_icp,
    person_rollup.is_win_supersize_icp,

    -- Party
    person_rollup.party_affiliation,

    -- Social
    person_rollup.website_url,
    person_rollup.linkedin_url,
    person_rollup.facebook_url,
    person_rollup.twitter_url,

    -- Mailing address
    person_rollup.mailing_address_line_1,
    person_rollup.mailing_address_line_2,
    person_rollup.mailing_city,
    person_rollup.mailing_state,
    person_rollup.mailing_zip,

    -- Metadata
    person_rollup.tier,
    person_rollup.source_systems,
    person_rollup.has_ts_person_enrichment,
    person_rollup.created_at,
    person_rollup.updated_at

from person_rollup
