-- BR-only person-grain rollup: one row per br_candidate_id, populated with
-- the person's latest term snapshot. Grain mechanics differ from
-- int__civics_candidate_ballotready: a latest-term window over term-grain BR
-- data, not a natural person grain.
--
-- selected_* columns expose which term row was picked so debugging "why this
-- person got office X" doesn't require re-running the window.
--
-- Vacancy terms (br_candidate_id IS NULL) are filtered out.
--
-- ICP flags are intentionally NOT carried at person grain: ICP is
-- position-scoped and a latest-term scalar misrepresents it. ICP lives at
-- term grain only.
with
    br_terms as (
        select *
        from {{ ref("int__civics_elected_official_ballotready") }}
        where br_candidate_id is not null
    ),

    latest_per_person as (
        select *
        from br_terms
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
    -- PK (person-grain canonical UUID from BR intermediate)
    gp_elected_official_id,

    -- Person-grain natural key
    br_candidate_id,

    -- Audit trail: which term was chosen as "latest"
    gp_elected_official_term_id as selected_gp_elected_official_term_id,
    br_office_holder_id as selected_br_office_holder_id,

    -- Name (latest term)
    first_name,
    last_name,
    middle_name,
    suffix,
    full_name,

    -- Contact (BR-only; TS contact merged in person mart)
    email,
    phone,
    office_phone,
    central_phone,

    -- Latest-term office attributes
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,

    -- Latest term dates
    term_start_date,
    term_end_date,

    -- Term-derived flags (latest term)
    is_judicial,

    -- Party (latest term)
    party_affiliation,

    -- Social (latest term)
    website_url,
    linkedin_url,
    facebook_url,
    twitter_url,

    -- Mailing address (latest term)
    mailing_address_line_1,
    mailing_address_line_2,
    mailing_city,
    mailing_state,
    mailing_zip,

    -- Tier (latest term's BR position tier)
    br_position_tier as tier,

    -- Timestamps from latest term (NOT person's earliest appearance)
    created_at,
    updated_at

from latest_per_person
