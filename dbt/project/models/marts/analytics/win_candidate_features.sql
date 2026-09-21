-- The Win candidate feature layer: one row per Win user, carrying every
-- feature the corroboration model is composed from, with the availability
-- contract attached to each column in the YAML.
--
-- Two things this table deliberately does NOT carry:
-- Scores. A score is a model output with a model id behind it; it lives in
-- model_predictions so a feature change and a score change stay separable.
-- Anything knowable only after signup. Every column here is gated to what
-- was true when the user arrived, which is what makes a point-in-time
-- training join against `as_of` honest. The governed election-day
-- incumbency flag is therefore reachable on the user spine and on
-- int__win_candidate_incumbency, but not from here.
--
-- Read the column metadata before using a column in a time-based model.
-- `pit_class`, `available_from` and `gate` record the point-in-time exercise
-- this feature layer already did, so the next modeler does not repeat it.
-- `claim_reserved_for` names the claim a column is an input to: a model input
-- cannot also be evidence about the group its own score defines, so an
-- analysis making one of those claims must read the matching restricted
-- score rather than this column set in full.
with
    spine as (
        select
            u.user_id,
            u.campaign_id,
            cast(u.ballotready_position_id as bigint) as br_position_id,
            u.campaign_state,
            u.office_type,
            u.br_normalized_position_type,
            u.campaign_party,
            u.voter_count
        from {{ ref("users_win_candidacy") }} as u
        where u.is_latest_version and not coalesce(u.is_demo, false)
        -- Matches the campaign choice every upstream feature model makes.
        qualify
            row_number() over (
                partition by u.user_id
                order by u.election_date desc nulls last, u.campaign_id desc
            )
            = 1
    ),

    candidacy_link as (
        select s.user_id, cand.gp_candidacy_id
        from spine as s
        inner join
            {{ ref("candidacy") }} as cand on s.campaign_id = cand.product_campaign_id
        qualify
            row_number() over (partition by s.user_id order by cand.gp_candidacy_id) = 1
    )

select
    -- Keys and lineage
    s.user_id,
    sf.signup_date as as_of,
    -- The BR position and L2 attribute blocks are read from a current
    -- snapshot rather than as of signup, so a consumer comparing two vintages
    -- needs to know which snapshot produced the row.
    current_timestamp() as snapshot_at,
    s.campaign_id,
    cl.gp_candidacy_id,
    s.br_position_id,

    -- Categoricals
    s.campaign_state,
    s.office_type,
    s.br_normalized_position_type,
    s.campaign_party,
    sf.email_domain_class,

    -- Timing and race scale
    sf.days_signup_to_election,
    sf.days_signup_to_deadline,
    sf.signed_up_after_deadline,
    sf.has_deadline,
    ln(1 + cast(s.voter_count as double)) as log_voter_count,
    cast(s.voter_count is null as int) as voter_count_missing,
    sf.n_br_by_deadline,
    sf.any_br_by_deadline,
    sf.br_arrivals_missing,

    -- Name shapes
    sf.name_n_tokens,
    sf.name_len,
    sf.name_has_digit,
    sf.name_has_nonalpha,
    sf.name_single_char_token,

    -- Geographic consistency
    sf.zip_campaign_state_match,
    sf.geo_state_match,
    sf.geo_zip_state_match,
    sf.geo_country_nonus,
    sf.geo_missing,

    -- Product-side shapes
    sf.usr_phone_present,
    sf.usr_phone_area_code_matches_state,
    sf.usr_email_is_role_address,
    sf.usr_email_has_office_words,
    sf.det_has_occupation,
    sf.det_has_past_experience,
    sf.det_has_website,
    sf.det_has_running_against,
    sf.det_has_campaign_committee,
    sf.det_has_custom_issues,
    sf.det_has_fun_fact,
    sf.det_n_selfreport_keys,

    -- BallotReady position attributes
    pf.pos_seats,
    pf.pos_tier,
    pf.pos_min_age,
    pf.pos_max_filing_fee,
    pf.pos_filing_fee_known,
    pf.pos_filing_fee_is_zero,
    pf.pos_filing_req_len,
    pf.pos_eligibility_len,
    pf.pos_paperwork_len,
    pf.pos_signature_count,
    pf.pos_signature_count_known,
    pf.pos_has_primary,
    pf.pos_majority_vote_primary,
    pf.pos_ranked_choice_general,
    pf.pos_is_partisan,
    pf.pos_is_judicial,
    pf.pos_is_staggered_term,
    pf.pos_unknown_boundaries,
    pf.pos_must_be_registered_voter,
    pf.pos_must_be_resident,
    pf.pos_must_have_prof_experience,
    pf.pos_salary_amount,
    pf.pos_salary_period,
    pf.pos_salary_class,
    pf.pos_salary_known,
    pf.pos_employment_type,
    pf.pos_partisan_type,
    pf.pos_br_level,
    pf.pos_election_frequency_years,
    pf.pos_holder_count,
    pf.pos_holder_known,
    pf.pos_holder_any_vacant,

    -- Person-level incumbency at signup
    inc.inc_is_officeholder_at_signup,
    inc.inc_serving_at_signup,
    inc.inc_holds_same_position,
    inc.inc_n_offices,
    inc.inc_years_in_office_at_signup,
    inc.inc_days_to_term_end,
    inc.inc_term_ends_within_1y
from spine as s
left join {{ ref("int__win_signup_features") }} as sf on sf.user_id = s.user_id
left join candidacy_link as cl on cl.user_id = s.user_id
left join
    {{ ref("int__win_position_features") }} as pf
    on pf.br_position_id = s.br_position_id
left join {{ ref("int__win_candidate_incumbency") }} as inc on inc.user_id = s.user_id
