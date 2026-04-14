/*
    Win User satisfaction survey (5 stars) responses from HubSpot Feedback
    Surveys. Combines survey_id 9 and 10 (same survey, relaunched for ICP).

    Grain: One row per survey response.
*/
with

    submissions as (
        select *
        from {{ ref("stg_airbyte_source__hubspot_api_feedback_submissions") }}
        where hs_survey_id in ('9', '10')
    ),

    contacts as (
        select
            id as hs_contact_id,
            email,
            first_name,
            last_name,
            state,
            type,
            verified_candidate_status,
            office_level
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    final as (
        select
            -- identifiers
            s.submission_id,

            -- survey metadata
            s.survey_name,
            s.hs_survey_id,
            s.survey_channel,

            -- satisfaction response
            s.satisfaction_stars,
            s.satisfaction_rating,
            s.survey_additional_notes,

            -- respondent info
            s.hs_contact_id,
            s.contact_email,
            s.contact_first_name,
            s.contact_last_name,

            -- respondent context (from contacts)
            c.state,
            c.type as contact_type,
            c.verified_candidate_status,
            c.office_level,

            -- submission context
            s.submission_url,
            s.submitted_at,
            s.created_at,
            s.updated_at

        from submissions s
        left join contacts c on s.hs_contact_id = c.hs_contact_id
    )

select *
from final
