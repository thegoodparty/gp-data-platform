/*
    PMF (Product-Market Fit) web survey responses from HubSpot Feedback Surveys.
    Filtered to survey_id = '8' (PMF - Web survey).
    Joined to HubSpot contacts for additional user context.

    Grain: One row per survey response.

    PMF Score = (count of 'Very Disappointed' / total responses) * 100
*/
with

    submissions as (
        select *
        from {{ ref("stg_airbyte_source__hubspot_api_feedback_submissions") }}
        where hs_survey_id = '8'
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
            s.survey_channel,

            -- PMF response (decoded from internal option names)
            s.pmf_response as pmf_response_raw,
            case
                s.pmf_response
                when 'Option 1'
                then 'Very Disappointed'
                when 'Option 2'
                then 'Somewhat Disappointed'
                when 'Not disappointed'
                then 'Not Disappointed'
                when 'N/A - I no longer use GoodParty'
                then 'N/A'
                else coalesce(s.pmf_response, 'N/A')
            end as pmf_response,
            coalesce(s.pmf_response = 'Option 1', false) as is_very_disappointed,

            -- respondent info (from submission)
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
