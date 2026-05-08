/*
    PMF (Product-Market Fit) web survey responses from HubSpot Feedback Surveys.
    Combines all surveys whose name starts with 'Serve PMF' or 'Win PMF' —
    these share identical fields but target different audiences. Filtering by
    name prefix (rather than hs_survey_id) means any future date-stamped
    relaunch is automatically included. As of 2026-04: id 8 = 'Serve PMF -
    Web survey', id 12 = 'Win PMF - Web survey'. Use the `pmf_variant`
    column to split or compare cohorts.

    Joined to HubSpot contacts for additional user context.

    Grain: One row per survey response.

    PMF Score = (count of 'Very Disappointed' / total responses) * 100
*/
with

    submissions as (
        select *
        from {{ ref("stg_airbyte_source__hubspot_api_feedback_submissions") }}
        where survey_name ilike 'Serve PMF%' or survey_name ilike 'Win PMF%'
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

    contact_icp as (
        select id as hs_contact_id, icp_win, icp_serve, icp_win_supersize
        from {{ ref("int__hubspot_contacts") }}
    ),

    final as (
        select
            -- identifiers
            s.submission_id,

            -- survey metadata
            s.hs_survey_id,
            s.survey_name,
            s.survey_channel,
            -- Pattern-match on survey_name prefix so future date-stamped
            -- relaunches (e.g. 'Serve PMF - Web survey (...)') keep mapping
            -- to the right variant without a code change.
            case
                when s.survey_name ilike 'Serve PMF%'
                then 'Serve'
                when s.survey_name ilike 'Win PMF%'
                then 'Win'
            end as pmf_variant,

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
            s.pmf_additional_feedback,

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

            icp.icp_win,
            icp.icp_serve,
            icp.icp_win_supersize,

            -- submission context
            s.submission_url,
            s.submitted_at,
            s.created_at,
            s.updated_at

        from submissions s
        left join contacts c on s.hs_contact_id = c.hs_contact_id
        left join contact_icp icp on s.hs_contact_id = icp.hs_contact_id
    )

select *
from final
