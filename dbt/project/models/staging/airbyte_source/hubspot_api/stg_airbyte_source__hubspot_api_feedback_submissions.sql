select
    -- identifiers
    id as submission_id,

    -- survey metadata
    properties:hs_survey_id::string as hs_survey_id,
    properties:hs_survey_name::string as survey_name,
    properties:hs_survey_type::string as survey_type,
    properties:hs_survey_channel::string as survey_channel,

    -- submission details
    cast(properties:hs_submission_timestamp::string as timestamp) as submitted_at,
    properties:hs_response_group::string as response_group,
    properties:hs_content::string as response_content,
    properties:hs_submission_url::string as submission_url,

    -- contact details (denormalized from HubSpot)
    properties:hs_contact_id::string as hs_contact_id,
    properties:hs_contact_email_rollup::string as contact_email,
    properties:hs_contact_firstname::string as contact_first_name,
    properties:hs_contact_lastname::string as contact_last_name,

    -- PMF-specific fields
    properties:pmf_survey_4_options::string as pmf_response,
    properties:pmf_additional_feedback::string as pmf_additional_feedback,

    -- satisfaction survey fields
    properties:user_satisfaction_rating::string as satisfaction_rating,
    cast(properties:user_satisfaction_stars::string as int) as satisfaction_stars,
    properties:survey_additional_notes::string as survey_additional_notes,

    -- research fields
    properties:user_research_opt_in::string as user_research_opt_in,
    properties:win_user_research_opt_in::string as win_user_research_opt_in,
    properties:feature_request::string as feature_request,

    -- timestamps
    cast(createdat as timestamp) as created_at,
    cast(updatedat as timestamp) as updated_at,
    _airbyte_extracted_at

from {{ source("airbyte_source", "hubspot_api_feedback_submissions") }}
