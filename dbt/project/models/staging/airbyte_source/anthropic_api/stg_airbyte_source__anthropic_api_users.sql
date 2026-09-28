with

    source as (select * from {{ source("airbyte_source", "anthropic_api_users") }}),

    -- the metric blocks land as nested JSON; flatten the counters the spend
    -- dashboard reads. Per-product blocks are always present (zeros, not null,
    -- when a product is unused), so these casts do not manufacture nulls.
    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(activity_date as date) as activity_date,
            user_id,
            user:email_address::string as user_email,
            chat_metrics:distinct_conversation_count::bigint
            as chat_distinct_conversation_count,
            chat_metrics:message_count::bigint as chat_message_count,
            chat_metrics:thinking_message_count::bigint as chat_thinking_message_count,
            chat_metrics:distinct_projects_used_count::bigint
            as chat_distinct_projects_used_count,
            chat_metrics:distinct_projects_created_count::bigint
            as chat_distinct_projects_created_count,
            chat_metrics:distinct_artifacts_created_count::bigint
            as chat_distinct_artifacts_created_count,
            chat_metrics:distinct_skills_used_count::bigint
            as chat_distinct_skills_used_count,
            chat_metrics:connectors_used_count::bigint as chat_connectors_used_count,
            chat_metrics:distinct_connectors_used_count::bigint
            as chat_distinct_connectors_used_count,
            chat_metrics:distinct_files_uploaded_count::bigint
            as chat_distinct_files_uploaded_count,
            claude_code_metrics:core_metrics.distinct_session_count::bigint
            as claude_code_distinct_session_count,
            claude_code_metrics:core_metrics.commit_count::bigint
            as claude_code_commit_count,
            claude_code_metrics:core_metrics.pull_request_count::bigint
            as claude_code_pull_request_count,
            claude_code_metrics:core_metrics.lines_of_code.added_count::bigint
            as claude_code_lines_added,
            claude_code_metrics:core_metrics.lines_of_code.removed_count::bigint
            as claude_code_lines_removed,
            claude_code_metrics:tool_actions.edit_tool.accepted_count::bigint
            as claude_code_edit_tool_accepted,
            claude_code_metrics:tool_actions.edit_tool.rejected_count::bigint
            as claude_code_edit_tool_rejected,
            claude_code_metrics:tool_actions.multi_edit_tool.accepted_count::bigint
            as claude_code_multi_edit_tool_accepted,
            claude_code_metrics:tool_actions.multi_edit_tool.rejected_count::bigint
            as claude_code_multi_edit_tool_rejected,
            claude_code_metrics:tool_actions.write_tool.accepted_count::bigint
            as claude_code_write_tool_accepted,
            claude_code_metrics:tool_actions.write_tool.rejected_count::bigint
            as claude_code_write_tool_rejected,
            claude_code_metrics:tool_actions.notebook_edit_tool.accepted_count::bigint
            as claude_code_notebook_edit_tool_accepted,
            claude_code_metrics:tool_actions.notebook_edit_tool.rejected_count::bigint
            as claude_code_notebook_edit_tool_rejected,
            cowork_metrics:distinct_session_count::bigint
            as cowork_distinct_session_count,
            cowork_metrics:message_count::bigint as cowork_message_count,
            cowork_metrics:action_count::bigint as cowork_action_count,
            cowork_metrics:skills_used_count::bigint as cowork_skills_used_count,
            cowork_metrics:connectors_used_count::bigint
            as cowork_connectors_used_count,
            design_metrics:distinct_session_count::bigint
            as claude_design_distinct_session_count,
            design_metrics:message_count::bigint as claude_design_message_count,
            office_metrics:excel.distinct_session_count::bigint
            as office_excel_session_count,
            office_metrics:excel.message_count::bigint as office_excel_message_count,
            office_metrics:powerpoint.distinct_session_count::bigint
            as office_powerpoint_session_count,
            office_metrics:powerpoint.message_count::bigint
            as office_powerpoint_message_count,
            office_metrics:word.distinct_session_count::bigint
            as office_word_session_count,
            office_metrics:word.message_count::bigint as office_word_message_count,
            office_metrics:outlook.distinct_session_count::bigint
            as office_outlook_session_count,
            office_metrics:outlook.message_count::bigint
            as office_outlook_message_count,
            cast(web_search_count as bigint) as web_search_count,
            cast(last_activity_date as date) as last_activity_date

        from source

    )

select *
from renamed
