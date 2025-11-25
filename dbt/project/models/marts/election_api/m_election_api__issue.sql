{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "issue"],
    )
}}


with
    new_issues as (
        select
            {{ generate_salted_uuid(fields=["tbl_new_issue.id"], salt="ballotready") }}
            as id,
            tbl_new_issue.created_at,
            tbl_new_issue.updated_at,
            tbl_new_issue.database_id as br_database_id,
            -- tbl_new_issue.expanded_text,
            tbl_new_issue.key,
            tbl_new_issue.name
        -- tbl_all_issues.parent_id
        from {{ ref("int__ballotready_issue") }} as tbl_new_issue
        {% if is_incremental() %}
            where tbl_new_issue.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select
    id,
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    br_database_id,
    cast('' as string) as expanded_text,  -- TODO: add expanded_text from API
    key,
    name,
    cast(null as string) as parent_id  -- TODO: add parent_id from API
from new_issues
