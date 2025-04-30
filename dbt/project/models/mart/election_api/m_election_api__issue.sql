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
            tbl_new_issue.database_id as br_database_id,
            -- tbl_new_issue.expanded_text,
            tbl_new_issue.key,
            tbl_new_issue.name
        -- tbl_all_issues.parent_id
        from {{ ref("int__ballotready_issue") }} as tbl_new_issue
        {% if is_incremental() %}
            left anti join
                {{ this }} as tbl_existing_issues
                on tbl_new_issue.database_id = tbl_existing_issues.br_database_id
        {% endif %}
    )
    {% if is_incremental() %}
        ,
        updated_ids_issues as (
            select
                tbl_existing_issues.br_database_id,
                tbl_existing_issues.created_at as original_created_at
            from {{ this }} as tbl_existing_issues
            -- at least one column value has changed
            left anti join
                {{ ref("int__ballotready_issue") }} as tbl_all_issues
                on tbl_all_issues.database_id = tbl_existing_issues.br_database_id
                -- and tbl_all_issues.expanded_text = tbl_existing_issues.expanded_text
                and tbl_all_issues.key = tbl_existing_issues.key
                and tbl_all_issues.name = tbl_existing_issues.name
        -- and tbl_all_issues.parent_id = tbl_existing_issues.parent_id
        ),
        updated_issues_with_original_created_at as (
            select
                tbl_all_issues.id,
                tbl_updated_ids_issues.original_created_at,
                tbl_all_issues.database_id as br_database_id,
                -- tbl_all_issues.expanded_text,
                tbl_all_issues.key,
                tbl_all_issues.name
            -- tbl_all_issues.parent_id
            from {{ ref("int__ballotready_issue") }} as tbl_all_issues
            left join
                updated_ids_issues as tbl_updated_ids_issues
                on tbl_all_issues.database_id = tbl_updated_ids_issues.br_database_id
        )
    {% else %}
    {% endif %}

select
    id,
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    br_database_id,
    cast(null as string) as expanded_text,  -- TODO: add expanded_text from API
    key,
    name,
    cast(null as string) as parent_id  -- TODO: add parent_id from API
from new_issues
{% if is_incremental() %}
    union all
    select
        id,
        original_created_at as created_at,
        current_timestamp() as updated_at,
        br_database_id,
        cast(null as string) as expanded_text,  -- TODO: add expanded_text from API
        key,
        name,
        cast(null as string) as parent_id  -- TODO: add parent_id from API
    from updated_issues_with_original_created_at
{% endif %}
