{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "stance"],
    )
}}

with
    exploded_stances as (
        select
            candidacy_id,
            stance.`databaseId` as br_database_id,
            stance.locale as stance_locale,
            stance.`referenceUrl` as stance_reference_url,
            stance.statement as stance_statement,
            stance.issue.`databaseId` as issue_database_id,
            created_at,
            updated_at
        from {{ ref("int__ballotready_stance") }} tbl_stance
        lateral view explode(stances) as stance
        where
            candidacy_id
            in (select br_database_id from {{ ref("m_election_api__candidacy") }})
            {% if is_incremental() %}
                and updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    ),
    enhanced_stances as (
        select
            {{
                generate_salted_uuid(
                    fields=["tbl_stance.br_database_id"], salt="ballotready"
                )
            }} as id,
            tbl_stance.created_at,
            tbl_stance.updated_at,
            tbl_stance.br_database_id,
            tbl_stance.stance_locale,
            tbl_stance.stance_reference_url,
            tbl_stance.stance_statement,
            tbl_issue.id as issue_id,
            tbl_candidacy.id as candidacy_id
        from exploded_stances as tbl_stance
        left join
            {{ ref("m_election_api__issue") }} as tbl_issue
            on tbl_stance.issue_database_id = tbl_issue.br_database_id
        left join
            {{ ref("m_election_api__candidacy") }} as tbl_candidacy
            on tbl_stance.candidacy_id = tbl_candidacy.br_database_id
    )
select
    id,
    created_at,
    updated_at,
    br_database_id,
    stance_locale,
    stance_reference_url,
    stance_statement,
    issue_id,
    candidacy_id
from enhanced_stances
