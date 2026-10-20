{{ config(materialized="view") }}

/*
    HubSpot owner id to primary team, so a contact's owner can be grouped by
    team (Campaign Success support on Win results) from Sigma. Names and emails
    are left out on purpose: the team is all the dashboards need.
*/
select
    -- String, to join user_journey.hs_owner_id directly.
    cast(id as string) as hs_owner_id,
    filter(teams, t -> t.primary)[0].name as primary_team_name,
    coalesce(
        exists (teams, t -> t.primary and t.name = 'Campaign Success'), false
    ) as is_campaign_success,
    coalesce(archived, false) as is_archived
from {{ ref("stg_airbyte_source__hubspot_api_owners") }}
