{{ config(materialized="view") }}

/*
    HubSpot owner id to primary team, so a contact's owner can be grouped by
    team (Campaign Success support on Win results) from Sigma. Names and emails
    are left out on purpose: the team is all the dashboards need.
*/
with
    active as (
        select id, teams, false as is_archived
        from {{ ref("stg_airbyte_source__hubspot_api_owners") }}
    ),
    -- Archived owners come from their own stream. Some ids are in both
    -- (reactivated owners); the active row is the current state.
    archived as (
        select id, teams, true as is_archived
        from {{ ref("stg_airbyte_source__hubspot_api_owners_archived") }}
        where id not in (select id from active where id is not null)
    ),
    owners as (
        select *
        from active
        union all
        select *
        from archived
    )
select
    -- String, to join user_journey.hs_owner_id directly.
    cast(id as string) as hs_owner_id,
    -- get() returns null for an owner with no primary team; [0] raises.
    get(filter(teams, t -> t.primary), 0).name as primary_team_name,
    coalesce(
        exists (teams, t -> t.primary and t.name = 'Campaign Success'), false
    ) as is_campaign_success,
    is_archived
from owners
