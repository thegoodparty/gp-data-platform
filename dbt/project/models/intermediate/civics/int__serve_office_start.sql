-- Whether each elected office was started by its holder. Since 2026-11-02
-- gp-api creates an office for every candidate win, used or not; only a
-- started office makes its holder a Serve user. Every Serve count reads
-- serve_start_status from here so the rule lives in one place.
with
    offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    polls as (
        select elected_office_id, min(created_at) as first_poll_at
        from {{ ref("stg_airbyte_source__gp_api_db_poll") }}
        group by elected_office_id
    ),

    classified as (
        select
            o.id as elected_office_id,
            o.user_id,
            o.organization_slug,
            o.campaign_id,
            o.created_at,
            o.onboarding_completed_at,
            o.onboarding_step,
            p.first_poll_at,
            -- gp-api creates a win's office at the 'confirm' step, with the
            -- earlier steps prefilled; the step only moves when its holder
            -- clicks Continue. No office created any other way rests
            -- unfinished at 'confirm'.
            (
                o.campaign_id is not null
                and o.onboarding_completed_at is null
                and o.onboarding_step = 'confirm'
                and p.first_poll_at is null
            ) as is_untouched_win_office,
            -- Win offices, including the Pro winner's own handoff, began on
            -- 2026-10-28; earlier campaign-linked offices came from first
            -- polls. Used only to date a win office its holder later starts.
            o.campaign_id is not null
            and o.created_at >= timestamp '2026-10-28' as is_win_created
        from offices as o
        left join polls as p on p.elected_office_id = o.id
    )

select
    elected_office_id,
    user_id,
    organization_slug,
    campaign_id,
    created_at,
    onboarding_completed_at,
    onboarding_step,
    first_poll_at,
    is_win_created,
    if(is_untouched_win_office, 'never_started', 'started') as serve_start_status,
    -- A win office can sit for months before its holder opens it, so its
    -- creation date is not when they started Serve. Without a step
    -- timestamp, a started but unfinished one falls back to creation.
    case
        when is_untouched_win_office
        then null
        when is_win_created
        then coalesce(least(onboarding_completed_at, first_poll_at), created_at)
        else created_at
    end as serve_started_at
from classified
