-- The one election each user's row speaks for, and the six-month window
-- running up to it. One row per user whether or not an election date reaches
-- them.
--
-- Calendar windows are the wrong frame for a candidate. Win-side activity is
-- shaped by proximity to the election, not by the month, so anything measuring
-- how hard someone campaigned has to be anchored on their own election date.
-- Six months is not a guess: measured across all seven outreach sources, the
-- 90th percentile of days between a send and the election is at or under 180
-- for every channel, and the medians run 15 to 55 days.
--
-- election_date_status is the column that keeps a null honest. Only about 43k
-- of 72k users have a usable election date, and a consumer that reads the other
-- 29k as zero intensity would be reporting a missing join as inactivity.
--
-- Table, not view: read by the outreach intensity model, by its tests, and by
-- the wide journey table.
{{ config(materialized="table") }}

{#
    Election dates outside this range are data entry, not elections. The
    campaigns mart carries 34 of them, including year 24 and year 275760, and
    add_months on the latter overflows rather than returning a window.
#}
{%- set min_plausible_election_date = "date '2000-01-01'" -%}
{%- set max_plausible_election_date = "date '2100-01-01'" -%}
{%- set window_months = 6 -%}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    -- One row per campaign: the latest version, demos dropped. A demo campaign
    -- carries a placeholder election date, so anchoring on one would hand a
    -- real user a window they never ran in.
    latest_campaigns as (
        select user_id, campaign_id, campaign_version_id, election_date as raw_date
        from {{ ref("campaigns") }}
        where is_latest_version and not coalesce(is_demo, false)
    ),

    candidate_campaigns as (
        select
            user_id,
            campaign_id,
            campaign_version_id,
            case
                when
                    raw_date
                    between {{ min_plausible_election_date }}
                    and {{ max_plausible_election_date }}
                then raw_date
            end as election_date,
            raw_date is not null
            and not (
                raw_date
                between {{ min_plausible_election_date }}
                and {{ max_plausible_election_date }}
            ) as has_implausible_date
        from latest_campaigns
    ),

    -- The anchor rule the column spec settled on: the run the user is in now if
    -- there is one, else the most recent run they finished. Only 68 users hold
    -- more than one campaign, so this is cheap, but it has to be deterministic
    -- or the whole row changes basis between builds.
    anchored as (
        select *
        from candidate_campaigns
        qualify
            row_number() over (
                partition by user_id
                order by
                    case
                        when election_date >= current_date()
                        then 0
                        when election_date is not null
                        then 1
                        else 2
                    end asc,
                    case
                        when election_date >= current_date() then election_date
                    end asc nulls last,
                    election_date desc nulls last,
                    campaign_id asc
            )
            = 1
    )

select
    k.user_id,

    a.campaign_id as anchor_campaign_id,
    a.campaign_version_id as anchor_campaign_version_id,

    a.election_date,

    -- Why a window is or is not computable, so the nulls below are never read
    -- as a zero. The two unknown reasons are kept apart because they are
    -- different problems: no_campaign is a user who never started a run,
    -- no_election_date is a run whose office never resolved to an election.
    case
        when a.user_id is null
        then 'no_campaign'
        when a.has_implausible_date
        then 'invalid_date'
        when a.election_date is null
        then 'no_election_date'
        when a.election_date >= current_date()
        then 'upcoming'
        else 'past'
    end as election_date_status,

    add_months(a.election_date, -{{ window_months }}) as outreach_window_start,
    a.election_date as outreach_window_end,

    -- An open window is the trap this model exists to flag. A candidate two
    -- weeks into their six months has a low count because the window is young,
    -- not because they are quiet, and nothing about the count itself says so.
    a.election_date >= current_date() as outreach_window_is_open,

    datediff(
        a.election_date, add_months(a.election_date, -{{ window_months }})
    ) as outreach_window_days_total,
    -- Clamped at both ends: zero before the window opens, full length once the
    -- election has passed. An unclamped datediff goes negative for an election
    -- more than six months out, which would divide the wrong way in any rate.
    --
    -- The outer guard is not redundant. greatest() skips nulls in Spark rather
    -- than propagating them, so without it every user who has no window at all
    -- reads as zero days elapsed while days_total stays null, and a consumer
    -- following this column's own advice to divide by it hits a zero.
    case
        when a.election_date is not null
        then
            greatest(
                datediff(
                    least(current_date(), a.election_date),
                    add_months(a.election_date, -{{ window_months }})
                ),
                0
            )
    end as outreach_window_days_elapsed
from keys as k
left join anchored as a using (user_id)
