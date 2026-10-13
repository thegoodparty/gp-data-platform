-- Last-touch UTM at signup per user, from Amplitude. One row per user whose
-- registration event carries any.
--
-- Reads utm_*_last off the user's registration event: the campaign in front of
-- them when they signed up, as opposed to int__user_first_touch_utm's first
-- arrival. The three columns come from the same event, so they describe one
-- touch. A user with more than one registration event keeps the earliest.
-- Normalization follows the first-touch model's rule so the two compare.
{{ config(materialized="table") }}

with
    registrations as (
        select
            try_cast(user_id as bigint) as user_id,
            event_time,
            named_struct(
                'source',
                event_properties:utm_source_last::string,
                'medium',
                event_properties:utm_medium_last::string,
                'campaign',
                event_properties:utm_campaign_last::string
            ) as utm
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            event_type = 'Onboarding - Registration Completed'
            and try_cast(user_id as bigint) is not null
            -- A blank source names no channel, so it is no touch.
            and nullif(trim(event_properties:utm_source_last::string), '') is not null
    ),

    signup_touch as (
        select user_id, min_by(utm, event_time) as utm
        from registrations
        group by user_id
    ),

    source_map as (
        select raw_source, normalized_source from {{ ref("utm_source_normalization") }}
    ),

    cleaned as (
        select
            user_id,
            utm,
            -- One value arrived as a whole query string; the source is what
            -- precedes the first '&'.
            split_part(lower(trim(utm.source)), '&', 1) as source_key
        from signup_touch
    )

select
    c.user_id,
    c.utm.source as utm_source_last,
    c.utm.medium as utm_medium_last,
    c.utm.campaign as utm_campaign_last,
    coalesce(
        m.normalized_source, nullif(c.source_key, ''), 'unknown'
    ) as utm_source_last_normalized,
    lower(trim(c.utm.medium)) as utm_medium_last_normalized
from cleaned as c
left join source_map as m on m.raw_source = c.source_key
