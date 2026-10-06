-- First-touch UTM attribution per user, from Amplitude. One row per user who
-- carries any.
--
-- Reads the SDK's initial_utm_* family only. Amplitude holds three
-- overlapping UTM families (this one, the webapp's own utm_* and utm_*_first,
-- and a space-separated set that ran for one week in May 2026), and unioning
-- them mixes different touches. The SDK family is first-touch by construction
-- and reaches the most users.
--
-- Each device captures its own initial values, so a user seen on two devices
-- can carry two. The three columns are taken together from the user's
-- earliest event that carries them, so source, medium and campaign always
-- describe the same touch. Values are left raw, typos included; the
-- _normalized columns beside them fold spellings of one channel together.
{{ config(materialized="table") }}

with
    utm_events as (
        select
            try_cast(user_id as bigint) as user_id,
            event_time,
            named_struct(
                'source',
                user_properties:initial_utm_source::string,
                'medium',
                user_properties:initial_utm_medium::string,
                'campaign',
                user_properties:initial_utm_campaign::string
            ) as utm
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            try_cast(user_id as bigint) is not null
            -- A blank source names no channel, so it is no touch.
            and nullif(trim(user_properties:initial_utm_source::string), '') is not null
    ),

    first_touch as (
        select user_id, min_by(utm, event_time) as utm from utm_events group by user_id
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
        from first_touch
    )

select
    c.user_id,
    c.utm.source as utm_source_first,
    c.utm.medium as utm_medium_first,
    c.utm.campaign as utm_campaign_first,
    coalesce(m.normalized_source, c.source_key) as utm_source_first_normalized,
    lower(trim(c.utm.medium)) as utm_medium_first_normalized
from cleaned as c
left join source_map as m on m.raw_source = c.source_key
