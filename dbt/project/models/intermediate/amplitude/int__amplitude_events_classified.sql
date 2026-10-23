{{ config(materialized="view") }}

/*
    One row per Amplitude event with the product it was used from.

    Shared features (Dictation, Contacts and the like) have no product of
    their own, so they take the profile they were used from: an explicit
    `product` property, then `activeProduct`, then the user's only product.
    A user with both products, neither, or no numeric id is assumed Win.
    assumed_win exists because Serve users are few (Tristan's ruling of
    2026-10-22). Property values other than win and serve are skipped.
*/
with
    events as (select * from {{ ref("stg_airbyte_source__amplitude_api_events") }}),

    catalog as (select * from {{ ref("int__amplitude_event_catalog") }}),

    users as (select user_id, is_win_user, is_serve_user from {{ ref("users") }}),

    joined as (
        select
            e.*,
            c.family,
            c.product_area,
            c.is_machine_emitted,
            c.is_recurrent,
            c.is_win,
            c.is_serve,
            c.is_shared,
            c.name_rule_family,
            case
                when lower(e.event_properties:product::string) in ('win', 'serve')
                then lower(e.event_properties:product::string)
            end as property_product,
            case
                when lower(e.event_properties:activeproduct::string) in ('win', 'serve')
                then lower(e.event_properties:activeproduct::string)
            end as active_product,
            case
                when u.is_serve_user and not u.is_win_user
                then 'serve'
                when u.is_win_user and not u.is_serve_user
                then 'win'
            end as user_product
        from events as e
        left join catalog as c on c.event_type = e.event_type
        left join users as u on u.user_id = try_cast(e.user_id as bigint)
    )

select
    * except (
        is_win,
        is_serve,
        is_shared,
        name_rule_family,
        property_product,
        active_product,
        user_product
    ),
    case
        when is_shared
        then coalesce(property_product, active_product, user_product, 'win')
        when is_win
        then 'win'
        when is_serve
        then 'serve'
    end as event_product,
    case
        when is_shared
        then
            case
                when property_product is not null
                then 'event_property'
                when active_product is not null
                then 'active_profile'
                when user_product is not null
                then 'user_product'
                else 'assumed_win'
            end
        when is_win or is_serve
        then
            case when name_rule_family = 'other' then 'govern_tag' else 'event_name' end
    end as product_source
from joined
