{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "calls", "hubspot", "prospects"],
    )
}}

-- One row per HubSpot contact_id with at least one call. Aggregates the
-- deduped int__hubspot_calls into disjoint per-outcome_family subtotals
-- plus a convenience connected_calls aggregate.
with

    exploded_calls as (
        select
            id as call_id,
            -- hs_timestamp is when the call happened in HubSpot; created_at is
            -- when the engagement record was created. Prefer hs_timestamp.
            coalesce(hs_timestamp, created_at) as call_at,
            outcome_family,
            hubspot_owner_id,
            explode(from_json(contacts, 'array<string>')) as contact_id
        from {{ ref("int__hubspot_calls") }}
        where contacts is not null and trim(contacts) not in ('', '[]')
    ),

    aggregated as (
        select
            contact_id,
            count(distinct call_id) as total_calls,
            count(
                distinct case when outcome_family = 'connected_pledge' then call_id end
            ) as connected_pledge_calls,
            count(
                distinct case when outcome_family = 'connected_reject' then call_id end
            ) as connected_reject_calls,
            count(
                distinct case when outcome_family = 'connected_other' then call_id end
            ) as connected_other_calls,
            count(
                distinct case when outcome_family = 'not_connected' then call_id end
            ) as not_connected_calls,
            count(
                distinct case when outcome_family = 'unmapped' then call_id end
            ) as unmapped_calls,
            count(
                distinct case when outcome_family = 'null_disposition' then call_id end
            ) as null_disposition_calls,
            min(call_at) as first_call_at,
            max(call_at) as last_call_at,
            max(
                case
                    when
                        outcome_family
                        in ('connected_pledge', 'connected_reject', 'connected_other')
                    then call_at
                end
            ) as last_connected_call_at,
            count(distinct hubspot_owner_id) as distinct_owners
        from exploded_calls
        group by contact_id
    ),

    last_owner as (
        select contact_id, hubspot_owner_id as last_owner_id
        from exploded_calls
        qualify row_number() over (partition by contact_id order by call_at desc) = 1
    ),

    final as (
        select
            a.contact_id,
            a.total_calls,
            a.connected_pledge_calls,
            a.connected_reject_calls,
            a.connected_other_calls,
            a.not_connected_calls,
            a.unmapped_calls,
            a.null_disposition_calls,
            a.connected_pledge_calls
            + a.connected_reject_calls
            + a.connected_other_calls as connected_calls,
            (
                a.connected_pledge_calls
                + a.connected_reject_calls
                + a.connected_other_calls
            )
            > 0 as ever_connected,
            a.first_call_at,
            a.last_call_at,
            a.last_connected_call_at,
            a.distinct_owners,
            lo.last_owner_id
        from aggregated a
        left join last_owner lo on a.contact_id = lo.contact_id
    )

select *
from final
