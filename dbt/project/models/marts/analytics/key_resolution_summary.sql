{{ config(materialized="view") }}

/*
    Per-source resolution counts, so a coverage drop is readable on the Sigma
    surface instead of only in a dbt test run. Small by construction: one row
    per source per outcome.
*/
with
    keys as (select * from {{ ref("int__user_resolved_keys") }}),

    hubspot as (
        select
            'hubspot' as source_name,
            case
                when hubspot_key_source = 'none'
                then 'user_no_hubspot_contact'
                else 'resolved'
            end as reason_code,
            'users' as count_basis,
            count(*) as record_count
        from keys
        group by reason_code
    ),

    stripe as (
        select
            'stripe' as source_name,
            case
                when stripe_customer_id is null then 'user_never_paid' else 'resolved'
            end as reason_code,
            'users' as count_basis,
            count(*) as record_count
        from keys
        group by reason_code
    ),

    exceptions as (
        select
            source_name,
            reason_code,
            'source_records' as count_basis,
            count(*) as record_count
        from {{ ref("int__key_resolution_exceptions") }}
        group by source_name, reason_code
    )

select *
from hubspot
union all
select *
from stripe
union all
select *
from exceptions
