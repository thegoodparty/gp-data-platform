-- Stripe linkage floor. The customer id is read from a JSON key the product
-- writes at checkout, so a rename or a shape change there takes every id to
-- null with no error anywhere: the id column carries no tests, the count column
-- coalesces to zero, and every Stripe customer simply becomes an orphan. A
-- ratio rather than an absolute so it survives growth in either table.
-- Orphans were 16.6% of Stripe customers when this was written.
with
    orphans as (select count(*) as n from {{ ref("int__key_resolution_exceptions") }}),
    customers as (
        select count(*) as n from {{ ref("stg_airbyte_source__stripe_api_customers") }}
    ),
    ratio as (
        select
            (select n from orphans) as orphan_count,
            (select n from customers) as customer_count,
            (select n from orphans) * 100.0 / (select n from customers) as pct
    )

select orphan_count, customer_count, pct
from ratio
where pct > 40 or customer_count = 0
