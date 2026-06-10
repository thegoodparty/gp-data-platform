{{ config(materialized="table") }}

-- Daily time spine required by the dbt semantic layer (MetricFlow).
-- Covers a wide range so any current or historical metric can join to it.
with
    days as (
        {{
            dbt_utils.date_spine(
                "day",
                start_date="cast('2015-01-01' as date)",
                end_date="cast('2035-01-01' as date)",
            )
        }}
    )

select cast(date_day as date) as date_day
from days
