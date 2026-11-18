-- A pair HubSpot keeps refusing never merges, and a warn is enough to notice it:
-- the pair stays in the queue, so the only cost of a missed warn is that it stays.
-- Only pairs whose latest row is still `failed` count, so a pair that later merged
-- or went missing stops warning.
{{ config(severity="warn") }}

with
    per_run as (
        select
            secondary_id,
            run_id,
            attempted_at,
            status,
            row_number() over (
                partition by secondary_id order by attempted_at desc
            ) as recency
        from {{ ref("stg_airflow_source__hubspot_contact_merge_log") }}
    ),

    failures as (
        select secondary_id, count(distinct run_id) as failed_runs
        from per_run
        where status = 'failed'
        group by secondary_id
        having count(distinct run_id) >= 3
    )

select p.secondary_id, p.run_id, p.attempted_at, p.status, f.failed_runs
from per_run as p
inner join failures as f on f.secondary_id = p.secondary_id
where p.recency = 1 and p.status = 'failed'
