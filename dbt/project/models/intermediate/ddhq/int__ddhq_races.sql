-- Race-level union of DDHQ reported and upcoming races. is_reported flags
-- whether results have been reported by DDHQ for the race. Reported wins on
-- transient overlap: a race appearing in both feeds during the handoff
-- window (e.g. election night) is kept as reported and dropped from upcoming
-- so the unique test on ddhq_race_id holds.
with
    reported as (
        select *, true as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_reported_races") }}
    ),

    upcoming as (
        select *, false as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_upcoming_races") }}
        where
            ddhq_race_id
            not in (select ddhq_race_id from reported where ddhq_race_id is not null)
    )

select *
from reported
union all
select *
from upcoming
