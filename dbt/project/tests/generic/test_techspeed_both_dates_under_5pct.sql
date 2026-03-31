-- Warns if more than 5% of TechSpeed rows have both primary and general dates
-- populated. Currently ~0.6%. If TechSpeed changes their data delivery to
-- populate both columns routinely, we may need to revisit the stage-type logic
-- which assumes primary takes priority.
{% test techspeed_both_dates_under_5pct(model) %}
    {{ config(severity="warn") }}

    select 'both_dates_proportion_exceeded' as failure_reason
    from
        (
            select
                sum(
                    case
                        when
                            primary_election_date_parsed is not null
                            and general_election_date_parsed is not null
                        then 1
                        else 0
                    end
                )
                * 100.0
                / nullif(count(*), 0) as both_dates_pct
            from {{ model }}
        )
    where both_dates_pct >= 5

{% endtest %}
