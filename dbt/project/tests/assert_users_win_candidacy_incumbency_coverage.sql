-- Incumbency coverage on the Win user spine must not silently regress.
--
-- is_incumbent is three-valued, so not_null cannot guard it: a broken seat or
-- term join upstream would turn labels into nulls while every other test on
-- this model stayed green. 88% of the current cycle classifies today, so 80%
-- is the floor. The population guard keeps a barely-started future cycle from
-- failing this on a handful of early sign-ups.
with
    cycle as (
        select count(*) as total, count(is_incumbent) as classified
        from {{ ref("users_win_candidacy") }}
        where
            is_latest_version
            and not is_demo
            and year(coalesce(general_election_date, election_date))
            = year(current_date())
    )

select total, classified
from cycle
where total >= 1000 and classified < total * 0.80
