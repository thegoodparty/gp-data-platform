-- The clock each Serve user's journey is measured on, for the User lookup
-- page: days into their elected-office term where the term is known, else
-- days since they started Serve. One row per Serve user. Internal only.
with
    serve_users as (
        select user_id, serve_started_at
        from {{ ref("user_journey") }}
        where coalesce(is_serve_user, false)
    ),

    -- An official can match several terms. Keep the one their Serve start
    -- falls in, else the latest.
    terms as (
        select t.user_id, t.term_start_date, t.term_end_date
        from {{ ref("user_elected_office_terms") }} as t
        join serve_users as u using (user_id)
        where t.term_start_date is not null
        qualify
            row_number() over (
                partition by t.user_id
                order by
                    case
                        when
                            to_date(
                                u.serve_started_at
                            ) between t.term_start_date and coalesce(
                                t.term_end_date, date '9999-12-31'
                            )
                        then 0
                        else 1
                    end,
                    t.term_start_date desc,
                    t.term_end_date desc
            )
            = 1
    )

select
    u.user_id,
    t.term_start_date,
    t.term_end_date,
    coalesce(
        cast(t.term_start_date as timestamp), cast(u.serve_started_at as timestamp)
    ) as anchor_at,
    case
        when t.term_start_date is not null then 'term' else 'serve_start'
    end as anchor_kind
from serve_users as u
left join terms as t using (user_id)
