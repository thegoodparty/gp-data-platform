with
    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    final as (
        select date_trunc('month', created_at) as signup_month, count(*) as user_count
        from users
        group by date_trunc('month', created_at)
    )

select *
from final
order by signup_month
