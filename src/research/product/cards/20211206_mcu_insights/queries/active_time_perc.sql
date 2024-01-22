with active_time as (
    select
        user_created,
        sum(least(current_date, activity_end) - activity_start) as active_days
    from dbt.mcu_activity
    group by 1
), base as (
    select
        user_created,
        case
            when datediff('year', user_created, current_date) < 3
            then datediff('year', user_created, current_date)::text || ' years'
            else '3+ years'
        end as cst_age,
        active_days::float / datediff('day',user_created, current_date) as active_rate
    from dbt.zrh_users
    inner join active_time
        using (user_created)
    where zrh_users.closed_at is null
), final_values as (
    select
        case
            when active_rate < 1
            then (floor(active_rate * 10) * 10)::text || '%-' || (floor(active_rate * 10) * 10 + 9)::text || '%'
            else '_100%'
        end as bracket,
        cst_age,
        count(*) as customers
    from base
    group by 1, 2
)
select
    bracket as time_as_mcu,
    cst_age as cst_tenure,
    customers::float / sum(customers) over () as customer_perc
from final_values
