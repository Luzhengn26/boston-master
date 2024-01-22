with base_users as (
    select
        u.user_created,
        coalesce(sum((mcu.user_created is not null)::int), 0) as mcu_periods,
        min(mcu.activity_start) as first_start,
        max(mcu.activity_end) as last_end
    from dbt.zrh_users u
    left join dbt.mcu_activity mcu
        on mcu.user_created = u.user_created
    where u.closed_at is null
        and u.ft_mau is not null
    group by 1
), mcu_category as (
    select
        case
            when mcu_periods < 3 then mcu_periods::text
            when mcu_periods < 5 then '3-4'
            else '5+'
        end as mcu_periods,
        count(*) as mcu
    from base_users
    group by 1
)
select
    mcu_periods,
    mcu::float / sum(mcu) over () as rate
from mcu_category