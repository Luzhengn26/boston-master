with base as (
    select
        u.user_created,
        u.ft_mau,
        date_diff('day', u.ft_mau, current_date) as active_days,
        coalesce(sum((mcu.user_created is not null)::int), 0) as mcu_period_count,
        coalesce(sum(date_diff('day', activity_start, least(current_date, activity_end))), 0) as mcu_days,
        mcu_days::float / active_days as mcu_rate,
        case
            when mcu_rate = 0 then 'None (0%)'
            when mcu_rate > 0.9 then 'Continuous (90%-100%)'
            when mcu_rate > 0.7 then 'High (70%-90%)'
            when mcu_rate < 0.2 then 'Low (1%-20%)'
            when mcu_rate < 0.4 then 'Sporadic (20%-40%)'
            else 'Mid (40%-70%)'
        end as activity_level,
        case
            when mcu_period_count < 3 then mcu_period_count::text
            when mcu_period_count < 5 then '3-4'
            else '5+'
        end as mcu_periods
    from dbt.zrh_users u
    left join dbt.mcu_activity mcu
        on u.user_created = mcu.user_created
    where u.ft_mau is not null
        and u.closed_at is null
        and u.ft_mau < current_date
    group by 1, 2
), final_data as (
    select
        activity_level,
        mcu_periods,
        count(*) as cst
    from base
    group by 1, 2
)
select
    *,
    cst::float / sum(cst) over () as customer_percentage,
    cst::float / sum(cst) over (partition by activity_level) as bracket_rate
from final_data