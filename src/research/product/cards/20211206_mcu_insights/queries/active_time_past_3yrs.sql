with cst_data as (
    select
        mcu_activity.user_created,
        mcu_activity.activity_start,
        mcu_activity.activity_end,
        zrh_users.ft_mau,
        coalesce(closed_at, current_date) as user_end
    from dbt.mcu_activity
    inner join dbt.zrh_users
        on mcu_activity.user_created = zrh_users.user_created
),
mcu_data as (
    select
        dwh_cohort_months.month,
        cst_data.user_created,
        nullif(datediff('day', cst_data.ft_mau, dwh_cohort_months.end_time), 0) as cst_lifetime,
        sum(datediff('day', cst_data.activity_start, least(cst_data.activity_end, dwh_cohort_months.end_time))) as mcu_duration,
        floor(mcu_duration::float / cst_lifetime * 10) * 10 as mcu_rate
    from dwh_cohort_months
    left join cst_data
        on dwh_cohort_months.start_time < cst_data.user_end
        and dwh_cohort_months.end_time > cst_data.activity_start
    where dwh_cohort_months.month < '2021-12'
        and dwh_cohort_months.month > '2019-01'
    group by 1, 2, 3
)
select
    month,
    case
        when mcu_rate < 100
        then mcu_rate::text || '%-' || (mcu_rate + 9)::text || '%'
        else '_100%'
    end as time_as_mcu,
    count(*) as customers
from mcu_data
where time_as_mcu is not null
group by 1, 2
