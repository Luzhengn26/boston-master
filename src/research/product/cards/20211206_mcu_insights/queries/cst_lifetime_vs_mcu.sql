with raw_data as (
    select
        zrh_users.user_created,
        month,
        1 as active_customer,
        -- Do a group because of activities starting/ending mid-month
        (sum((mcu_activity.id is not null)::int) > 0)::int as mcu_customer
    from dwh_cohort_months month
    inner join dbt.zrh_users
        --on zrh_users.kyc_first_completed < month.end_time
        on zrh_users.ft_mau < month.end_time
        and coalesce(zrh_users.closed_at, current_date) > month.start_time
        --and zrh_users.kyc_first_completed is not null
        and zrh_users.ft_mau is not null
    left join dbt.mcu_activity
        on zrh_users.user_created = mcu_activity.user_created
        and mcu_activity.activity_start < month.end_time
        and mcu_activity.activity_end > month.start_time
    where month < '2021-12'
    group by 1, 2
), base as (
    select
        user_created,
        month,
        active_customer,
        mcu_customer,
        rank() over (partition by user_created order by month) as lifetime_month
    from raw_data
)
select
    lifetime_month,
    sum(active_customer) as active_customers,
    sum(mcu_customer) as mcu,
    mcu::float / active_customers as mcu_rate
from base
where lifetime_month <= 48
group by 1
