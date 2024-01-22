with raw_data as (
    select
        user_created,
        min(activity_start) as first_activity_start,
        max(activity_end) as last_activity_end,
        count(*) as active_stretches
    from dbt.mcu_activity
    group by 1
), user_cat as (
    select
        user_created,
        case
            when last_activity_end <= current_date then
                case
                    when datediff('month', first_activity_start, last_activity_end) <= 4 then 'lapsed in 4 months'
                    else 'lapsed in 4+ months'
                end
            when last_activity_end > current_date then
                case
                  when active_stretches = 1
                  then 'one continuous activity'
                  else 'lapsed/relapsed but active'
                end
        end as mcu_type
    from raw_data
), base as (
    select
        zrh_users.user_created,
        month,
        mcu_type,
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
    left join user_cat
        on zrh_users.user_created = user_cat.user_created
    left join dbt.mcu_activity
        on zrh_users.user_created = mcu_activity.user_created
        and mcu_activity.activity_start < month.end_time
        and mcu_activity.activity_end > month.start_time
    where month < '2021-12'
    group by 1, 2, 3
), lifetime_base as (
    select
        rank() over (partition by user_created order by month) as lifetime_month,
        mcu_customer,
        mcu_type
    from base
)
select
    lifetime_month,
    mcu_type,
    sum(mcu_customer) as customers
from lifetime_base
where lifetime_month <= 48
    and mcu_type is not null
group by 1, 2
