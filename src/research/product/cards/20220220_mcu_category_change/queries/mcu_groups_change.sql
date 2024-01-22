with base as (
    select
        u.user_created,
        u.ft_mau,
        date_diff('day', u.ft_mau, current_date) as active_days,
        max(mcu.activity_end) as last_activity_end,
        coalesce(sum((mcu.user_created is not null)::int), 0) as mcu_periods,
        coalesce(sum(date_diff('day', activity_start, least(current_date, activity_end))), 0) as mcu_days,
        mcu_days::float / active_days as mcu_rate,
        case
            when last_activity_end > current_date then 'Active'
            when date_diff('month', last_activity_end, current_date) > 5 then 'Lapsed'
            when date_diff('month', last_activity_end, current_date) <= 5 then 'Dormant'
        end as mcu_status
    from dbt.zrh_users u
    left join dbt.mcu_activity mcu
        on u.user_created = mcu.user_created
        and mcu.activity_start < current_date
    where u.ft_mau < current_date
        and u.closed_at is null
        and u.ft_mau < current_date
    group by 1, 2
), final_data as (
    select
        user_created,
        case
            when mcu_periods = 0 then 'Potential'
            when mcu_status != 'Active' then mcu_status
            when mcu_rate > 0.9 then 'Continuous'
            when mcu_periods < 3 then 'Undecided'
            when mcu_periods >= 3 then 'Sporadic'
        end as mcu_category
    from base
), base_aug as (
    select
        u.user_created,
        u.ft_mau,
        date_diff('day', u.ft_mau, dateadd('month', -6, current_date)) as active_days,
        max(mcu.activity_end) as last_activity_end,
        coalesce(sum((mcu.user_created is not null)::int), 0) as mcu_periods,
        coalesce(sum(date_diff('day', activity_start, least(dateadd('month', -6, current_date), activity_end))), 0) as mcu_days,
        mcu_days::float / active_days as mcu_rate,
        case
            when last_activity_end > dateadd('month', -6, current_date) then 'Active'
            when date_diff('month', last_activity_end, dateadd('month', -6, current_date)) > 5 then 'Lapsed'
            when date_diff('month', last_activity_end, dateadd('month', -6, current_date)) <= 5 then 'Dormant'
        end as mcu_status
    from dbt.zrh_users u
    left join dbt.mcu_activity mcu
        on u.user_created = mcu.user_created
        and mcu.activity_start < dateadd('month', -6, current_date)
    where u.ft_mau < dateadd('month', -6, current_date)
        and (u.closed_at is null or u.closed_at > dateadd('month', -6, current_date))
        and u.ft_mau < current_date
    group by 1, 2
), final_data_aug as (
    select
        user_created,
        case
            when mcu_periods = 0 then 'Potential'
            when mcu_status != 'Active' then mcu_status
            when mcu_rate > 0.9 then 'Continuous'
            when mcu_periods < 3 then 'Undecided'
            when mcu_periods >= 3 then 'Sporadic'
        end as mcu_category
    from base_aug
)
select
    final_data_aug.mcu_category as start_category,
    coalesce(final_data.mcu_category, 'xClosed') as end_category,
    case when count(*) > 40000 then end_category else ' ' end as display_text,
    count(*)
from final_data_aug
left join final_data
    using (user_created)
group by 1, 2
