with raw_data as (
    select
        mau.user_created,
        mau.activity_start as mau_activity_start,
        mcu.activity_start as mcu_activity_start,
        mau.activity_end as mau_activity_end,
        mcu.activity_end as mcu_activity_end
    from dbt.zrh_user_activity_txn mau
    left join dbt.mcu_activity mcu
        on mau.user_created = mcu.user_created
        and mau.activity_start < mcu.activity_end
        and mau.activity_end > mcu.activity_start
    where mau.activity_type = '1_tx_35'
        and mau.activity_end > date('2021-01-01')
), base as (
    select
        case when mcu_activity_start is null then 'only MAU' else 'MCU and MAU' end as no_card_mau,
        case
            when extract('day' from mau_activity_end - mau_activity_start) <= 35
                then '1 month'
            when extract('day' from mau_activity_end - mau_activity_start) <= 65
                then '2 months'
            when extract('day' from mau_activity_end - mau_activity_start) <= 95
                then '3 months'
            when extract('day' from mau_activity_end - mau_activity_start) > 95
                then '3+ months'
        end as activity_length,
        count(*) as activity_periods
    from raw_data
    group by 1, 2
)
select
    no_card_mau,
    activity_length,
    activity_periods::numeric / sum(activity_periods) over (partition by no_card_mau) as percentage
from base
