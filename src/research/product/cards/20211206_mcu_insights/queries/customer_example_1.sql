select
    week,
    mcu_activity.id is not null::int as mcu,
    count(zrh_card_transactions.created) as card_transactions
from dwh_cohort_weeks week
left join dbt.mcu_activity
    on activity_start < end_time
    and activity_end > start_time
    and user_created = '2019-10-11 09:18:55.903250'
left join dbt.zrh_card_transactions
    on zrh_card_transactions.user_created = '2019-10-11 09:18:55.903250'
    and zrh_card_transactions.type = 'AA'
    and zrh_card_transactions.created between start_time and end_time
where week.start_time < current_date
    and week.end_time > '2019-10-11 09:18:55.903250'
group by 1, 2
