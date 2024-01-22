select
    to_char(mau.activity_end, 'yyyy-mm') as mau_end,
    avg((mau.activity_end <= mcu.activity_end + 1)::int::float) as caused_by_mcu_lapse
from dbt.zrh_user_activity_txn mau
left join dbt.mcu_activity mcu
    on mau.user_created = mcu.user_created
    and mau.activity_start < mcu.activity_end
    and mau.activity_end > mcu.activity_start
where mau.activity_type = '1_tx_35'
    and mau.activity_end < date('2021-12-01')
    and mau.activity_end > date('2019-01-01')
group by 1
