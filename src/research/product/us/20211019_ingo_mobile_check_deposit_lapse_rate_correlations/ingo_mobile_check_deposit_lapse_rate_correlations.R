library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/src/research/product/us/20211019_ingo_mobile_check_deposit_lapse_rate_correlations","data")

start_date <- '2020-01-01'

ingo_mau <- queryDB(paste0(" 
with start_dates as (
  select 
    f.user_id,
    min(f.activity_start) as first_txn
  from dbt.f_user_activity_txn f
  join dbt.dim_users u on u.user_id = f.user_id
  where f.activity_type = '1_tx_35'
    -- this makes sure we only look at users who have at least 8 weeks with us
    and u.main_account_status = 'OPEN'
    and u.first_fraud_flag is null
  group by 1
  having first_txn between '",start_date,"' and current_date - interval '8 weeks'
)
, activity as (
  select 
    sd.user_id,
    sd.first_txn,
    sd.first_txn + interval '8 weeks' as eight_weeks,
    min(case when f.period_id = 2 then f.activity_start else '2100-01-01' end) as reactivation,
    count(distinct f.period_id) as periods,
    min(f.activity_end) as activity_end
  from dbt.f_user_activity_txn f
  join start_dates sd on sd.user_id = f.user_id
  where f.activity_type = '1_tx_35'
    and f.activity_start <= sd.first_txn + interval '8 weeks'
  group by 1,2,3
)
, ingo_users as (
  select
    user_id,
    min(transaction_created) as completed_tstamp
  from tb_transactions 
  where 1=1
    and card_acceptor_mid LIKE 'INGO%'
    and state != 'DECLINED'
  group by 1
)
select distinct
  a.user_id,
  case when a.activity_end >= a.eight_weeks then 'unbroken_mau'
    when a.reactivation <= a.eight_weeks then 'reactivated'
    when a.activity_end <= a.eight_weeks then 'lapsed'
    end as mau_group,
  a.reactivation,
  a.first_txn,
  a.activity_end,
  a.eight_weeks,
  case when i.user_id is not null then i.completed_tstamp else null end as ingo_user
from activity a
left join ingo_users i on i.user_id = a.user_id
"),'redshift-us')

ingo_mau <- as.data.table(ingo_mau)

ingo_mau[user_id == '0811198e-7ecd-4b64-90be-97e4dc0b1157', ]

save(ingo_mau,
     file = file.path(kDataPath,"ingo_mau_data.RData"))
