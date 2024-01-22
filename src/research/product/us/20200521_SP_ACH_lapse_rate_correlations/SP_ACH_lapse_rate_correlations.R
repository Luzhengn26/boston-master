library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/SP_ACH_lapse_rate_correlations","data")

mau_ach <- queryDB(" 
with start_dates as (
select 
  zuat.user_created,
  u.id as user_id,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
  -- this makes sure we only look at users who have at least 8 weeks with us
  and u.user_created between '2020-03-01' and current_date - interval '8 weeks'
group by 1,2
)
, activity as (
select 
  sd.user_id,
  sd.user_created,
  sd.first_txn,
  count(distinct zuat.period_id) as periods,
  min(zuat.activity_end) as activity_end
from dbt.zrh_user_activity_txn zuat
join start_dates sd on sd.user_created = zuat.user_created
where zuat.activity_type = '1_tx_35'
group by 1,2,3
)
select distinct
  a.user_id,
  a.user_created,
  case when a.activity_end - a.first_txn >= interval '8 weeks' and a.periods = 1 then 'unbroken_mau'
    when a.activity_end - a.first_txn >= interval '8 weeks' and a.periods > 1 then 'reactivated'
    when a.activity_end - a.first_txn <= interval '8 weeks' and a.periods > 1 then 'reactivated'
    when a.activity_end - a.first_txn <= interval '8 weeks' and a.periods = 1 then 'full_lapse'
    end as mau_group,
  a.periods,
  a.first_txn,
  a.activity_end,
  a.first_txn + interval '8 weeks' as eight_weeks,
  case when t.user_id is not null then t.completed_tstamp else null end as ach_user
from activity a
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account cra on cra.id = cua.account_id and cra.status != 'CLOSED' and cra.status != 'SEIZED'
left join dbt.zrh_transactions t on t.user_id = a.user_id and t.type='SP_ACH'
",'postgres-us')

mau_ach <- as.data.table(mau_ach)

save(mau_ach,
     file = file.path(kDataPath,"sp_ach_data.RData"))
