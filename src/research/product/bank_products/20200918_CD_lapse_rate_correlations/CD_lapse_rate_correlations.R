library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/CD_lapse_rate_correlations_20200918","data")

mau_raisin <- queryDB(" 
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
    sd.first_txn + interval '8 weeks' as eight_weeks,
    count(distinct zuat.period_id) as periods,
    min(zuat.activity_end) as activity_end
  from dbt.zrh_user_activity_txn zuat
  join start_dates sd on sd.user_created = zuat.user_created
  where zuat.activity_type = '1_tx_35'
    and zuat.activity_start <= sd.first_txn + interval '8 weeks'
  group by 1,2,3,4
)
, raisin_users as (
  select 
    ftp.user_created,
    min(ftp.created) as created
  from st_fixed_term_plan ftp
  group by 1
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
  a.eight_weeks,
  case when r.user_created is not null then r.created else null end as raisin_user
from activity a
left join raisin_users r on r.user_created = a.user_created
",'redshift-eu')

mau_raisin <- as.data.table(mau_raisin)

save(mau_raisin,
     file = file.path(kDataPath,"raisin_data.RData"))
