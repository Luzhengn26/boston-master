library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/Spaces_lapse_rate_correlations_20200917","data")

mau_spaces <- queryDB(" 
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
, fraud_labels as (
  select distinct 
    lel.entity_id as user_id
  from public.lin_entity_label lel 
  join public.lin_label ll on ll.id = lel.label_id
  where ll.name in ('AML - General',
    'AML - Terrorist Financing',
    'Fraud',
    'Fraud - ACH',
    'Fraud - Card - Chargeback',
    'Fraud - Card - Stripe Top Up',
    'Fraud - KYC - ID Theft',
    'Fraud - OCT - CashApp',
    'Fraud - Presentment Refund')
)
, spaces_users as (
  select  
    cua.user_id,
    min(ca.opened_at) as opened_at
  from cr_account ca 
  join cr_user_account cua on cua.account_id = ca.id
  where ca.closed_at is null 
    and ca.account_role <> 'PRIMARY'
    and ca.opened_at is not null
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
  a.first_txn + interval '8 weeks' as eight_weeks,
  case when su.user_id is not null then su.opened_at else null end as spaces_user
from activity a
left join fraud_labels fl on fl.user_id = a.user_id
left join spaces_users su on su.user_id = a.user_id
-- exclude fraudsters
where fl.user_id is null
",'postgres-us')

mau_spaces <- as.data.table(mau_spaces)

save(mau_spaces,
     file = file.path(kDataPath,"spaces_data.RData"))
