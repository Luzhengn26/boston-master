library(n26)
library(data.table)

kDataPath <- file.path("/Users/danielmermelstein/src/boston/research/product/split_tests/stripe_topup_test_20200325","data")

test.start.date <- '2020-03-13'
test.end.date <- '2020-05-05'

first_time_funders_completed <- queryDB(paste0("
with stripe as (
select
  t.id,
  t.stripe_id,
  d.started as disputed,
  c.ci__outcome__risk_score
from public.pt_charge as c
join etl_reporting.pt_top_up t on t.stripe_id = c.payment_intent_id 
left join etl_reporting.pt_dispute as d on c.stripe_id = d.charge_id
where c.status='succeeded'
)
, ft_faus AS (
select
  t.user_created,
  t.user_id,
  t.completed_tstamp as created,
  t.txn_id,
  t.bank_balance_impact_cents/100::float as txn_amount,
  s.id as topup_id,
  s.disputed,
  s.ci__outcome__risk_score,
  case when t.type = 'SP_ACH' then TRUE else FALSE end as sp_ach,
  t.is_first_time_mau as ft_fau
from dbt.zrh_transactions t
left join stripe s on s.id = t.txn_id 
where t.completed_tstamp::date between '",test.start.date,"' and '",test.end.date,"'
  and t.direction = 'Incoming'
  and t.type <> 'Spaces'
  and t.is_internal_txn IS FALSE
  AND t.is_micro_deposit IS FALSE
  and t.is_first_time_mau
order by 2,3
)
, test as (
select *
from dbt.zrh_fraud_experiment fe
JOIN cmd_users cmu ON cmu.id = fe.user_id
-- looks at users who have signed up in the last 4 months
where cmu.user_created::date between '2019-11-01' and '",test.end.date,"'
)
select distinct
  t.user_id,
  t.user_created,
  t.variant,
  ftf.txn_amount,
  case when t.user_created::date >= '",test.start.date,"' then 'new' else 'tenured' end as cohort_age,
  case when ftf.ft_fau then ftf.created else null end as ft_fau,
  case when ftf.sp_ach then ftf.created else null end as sp_ach,
  case when ftf.topup_id is not null then ftf.created else null end as card_funded,
  ftf.disputed,
  ftf.ci__outcome__risk_score as stripe_radar_risk_score
from test t
left join ft_faus ftf on ftf.user_id = t.user_id and ft_fau = true
"), "postgres-us")


chargeback_users <- queryDB(paste0("
select  
case when is_user_in_tg( f.user_id,  'us_digital_wallet_only', 50) = true then 'Both methods' else 'Google/Apple pay only' end as variant,  
f.user_id,
case when pt_charge.id is not null then f.user_id else null end as used,
pt_charge.ci__outcome__risk_score
from dbt.zrh_fraud_experiment f 
join pt_charge on f.user_id = pt_charge.user_id 
where currency = 'USD' 
  and created between '",test.start.date,"' and '",test.end.date,"'
  and status = 'succeeded'
"), 'postgres-us')

save(first_time_funders_completed,
     chargeback_users,
     test.start.date,
     test.end.date,
     file = file.path(kDataPath,"first_time_funders.RData"))
