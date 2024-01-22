library(n26)
library(data.table)

kDataPath <- file.path("/Users/danielmermelstein/src/boston/research/product/deep_dive/stripe_recurring_price_change_20201222","data")

start.date <- '2020-10-23'
end.date <- '2020-12-31'

interval <- 14
cohort <- 7

user_transactions <- queryDB(paste0("
with user_transactions as (
select 
  t.user_id,
  t.type,
  date_trunc('week', t.user_created)::date as cohort_week,
  t.txn_id,
  t.completed_tstamp,
  date_trunc('month', t.completed_tstamp)::date as txn_month,
  t.bank_balance_impact_cents/100::float as txn_amount,
  d.started as disputed,
  c.ci__outcome__risk_score as stripe_radar_risk_score,
  case when t.type = 'TopUp' and t.direction = 'Incoming' then row_number() over (partition by t.user_id, t.type = 'TopUp', t.direction = 'Incoming' order by t.completed_tstamp) else 0 end as num
from dbt.zrh_transactions t
left join etl_reporting.pt_top_up pt on pt.id = t.txn_id
left join public.pt_charge c on c.payment_intent_id = pt.stripe_id
left join etl_reporting.pt_dispute as d on c.stripe_id = d.charge_id
where t.completed_tstamp::date between '",start.date,"'::date - interval '",interval," days' and '",start.date,"'::date + interval '",interval," days'
  and not t.is_internal_txn
  and not t.is_micro_deposit
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
select 
  ut.*,
  case when ut.completed_tstamp < '",start.date,"' then 'pre' else 'post' end as category,
  case when ut.num > 1 then 'recurring' 
    when ut.num = 1 then 'first'
    else 'none' end as topup_type
from user_transactions ut 
left join fraud_labels fl on fl.user_id = ut.user_id
where fl.user_id is null
"), "postgres-us")


# gonna use this for an attempted incrementality analysis
# do a pre/post analysis
users <- queryDB(paste0("
with txns as (
  select 
    t.user_id,
    t.completed_tstamp,
    t.bank_balance_impact_cents/100::float as txn_amount,
    case when t.type = 'TopUp' then row_number() over (partition by t.user_id, t.type='TopUp' order by t.completed_tstamp) else 0 end as num,
    row_number() over (partition by t.user_id order by t.completed_tstamp) as dep_num
  from dbt.zrh_transactions t
  where t.direction = 'Incoming'
    and not t.is_internal_txn
    and not t.is_micro_deposit
)
, stats as (
  select 
    u.user_id,
    u.kyc_first_completed,
    date_trunc('week', u.kyc_first_completed)::date as week_cohort,
    min(case when num = 1 and t.completed_tstamp <= u.kyc_first_completed + interval '",interval," days' then t.completed_tstamp else null end) as first_topup,
    min(case when num > 1 and t.completed_tstamp <= u.kyc_first_completed + interval '",interval," days' then t.completed_tstamp else null end) as first_recurring_topup,
    min(case when t.completed_tstamp <= u.kyc_first_completed + interval '",interval," days' then t.completed_tstamp else null end) as first_txn,
    min(case when dep_num = 2 and t.completed_tstamp <= u.kyc_first_completed + interval '",interval," days' then t.completed_tstamp else null end) as second_txn,
    sum(case when t.completed_tstamp <= u.kyc_first_completed + interval '",interval," days' then t.txn_amount else 0 end) as amount,
    max(dep_num) as deposit_cnt
  from dbt.zrh_users u 
  left join txns t on t.user_id = u.user_id
  where u.kyc_first_completed is not null
  group by 1,2,3
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
select 
  case when s.kyc_first_completed < '",start.date,"' then 'pre' else 'post' end as category,
  s.*
from stats s
left join fraud_labels fl on fl.user_id = s.user_id
where ((s.kyc_first_completed >= '",start.date,"'::date - interval '",interval+cohort," days' and s.kyc_first_completed < '",start.date,"'::date - interval '",interval," days')
    or (s.kyc_first_completed between '",start.date,"' and '",start.date,"'::date + interval '",cohort," days'))
  and s.kyc_first_completed < current_date - interval '",interval," days'
  and fl.user_id is null
"), "postgres-us")

lapse_cohort <- 2
mau_topup <- queryDB(paste0(" 
with start_dates as (
  select 
    zuat.user_created,
    case when zuat.user_created >= '",start.date,"' then 'post' else 'pre' end as category,
    u.id as user_id,
    min(zuat.activity_start) as first_txn
  from dbt.zrh_user_activity_txn zuat
  join public.cmd_users u on zuat.user_created = u.user_created
  where zuat.activity_type = '1_tx_35'
    -- this makes sure we only look at users who have at least 8 weeks with us
    and ((u.user_created >= '",start.date,"'::date - interval '",lapse_cohort + 8," weeks' and u.user_created < '",start.date,"'::date - interval '8 weeks')
      or (u.user_created between '",start.date,"' and '",start.date,"'::date + interval '",lapse_cohort," weeks'))
  group by 1,2,3
)
, activity as (
  select 
    sd.user_id,
    sd.category,
    sd.user_created,
    sd.first_txn,
    sd.first_txn + interval '8 weeks' as eight_weeks,
    count(distinct zuat.period_id) as periods,
    min(zuat.activity_end) as activity_end
  from dbt.zrh_user_activity_txn zuat
  join start_dates sd on sd.user_created = zuat.user_created
  where zuat.activity_type = '1_tx_35'
    and zuat.activity_start <= sd.first_txn + interval '8 weeks'
  group by 1,2,3,4,5
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
, topup_users as (
  select 
    t.user_id,
    min(t.completed_tstamp) as completed_tstamp
  from dbt.zrh_transactions t
  where t.direction = 'Incoming'
    and not t.is_internal_txn
    and not t.is_micro_deposit
    and t.type = 'TopUp'
  group by 1
)
select distinct
  a.user_id,
  a.category,
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
  case when tu.user_id is not null then tu.completed_tstamp else null end as topup_user
from activity a
left join topup_users tu on tu.user_id = a.user_id and tu.completed_tstamp <= a.eight_weeks
left join fraud_labels fl on fl.user_id = a.user_id
-- exclude fraudsters
where fl.user_id is null
  and a.eight_weeks < current_date
"),'postgres-us')


# set as datatable so we can easily aggregate
user_transactions <- as.data.table(user_transactions)
users <- as.data.table(users)
mau_topup <- as.data.table(mau_topup)

save(user_transactions,
     users,
     mau_topup,
     cohort,
     interval,
     lapse_cohort,
     start.date,
     end.date,
     file = file.path(kDataPath,"stripe_data.RData"))
