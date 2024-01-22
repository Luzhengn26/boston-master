library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/us/dosh_cashback_impact_20210414","data")

retention <- queryDB(paste0("
with start_dates as (
  select 
    u.user_created,
    u.id as user_id,
    min(zuat.activity_start) as first_txn
  from dbt.f_user_activity_txn zuat
  join etl_reporting.cmd_users u on zuat.user_id = u.id
  where zuat.activity_type = '1_tx_35'
    -- this makes sure we only look at users who have at least 8 weeks with us
    and u.user_created between '2021-02-15' and current_date - interval '8 weeks'
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
  from dbt.f_user_activity_txn zuat
  join start_dates sd on sd.user_id = zuat.user_id
  where zuat.activity_type = '1_tx_35'
    and zuat.activity_start <= sd.first_txn + interval '8 weeks'
  group by 1,2,3,4
)
, fraud_labels as (
  select distinct 
    lel.entity_id as user_id
  from etl_reporting.lin_entity_label lel 
  join etl_reporting.lin_label ll on ll.id = lel.label_id
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
, cashback_users as (
  -- users with cashback-eligible transactions
  select 
    tt.user_id,
    min(tt.created) as first_cashback_txn
  from dbt.f_transactions zt 
  join etl_reporting.tb_transactions tt on tt.id = zt.txn_id 
  join etl_reporting.tbm_dosh_transactions te on te.link_id = tt.link_id 
  where exists (select 1 from etl_reporting.tbm_dosh_cashback_events tdce where tdce.dosh_transaction_id = te.id and tdce.status = 'REWARD_PENDING')
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
  case when c.user_id is not null then c.first_cashback_txn else null end as cashback_user
from activity a
left join cashback_users c on c.user_id = a.user_id
left join fraud_labels fl on fl.user_id = a.user_id
-- exclude fraudsters
where fl.user_id is null
"), "redshift-us")

activity <- queryDB(paste0("
with start_dates as (
  select distinct
    u.user_created,
    u.user_id
  from dbt.dim_users u
  where 1=1
    and u.user_created between '2021-02-01' and '2021-02-28'
    and u.kyc_first_completed is not null
)
, activity as (
  select 
    sd.user_id,
    sd.user_created,
    min(zuat.activity_end) as activity_end
  from start_dates sd
  left join dbt.f_user_activity_txn zuat on zuat.user_id = sd.user_id and zuat.activity_type = '1_tx_35' and zuat.activity_start <= sd.user_created + interval '14 days'
  group by 1,2
)
, fraud_labels as (
  select distinct 
    lel.entity_id as user_id
  from etl_reporting.lin_entity_label lel 
  join etl_reporting.lin_label ll on ll.id = lel.label_id
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
, cashback_users as (
  -- users with cashback-eligible transactions
  select 
    tt.user_id,
    min(tt.created) as first_cashback_txn
  from dbt.f_transactions zt 
  join etl_reporting.tb_transactions tt on tt.id = zt.txn_id 
  join etl_reporting.tbm_dosh_transactions te on te.link_id = tt.link_id 
  where exists (select 1 from etl_reporting.tbm_dosh_cashback_events tdce where tdce.dosh_transaction_id = te.id and tdce.status = 'REWARD_PENDING')
  group by 1
)
select distinct
  a.user_id,
  a.user_created,
  a.activity_end,
  case when a.activity_end is not null then 1 else 0 end as mau,
  case when c.user_id is not null then c.first_cashback_txn else null end as cashback_user,
  case when a.user_created < '2021-02-15' then 'pre' else 'post' end as group
from activity a
left join cashback_users c on c.user_id = a.user_id
left join fraud_labels fl on fl.user_id = a.user_id
-- exclude fraudsters
where fl.user_id is null
"), "redshift-us")

# do they spend more money than non-cashback users?
transactions <- queryDB(paste0("
with start_dates as (
  select distinct
    u.user_created,
    u.user_id
  from dbt.dim_users u
  where 1=1
    and u.user_created between '2021-02-15' and current_date - interval '14 days'
    and u.kyc_first_completed is not null
)
, activity as (
  select 
    sd.user_id,
    sd.user_created,
    min(zuat.activity_start) as activity_start
  from start_dates sd
  join dbt.f_user_activity_txn zuat on zuat.user_id = sd.user_id and zuat.activity_type = '1_tx_35' and zuat.activity_start <= sd.user_created + interval '14 days'
  group by 1,2
)
, fraud_labels as (
  select distinct 
    lel.entity_id as user_id
  from etl_reporting.lin_entity_label lel 
  join etl_reporting.lin_label ll on ll.id = lel.label_id
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
, cashback_users as (
  -- users with cashback-eligible transactions
  select 
    tt.user_id,
    min(tt.created) as first_cashback_txn
  from dbt.f_transactions zt 
  join etl_reporting.tb_transactions tt on tt.id = zt.txn_id 
  join etl_reporting.tbm_dosh_transactions te on te.link_id = tt.link_id 
  where exists (select 1 from etl_reporting.tbm_dosh_cashback_events tdce where tdce.dosh_transaction_id = te.id and tdce.status = 'REWARD_PENDING')
  group by 1
)
select
  a.user_id,
  a.user_created,
  a.activity_start,
  t.txn_id,
  t.bank_balance_impact as amount,
  case when c.user_id is not null then c.first_cashback_txn else null end as cashback_user
from activity a
join dbt.f_transactions t on t.user_id = a.user_id and t.completed_tstamp <= a.activity_start + interval '14 days' and t.direction = 'Outgoing' and t.type = 'Card'
left join cashback_users c on c.user_id = a.user_id
left join fraud_labels fl on fl.user_id = a.user_id
-- exclude fraudsters
where fl.user_id is null
"), "redshift-us")

# set as datatable so we can easily aggregate
retention <- as.data.table(retention)
activity <- as.data.table(activity)
transactions <- as.data.table(transactions)

save(retention,
     activity,
     transactions,
     file = file.path(kDataPath,"cashback_data.RData"))
