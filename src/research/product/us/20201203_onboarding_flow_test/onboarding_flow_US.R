library(n26)
library(data.table)

kDataPath <- file.path("/Users/danielmermelstein/src/boston/research/product/split_tests/onboarding_flow_US_20201203","data")

# get users who went through the new flow
# in lieu of ksp tables in the postgres DWH actually working, this is an alternate way to get our test population
onboarding <- queryDB("
with txns as (
select 
  user_id,
  completed_tstamp,
  bank_balance_impact_cents/100::float as txn_amount,
  row_number() over (partition by user_id order by completed_tstamp asc) as num
from dbt.zrh_transactions zt 
where direction = 'Incoming'
  and is_internal_txn is false 
  and is_micro_deposit is false
)
select distinct
  u.user_id,
  case when poi.user_id is not null then 'treatment' else 'control' end as variant,
  coalesce(poi.created, u.kyc_first_completed) as assignment_date,
  u.kyc_first_completed,
  u.card_first_activated,
  case when u.first_time_mau_tstamp - u.kyc_first_completed <= interval '35 days' then u.first_time_mau_tstamp else null end as first_time_mau_tstamp,
  case when zt.completed_tstamp - u.kyc_first_completed <= interval '35 days' then zt.bank_balance_impact_cents/100::float else null end as ft_funding_amount,
  case when t.completed_tstamp - u.kyc_first_completed <= interval '35 days' then t.completed_tstamp else null end as second_deposit,
  case when t.completed_tstamp - u.kyc_first_completed <= interval '35 days' then txn_amount else null end as second_deposit_amount
from dbt.zrh_users u
left join pt_onboarding_info poi on poi.user_id = u.user_id and created > '2020-10-10'
left join dbt.zrh_transactions zt on zt.user_id = u.user_id and zt.is_first_time_funding and zt.type <> 'Spaces' and not zt.is_internal_txn and not zt.is_micro_deposit
left join txns t on t.user_id = u.user_id and t.num = 2
where 1=1
  and u.last_login_app_version in ('n26-android_3.52',
      'n26-android_3.52.1',
      'n26-android_3.53-internal.47055',
      'n26-ios_3.52',
      'n26-ios_3.53',
      'n26-android_3.53.1',
      'n26-android_3.53',
      'n26-ios_3.54',
      'n26-android_3.54')
  and u.kyc_first_completed >= '2020-10-10'
","postgres-us")

onboarding <- as.data.table(onboarding)

save(onboarding,
     file = file.path(kDataPath,"onboarding.RData"))
