# Who are our users - refresh with only Direct Deposit users

library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/who_are_our_users_20200430_US","data")

# external data files can be found here: https://drive.google.com/drive/u/0/folders/194AKqdLVvmX18bNIqFXd-TFTwiEh91N8


# MAU 90 Day group

mau_90_demo <- queryDB(" 
with dir_dep_users as (
  select distinct
    t.user_id,
    s.id as shadow_user_id
  from dbt.zrh_transactions t
  left join cmd_shadow_user s on s.user_id = t.user_id
  where t.type = 'DIR_DEP'
)
, signup_platform as (
  select distinct 
    c.user_id,
    w.os_family
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  -- join public.ksp_event_types t on t.event_type = c.event_type 
  where c.collector_tstamp::date >= current_date - interval '180 days'
    -- for this event: signup.account_created
    and c.event_type in (87, 93, 209)
    and c.user_id in (select shadow_user_id from dir_dep_users)
)
, start_dates as (
select 
  zuat.user_created,
  cu.id as user_id,
  u.shadow_user_id,
  cu.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users cu on cu.user_created = zuat.user_created
join dir_dep_users u on u.user_id = cu.id
where zuat.activity_type = '1_tx_35'
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
, activity as (
select 
  sd.user_id,
  sd.shadow_user_id,
  sd.user_created,
  sd.birth_date,
  sd.first_txn,
  count(distinct zuat.period_id) as periods,
  max(zuat.activity_end) as activity_end
from dbt.zrh_user_activity_txn zuat
join start_dates sd on sd.user_created = zuat.user_created and zuat.activity_start - sd.first_txn <= interval '90 days'
-- exclude fraudsters
left join fraud_labels fl on fl.user_id = sd.user_id
where zuat.activity_type = '1_tx_35'
  and fl.user_id is null
group by 1,2,3,4,5
)
select distinct
  a.user_id,
  a.user_created,
  a.birth_date,
  o.occupation,
  ca.city,
  ca.state,
  ca.zip_code,
  sp.os_family,
  case when a.activity_end - a.first_txn >= interval '90 days' and a.periods = 1 then 'unbroken_mau'
    when a.activity_end - a.first_txn >= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '35 days' and a.periods = 1 then 'mau_lapsed_lapsed_short'
    when a.activity_end - a.first_txn < interval '90 days' and a.periods = 1 then 'mau_lapsed_lapsed_long'
    end as mau_group,
  a.periods,
  a.first_txn,
  a.activity_end,
  a.first_txn + interval '90 days' as ninety_day_period_end
from activity a
join etl_reporting.cmd_address ca on ca.user_id = a.user_id and ca.type = 'FIRST_SHIPPING'
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- join signup_device
left join signup_platform sp on sp.user_id = a.shadow_user_id
where a.first_txn::date between current_date - interval '180 days' and current_date - interval '90 days'
-- add logic afterwards that says 
-- case when periods = 2 and ninety_day_period_end < activity_end then 2 
--      when periods = 3 then 2 
--      when mau_group = 'unbroken_mau' then 0
--      when mau_group = 'mau_lapsed_lapsed' then 1
--    end as times_lapsed
",'postgres-us')

 # get age from date of birth
mau_90_demo$age <- floor(as.numeric(difftime(Sys.Date(),mau_90_demo$birth_date, units = "weeks"))/52.25)
# group operating system by general platform
# unique(mau_90_demo$os_family)
mau_90_demo$signup_device <- ifelse(mau_90_demo$os_family == 'ios', 'ios', 
                                       ifelse(mau_90_demo$os_family == 'android', 'android', 
                                              ifelse(mau_90_demo$os_family %in% c('mac os x', 'windows 7', 'windows', 'linux', 'ubuntu', 'chrome os', 'windows 8'), 'web', 'NULL')))
# unique(mau_90_demo$signup_device)


mau_90_funding <- queryDB("
with dir_dep_users as (
  select distinct
    t.user_id
  from dbt.zrh_transactions t
  where t.type = 'DIR_DEP'
)
, start_dates as (
select 
  zuat.user_created,
  cu.id as user_id,
  cu.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users cu on cu.user_created = zuat.user_created
join dir_dep_users u on u.user_id = cu.id
where zuat.activity_type = '1_tx_35'
group by 1,2,3
)
, activity as (
select 
  sd.user_id,
  sd.user_created,
  sd.birth_date,
  sd.first_txn,
  count(distinct zuat.period_id) as periods,
  max(zuat.activity_end) as activity_end
from dbt.zrh_user_activity_txn zuat
join start_dates sd on sd.user_created = zuat.user_created and zuat.activity_start - sd.first_txn <= interval '90 days'
where zuat.activity_type = '1_tx_35'
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
, users as (
select distinct
  a.user_id,
  case when a.activity_end - a.first_txn >= interval '90 days' and a.periods = 1 then 'unbroken_mau'
    when a.activity_end - a.first_txn >= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '35 days' and a.periods = 1 then 'mau_lapsed_lapsed_short'
    when a.activity_end - a.first_txn < interval '90 days' and a.periods = 1 then 'mau_lapsed_lapsed_long'
    end as mau_group,
  a.periods,
  a.first_txn,
  a.activity_end,
  a.first_txn + interval '90 days' as ninety_day_period_end
from activity a
join etl_reporting.cmd_address ca on ca.user_id = a.user_id and ca.type = 'FIRST_SHIPPING'
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
left join fraud_labels fl on fl.user_id = a.user_id
where a.first_txn::date between current_date - interval '180 days' and current_date - interval '90 days'
  and fl.user_id is null
)
select distinct
  t.user_id,
  u.mau_group,
  t.is_first_time_mau,
  t.direction,
  t.is_internal_txn,
  t.type,
  t.txn_id,
  t.company,
  t.completed_tstamp,
  date_trunc('month', t.completed_tstamp)::date as month,
  t.bank_balance_impact_cents as amount_cents
from dbt.zrh_transactions t 
join users u on u.user_id = t.user_id and t.completed_tstamp <= u.ninety_day_period_end
-- exclude fraudsters
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
order by t.user_id, t.completed_tstamp
",'postgres-us')

mau_90_spending <- queryDB("
with dir_dep_users as (
  select distinct
    t.user_id
  from dbt.zrh_transactions t
  where t.type = 'DIR_DEP'
)
, start_dates as (
select 
  zuat.user_created,
  cu.id as user_id,
  cu.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users cu on cu.user_created = zuat.user_created
join dir_dep_users u on u.user_id = cu.id
where zuat.activity_type = '1_tx_35'
group by 1,2,3
)
, activity as (
select 
  sd.user_id,
  sd.user_created,
  sd.birth_date,
  sd.first_txn,
  count(distinct zuat.period_id) as periods,
  max(zuat.activity_end) as activity_end
from dbt.zrh_user_activity_txn zuat
join start_dates sd on sd.user_created = zuat.user_created and zuat.activity_start - sd.first_txn <= interval '90 days'
where zuat.activity_type = '1_tx_35'
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
, users as (
select distinct
  a.user_id,
  case when a.activity_end - a.first_txn >= interval '90 days' and a.periods = 1 then 'unbroken_mau'
    when a.activity_end - a.first_txn >= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '90 days' and a.periods > 1 then 'mau_lapsed_mau'
    when a.activity_end - a.first_txn <= interval '35 days' and a.periods = 1 then 'mau_lapsed_lapsed_short'
    when a.activity_end - a.first_txn < interval '90 days' and a.periods = 1 then 'mau_lapsed_lapsed_long'
    end as mau_group,
  a.periods,
  a.first_txn,
  a.activity_end,
  a.first_txn + interval '90 days' as ninety_day_period_end
from activity a
join etl_reporting.cmd_address ca on ca.user_id = a.user_id and ca.type = 'FIRST_SHIPPING'
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
left join fraud_labels fl on fl.user_id = a.user_id
where a.first_txn::date between current_date - interval '180 days' and current_date - interval '90 days'
  and fl.user_id is null
)
select distinct
t.user_id,
u.mau_group,
t.completed_tstamp,
date_trunc('month', t.completed_tstamp)::date as month,
t.type,
t.txn_id,
zt.card_type,
case when zt.original_currency != 'USD' then 'international' else 'domestic' end as location,
case when zt.merchant_name like 'CASH APP%' then 'CASH APP' else zt.merchant_name end as merchant_name,
t.bank_balance_impact_cents as amount_cents,
case when (zt.mcc_group between 3000 and 3299) or (zt.mcc_group = 4511) then 'airline'
  when (zt.mcc_group between 3351 and 3441) or (zt.mcc_group = 7512) then 'car_rental'
  when (zt.mcc_group between 3501 and 3790) or (zt.mcc_group = 7011) then 'hotel_lodging' 
  when (zt.mcc_group = 5411 or zt.mcc_group = 5499) then 'grocery_market'
  when zt.mcc_group = 6011 then 'atm'
  when zt.mcc_group in (5812,5811) then 'restaurants'
  when zt.mcc_group in (5814) then 'fast_food'
  when zt.mcc_group in (4121) then 'taxicabs'
  when zt.mcc_group in (5541,5542) then 'gas_service_station'
  when zt.mcc_group in (5735) then 'record_stores'
  when zt.mcc_group in (4111,4112,4131) then 'local_transport_railway' --transportation-suburban and local commuter passenger, including ferries
  when zt.mcc_group in (5499,5921,5441) then 'food_drink_stores' -- liquer/beer,convenience stores, markets, specialty stores, vending machines
  when zt.mcc_group in (5999,5941,5993,5399,5947,5309,5945,5994,5940) then 'retail_store' -- bikes,game/toy/hobby,miscellaneous specialty retail, sporting good store, cigar
  when zt.mcc_group in (5912) then 'drug_pharma'
  when zt.mcc_group in (5813) then 'bars_clubs'
  when zt.mcc_group in (5942) then 'bookstores'
  when zt.mcc_group in (7011) then 'hotel_lodging'
  when zt.mcc_group in (7995,7800,7801,7802) then 'gambling_gaming'
  when zt.mcc_group in (5734,5732,5945,5045,5946) then 'computer_electronic_stores' --computer, electronic, photography
  when zt.mcc_group in (7399,7392,7299,6513,4215,8641) then 'business_org_services'
  when zt.mcc_group in (5331,5200,5712,5251,5072,5074,5085,5193,5211,5261,5231,5533,5719,5992,5995,763,5722) then 'household_store' -- repair shops, pets, florists,variety store, wide range or household goods
  when zt.mcc_group in (5651,5691,5611,5621,5631,5641,5655,5661,5681,5697,5698,5699,5311) then 'clothing_depart_store'
  when zt.mcc_group in (4784,7523) then 'car_toll_parking' -- bridge and road fees, toll
  when zt.mcc_group in (5462) then 'bakeries'
  when zt.mcc_group in (5968,5964,5969,5967,5965) then 'subscriptions' --Direct Marketing–Continuity/Subscription Merchants
  when zt.mcc_group in (5815,5816,5817,5818) then 'digital_goods' --Digital Goods: Books, Movies, Music
  when zt.mcc_group in (4899, 4900, 4812,4813,4814,4816) then 'utilities' -- cable/satelite/television/electric/heating/water
  when zt.mcc_group in (4789) then 'transport_serv' --Transportation Services Not Elsewhere Classified
  when zt.mcc_group in (7512) then 'car_rental'
  when zt.mcc_group in (8999) then 'prof_serv' -- professional services and membership organizations
  when zt.mcc_group in (7372) then 'computer_data_serv' -- Computer Programming, Data Processing and Integrated System Design Services
  when zt.mcc_group in (4722) then 'travel_tour_agencies' 
  when zt.mcc_group in (5310) then 'discount_stores'
  when zt.mcc_group in (5977,7230) then 'beauty_stores' --cosmetic, barber, beauty shops
  when zt.mcc_group in (7311) then 'advertising_serv'
  when zt.mcc_group in (7832,7922,7829,7833,7841,7922,7929,7932,7933,7941,7991,7992,7993,7994,7996,7997,7998,7999) 
    then 'entertainment' --games/arcades,tourist, sports field,music, dance,movies,theaters,pool,bowling,golf,amusement
  when zt.mcc_group in (9211,9222,9223,9311,9399,9402,9405) then 'fines_taxes_gov' --government services
  when zt.mcc_group in (4829,6051,6050,6012,6540) then 'money_cash_financial' --quasi cash merchant,wire transfer money order, money transfer
  when zt.mcc_group in (7273) then 'dating_serv' 
  when zt.mcc_group between 8011 and 8099 then 'health_serv' --doctors,hospitals, dental, chiropractor,nurse
  when zt.mcc_group between 8211 and 8299 then 'education' -- schools, colleges/uni
  else 'no_cat' end as merchant_category
from dbt.zrh_transactions t  
join etl_reporting.zr_transaction zt on zt.id = t.txn_id
join users u on u.user_id = t.user_id and t.completed_tstamp <= u.ninety_day_period_end
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
", 'postgres-us')


# convert the rest to data.table format for easy aggregation later
mau_90_demo_dir_dep <- as.data.table(mau_90_demo)
mau_90_funding_dir_dep <- as.data.table(mau_90_funding)
mau_90_spending_dir_dep <- as.data.table(mau_90_spending)

save(mau_90_demo_dir_dep,
     mau_90_funding_dir_dep,
     mau_90_spending_dir_dep,
     file = file.path(kDataPath,"mau_90_dir_dep.RData"))
