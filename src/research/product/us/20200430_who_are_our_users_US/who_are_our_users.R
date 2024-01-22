library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/who_are_our_users_20200430_US","data")

# external data files can be found here: https://drive.google.com/drive/u/0/folders/194AKqdLVvmX18bNIqFXd-TFTwiEh91N8

# All MAU Ever group

all_mau_demo <- queryDB(" 
with signup_platform as (
  select distinct 
    c.user_id,
    w.os_family
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type 
  where c.collected_tstamp::date >= '2019-07-11'
    and t.se_action = 'signup.account_created'
)
select distinct
  u.id as user_id,
  u.user_created,
  o.occupation,
  a.zip_code,
  a.city,
  a.state,
  u.birth_date,
  sp.os_family
from dbt.zrh_user_activity_txn zuat 
join public.cmd_users u on zuat.user_created = u.user_created
join etl_reporting.cmd_address a on a.user_id = u.id 
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
-- get signup device
left join cmd_shadow_user s on s.user_id = u.id
left join signup_platform sp on sp.user_id = s.id
where zuat.activity_type = '1_tx_35'
  and a.type = 'FIRST_SHIPPING'
  and zuat.activity_start <= '2020-04-30'
",'postgres-us')

# get age from date of birth
all_mau_demo$age <- floor(as.numeric(difftime(Sys.Date(),all_mau_demo$birth_date, units = "weeks"))/52.25)
# group operating system by general platform
unique(all_mau_demo$os_family)
all_mau_demo$signup_device <- ifelse(all_mau_demo$os_family == 'ios', 'ios', 
                                       ifelse(all_mau_demo$os_family == 'android', 'android', 
                                              ifelse(all_mau_demo$os_family %in% c('mac os x', 'windows 7', 'windows', 'linux', 'ubuntu', 'chrome os', 'windows 8'), 'web', 'NULL')))

# checking how many users we have info for
as.data.table(all_mau_demo)[, .(users = .N), by=.(signup_device)]
# sanity check
as.data.table(all_mau_demo)[, .(entries = .N), by=.(user_id)][which(entries > 1),]

all_mau_funding <- queryDB("
select distinct
  t.user_id,
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and t.completed_timestamp between '2019-07-11' and '2020-04-30'
order by t.user_id, t.completed_tstamp
",'postgres-us')

all_mau_spending <- queryDB("
select distinct
t.user_id,
t.completed_tstamp,
date_trunc('month', t.completed_tstamp)::date as month,
t.type,
t.txn_id,
zt.card_type,
case when zt.original_currency != 'USD' then 'international' else 'domestic' end as location,
zt.original_currency,
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
  and t.completed_timestamp between '2019-07-11' and '2020-04-30'
", 'postgres-us')


# MAU 90 Day group

mau_90_demo <- queryDB(" 
with signup_platform as (
  select distinct 
    c.user_id,
    w.os_family
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type 
  where c.derived_tstamp::date >= '2019-07-11'
    and t.se_action = 'signup.account_created'
)
, start_dates as (
select 
  zuat.user_created,
  u.id as user_id,
  u.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
group by 1,2, 3
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account cra on cra.id = cua.account_id and cra.status != 'CLOSED' and cra.status != 'SEIZED'
-- join signup_device
left join cmd_shadow_user s on s.user_id = a.user_id
left join signup_platform sp on sp.user_id = s.id
where a.first_txn::date between '2019-07-11' and '2020-01-29'
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
with start_dates as (
select 
  zuat.user_created,
  u.id as user_id,
  u.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
group by 1,2, 3
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
where a.first_txn::date between '2019-07-11' and '2020-01-29'
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
join public.cr_user_account cua on cua.user_id = u.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
order by t.user_id, t.completed_tstamp
",'postgres-us')

mau_90_spending <- queryDB("
with start_dates as (
select 
  zuat.user_created,
  u.id as user_id,
  u.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
group by 1,2, 3
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
where a.first_txn::date between '2019-07-11' and '2020-01-29'
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = u.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
", 'postgres-us')


# Current MAU who used ACH or Apple Pay

current_mau_ach_apple <- queryDB(" 
with signup_platform as (
  select distinct 
    c.user_id,
    w.os_family
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type 
  where c.derived_tstamp::date >= '2019-07-11'
    and t.se_action = 'signup.account_created'
)
select distinct
  u.id as user_id,
  u.user_created,
  o.occupation,
  a.city,
  a.state,
  a.zip_code,
  u.birth_date,
  case when ac.item_id is not null then true else false end as ach_user,
  case when tvw.card_id is not null then true else false end as apple_pay_user,
  sp.os_family
from dbt.zrh_user_activity_txn zuat 
join public.cmd_users u on zuat.user_created = u.user_created
join etl_reporting.cmd_address a on a.user_id = u.id 
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
-- ACH (linked account)
left join io_items it on it.user_id = u.id 
left join io_accounts ac on ac.item_id = it.item_id
-- Apple Pay (provisioned token)
left join th_cards tc on tc.user_id = u.id 
left join th_virtual_wallet tvw on tvw.card_id = tc.id and tvw.device is not null
-- get signup device
left join cmd_shadow_user s on s.user_id = u.id
left join signup_platform sp on sp.user_id = s.id
where zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
  and a.type = 'FIRST_SHIPPING'
",'postgres-us')


# get age from date of birth
current_mau_ach_apple$age <- floor(as.numeric(difftime(Sys.Date(),current_mau_ach_apple$birth_date, units = "weeks"))/52.25)
# group operating system by general platform
# unique(all_mau_demo$os_family)
current_mau_ach_apple$signup_device <- ifelse(current_mau_ach_apple$os_family == 'ios', 'ios', 
                                       ifelse(current_mau_ach_apple$os_family == 'android', 'android', 
                                              ifelse(current_mau_ach_apple$os_family %in% c('mac os x', 'windows 7', 'windows', 'linux', 'ubuntu', 'chrome os', 'windows 8'), 'web', 'NULL')))

current_mau_ach_apple_funding <- queryDB("
select distinct
  t.user_id,
  case when ac.item_id is not null then true else false end as ach_user,
  case when tvw.card_id is not null then true else false end as apple_pay_user,
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
join dbt.zrh_user_activity_txn zuat on zuat.user_created = t.user_created
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
-- ACH (linked account)
left join io_items it on it.user_id = t.user_id
left join io_accounts ac on ac.item_id = it.item_id
-- Apple Pay (provisioned token)
left join th_cards tc on tc.user_id = t.user_id
left join th_virtual_wallet tvw on tvw.card_id = tc.id and tvw.device is not null
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
order by t.user_id, t.completed_tstamp
",'postgres-us')

current_mau_ach_apple_spending <- queryDB("
select distinct
t.user_id,
case when ac.item_id is not null then true else false end as ach_user,
case when tvw.card_id is not null then true else false end as apple_pay_user,
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
join dbt.zrh_user_activity_txn zuat on zuat.user_created = t.user_created
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
-- ACH (linked account)
left join io_items it on it.user_id = t.user_id
left join io_accounts ac on ac.item_id = it.item_id
-- Apple Pay (provisioned token)
left join th_cards tc on tc.user_id = t.user_id
left join th_virtual_wallet tvw on tvw.card_id = tc.id and tvw.device is not null
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
  and zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
", 'postgres-us')

# limit to the users who engaged with ACH and Apple Pay
current_mau_ach_apple_demo <- as.data.table(current_mau_ach_apple)
current_mau_ach_apple_funding <- as.data.table(current_mau_ach_apple_funding)
current_mau_ach_apple_spending <- as.data.table(current_mau_ach_apple_spending)
current_mau_ach_apple_demo <- current_mau_ach_apple_demo[which(ach_user==TRUE | apple_pay_user==TRUE),]
current_mau_ach_apple_funding <- current_mau_ach_apple_funding[which(ach_user==TRUE | apple_pay_user==TRUE),]
current_mau_ach_apple_spending <- current_mau_ach_apple_spending[which(ach_user==TRUE | apple_pay_user==TRUE),]

current_mau_ach_apple_demo$product <- ifelse(current_mau_ach_apple_demo$ach_user & current_mau_ach_apple_demo$apple_pay_user, 'both', 
                                       ifelse(current_mau_ach_apple_demo$ach_user, 'ach', 
                                              ifelse(current_mau_ach_apple_demo$apple_pay_user, 'apple', 'NULL')))
current_mau_ach_apple_funding$product <- ifelse(current_mau_ach_apple_funding$ach_user & current_mau_ach_apple_funding$apple_pay_user, 'both', 
                                       ifelse(current_mau_ach_apple_funding$ach_user, 'ach', 
                                              ifelse(current_mau_ach_apple_funding$apple_pay_user, 'apple', 'NULL')))
current_mau_ach_apple_spending$product <- ifelse(current_mau_ach_apple_spending$ach_user & current_mau_ach_apple_spending$apple_pay_user, 'both', 
                                       ifelse(current_mau_ach_apple_spending$ach_user, 'ach', 
                                              ifelse(current_mau_ach_apple_spending$apple_pay_user, 'apple', 'NULL')))


reengagement_ach_apple <- queryDB("
with reactivated as (
  select distinct
  t.user_id,
  case when au.external_id is not null then 'apple'
    when t.type = 'SP_ACH' then 'ach' else null end as reactivation_txn,
  t.is_first_time_mau,
  zuat.activity_start,
  zuat.period_id,
  t.completed_tstamp,
  t.bank_balance_impact_cents 
  from dbt.zrh_transactions t 
  join dbt.zrh_user_activity_txn zuat on zuat.user_created = t.user_created 
  left join tb_transactions tb on tb.id = t.txn_id and tb.token_requestor_name ='APPLE_PAY' 
  left join au_transactions au on tb.id = au.external_id::UUID AND au.provider_id = 9
  where zuat.activity_start = t.completed_tstamp 
    and zuat.period_id > 1
    and zuat.activity_type = '1_tx_35'
    and '2020-04-30' between zuat.activity_start and zuat.activity_end
)
select *
from reactivated
where reactivation_txn is not null
", 'postgres-us')


# Current MAU who received IRS refunds or stimulus checks

current_mau_irs <- queryDB(" 
with signup_platform as (
  select distinct 
    c.user_id,
    w.os_family
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type
  where c.derived_tstamp::date >= '2019-07-11'
    and t.se_action = 'signup.account_created'
    
)
, users as (
  select distinct
    user_id
  from dbt.zrh_transactions t
  where t.company like '%IRS %' 
    and t.description like '%TAX REF%'
    and t.direction='Incoming'
    and t.completed_tstamp::date >= '2020-01-01'
)
select distinct
  u.id as user_id,
  u.user_created,
  o.occupation,
  a.city,
  a.state,
  a.zip_code,
  u.birth_date,
  sp.os_family
from dbt.zrh_user_activity_txn zuat 
join public.cmd_users u on zuat.user_created = u.user_created
join users on users.user_id = u.id
join etl_reporting.cmd_address a on a.user_id = u.id and a.type = 'FIRST_SHIPPING'
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
-- get signup device
left join cmd_shadow_user s on s.user_id = u.id
left join signup_platform sp on sp.user_id = s.id
where zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
",'postgres-us')

current_mau_irs_demo <- current_mau_irs
# get age from date of birth
current_mau_irs_demo$age <- floor(as.numeric(difftime(Sys.Date(), current_mau_irs_demo$birth_date, units = "weeks"))/52.25)

current_mau_irs_demo$signup_device <- ifelse(current_mau_irs_demo$os_family == 'ios', 'ios', 
                                       ifelse(current_mau_irs_demo$os_family == 'android', 'android', 
                                              ifelse(current_mau_irs_demo$os_family %in% c('mac os x', 'windows 7', 'windows', 'linux', 'ubuntu', 'chrome os', 'windows 8'), 'web', 'NULL')))

current_mau_irs_funding <- queryDB("
with users as (
  select distinct
    user_id
  from dbt.zrh_transactions t
  where t.company ilike '%IRS %' 
    and t.description ilike '%TAX REF%'
    and t.direction='Incoming'
)
select distinct
  t.user_id,
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
join users on users.user_id = t.user_id
join dbt.zrh_user_activity_txn zuat on zuat.user_created = t.user_created
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED' 
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
order by t.user_id, t.completed_tstamp
",'postgres-us')


current_mau_irs_spending <- queryDB("
with users as (
  select distinct
    user_id
  from dbt.zrh_transactions t
  where t.company ilike '%IRS %' 
    and t.description ilike '%TAX REF%'
    and t.direction='Incoming'
)
select distinct
t.user_id,
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
join users on users.user_id = t.user_id
join etl_reporting.zr_transaction zt on zt.id = t.txn_id
join dbt.zrh_user_activity_txn zuat on zuat.user_created = t.user_created
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
  and zuat.activity_type = '1_tx_35'
  and '2020-04-30' between zuat.activity_start and zuat.activity_end
", 'postgres-us')



# look specifically at users from the James Crease survey
survey_users <- read.csv('~/src/boston/research/product/deep_dive/who_are_our_users_20200430_US/data/Segmentation Participants IDs - Survey Data.csv', stringsAsFactors = FALSE, header = TRUE)
survey_users <- as.data.table(survey_users)
# limit to relevant columns and to only US users
survey_users <- survey_users[,c("user_id","H_COUNTRY","DVCLUS5","C6r1")][H_COUNTRY==5 & user_id != '#N/A',]
survey_user_list <- paste(shQuote(survey_users$user_id), collapse=", ")

survey_users_demo <- queryDB(paste0(" 
with start_dates as (
select 
  zuat.user_created,
  u.id as user_id,
  u.birth_date,
  min(zuat.activity_start) as first_txn
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
group by 1,2, 3
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
select distinct
  u.id as user_id,
  u.user_created,
  o.occupation,
  a.city,
  a.state,
  a.zip_code,
  act.first_txn,
  case when act.activity_end - act.first_txn >= interval '90 days' and act.periods = 1 then 'unbroken_mau'
    when act.activity_end - act.first_txn >= interval '90 days' and act.periods > 1 then 'mau_lapsed_mau'
    when act.activity_end - act.first_txn <= interval '90 days' and act.periods > 1 then 'mau_lapsed_mau'
    when act.activity_end - act.first_txn <= interval '35 days' and act.periods = 1 then 'mau_lapsed_lapsed_short'
    when act.activity_end - act.first_txn < interval '90 days' and act.periods = 1 then 'mau_lapsed_lapsed_long'
    end as mau_group,
  u.birth_date
from activity act
join public.cmd_users u on act.user_id = u.id
join etl_reporting.cmd_address a on a.user_id = u.id 
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where a.type = 'FIRST_SHIPPING'
  and u.id in (", survey_user_list ,")
  and zuar.activity_start <= '2020-04-30'
"),'postgres-us')

# get age from date of birth
survey_users_demo$age <- floor(as.numeric(difftime(Sys.Date(),survey_users_demo$birth_date, units = "weeks"))/52.25)

survey_users_activity <- queryDB(paste0(" 
select distinct
  zuat.user_created,
  u.id as user_id,
  zuat.period_id,
  zuat.activity_start,
  zuat.activity_end
from dbt.zrh_user_activity_txn zuat
join public.cmd_users u on zuat.user_created = u.user_created
where zuat.activity_type = '1_tx_35'
  and u.id in (", survey_user_list ,")
  and zuat.activity_start <= '2020-04-30'
"),'postgres-us')


survey_users_funding <- queryDB(paste0("
select distinct
  t.user_id,
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and t.user_id in (", survey_user_list ,")
  and t.completed_tstamp between '2019-07-11' and '2020-04-30'
order by t.user_id, t.completed_tstamp
"),'postgres-us')

survey_users_spending <- queryDB(paste0("
select distinct
t.user_id,
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
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.direction = 'Outgoing'
  and t.is_micro_deposit is false 
  and t.type not in ('Spaces','N26Fee')
  and t.user_id in (", survey_user_list ,")
  and t.completed_tstamp between '2019-07-11' and '2020-04-30'
"), 'postgres-us')


# rename some survey columns

survey_users$cluster <- ifelse(survey_users$DVCLUS5==1, 'Confident Explorer',
                               ifelse(survey_users$DVCLUS5==2,'Free and unengaged',
                                      ifelse(survey_users$DVCLUS5==3,'Secure and steady',
                                             ifelse(survey_users$DVCLUS5==4,'Overspending worrier',
                                                    ifelse(survey_users$DVCLUS5==5,'Cautious support seeker','NONE')))))

survey_users <- survey_users%>% 
  rename(
    reported_income = C6r1
    )
# add in survery group info from the CSV

survey_users_demo <- merge(survey_users_demo, survey_users[,c("user_id","reported_income","cluster")], all.x = TRUE, by="user_id")
survey_users_activity <- merge(survey_users_activity, survey_users[,c("user_id","cluster")], all.x = TRUE, by="user_id")
survey_users_funding <- merge(survey_users_funding, survey_users[,c("user_id","cluster")], all.x = TRUE, by="user_id")
survey_users_spending <- merge(survey_users_spending, survey_users[,c("user_id","cluster")], all.x = TRUE, by="user_id")

# convert the rest to data.table format for easy aggregation later
all_mau_demo <- as.data.table(all_mau_demo)
all_mau_funding <- as.data.table(all_mau_funding)
all_mau_spending <- as.data.table(all_mau_spending)
mau_90_demo <- as.data.table(mau_90_demo)
mau_90_funding <- as.data.table(mau_90_funding)
mau_90_spending <- as.data.table(mau_90_spending)
reengagement_ach_apple <- as.data.table(reengagement_ach_apple)
current_mau_irs_demo <- as.data.table(current_mau_irs_demo)
current_mau_irs_funding <- as.data.table(current_mau_irs_funding)
current_mau_irs_spending <- as.data.table(current_mau_irs_spending)
survey_users_demo <- as.data.table(survey_users_demo)
survey_users_activity <- as.data.table(survey_users_activity)
survey_users_funding <- as.data.table(survey_users_funding)
survey_users_spending <- as.data.table(survey_users_spending)

# save data locally
save(all_mau_demo,
     all_mau_funding,
     all_mau_spending,
     file = file.path(kDataPath,"all_mau.RData"))

save(mau_90_demo,
     mau_90_funding,
     mau_90_spending,
     file = file.path(kDataPath,"mau_90.RData"))

save(current_mau_ach_apple_demo,
     current_mau_ach_apple_funding,
     current_mau_ach_apple_spending,
     reengagement_ach_apple,
     file = file.path(kDataPath,"current_mau_ach_apple.RData"))

save(current_mau_irs_demo,
     current_mau_irs_funding,
     current_mau_irs_spending,
     file = file.path(kDataPath,"current_mau_irs.RData"))

save(survey_users_demo,
     survey_users_activity,
     survey_users_funding,
     survey_users_spending,
     file = file.path(kDataPath,"survey_users.RData"))
