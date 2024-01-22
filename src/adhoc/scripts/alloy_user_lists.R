###
# Date: 2021-09-22
# Author: Dani Mermelstein
# Objective: We're pulling users lists for different subsets/samples of our userbase to evaluate how Evolve/Alloy's KYC solution would have performed in a backtest
###

library(n26)
library(data.table)

# source data can be found here: https://docs.google.com/spreadsheets/d/19QvIgUDKWToYkcpL_mj22Ok1oppL1td_
# and unbanked users here: https://docs.google.com/spreadsheets/d/1huCrn5Z35qrjY8ZcUzg0NBlXDdeaFNst/edit#gid=1337537620
socure_txns <- read.csv('/Users/danielmermelstein/src/boston/src/adhoc/scripts/alloy_user_lists/socure_test_txns.csv', stringsAsFactors = FALSE)
socure_hrc_users <- read.csv('/Users/danielmermelstein/src/boston/src/adhoc/scripts/alloy_user_lists/socure_hrc_users.csv', stringsAsFactors = FALSE)
socure_txn_list <- paste(shQuote(socure_txns$socure_txn), collapse=", ")
socure_hrc_user_list <- paste(shQuote(socure_hrc_users$user_id), collapse=", ")

socure_users <- queryDB(paste0("
  select distinct
    user_id
  from km_processes
  where provider_reference_id in (",socure_txn_list,")
"), 'redshift-us')
socure_user_list <- paste(shQuote(socure_users$user_id), collapse=", ")

# kycc users (half non-Doc, half DocV)
doc_v <- queryDB(paste0("
-- DocV completions
with ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn
  from km_processes k  
  where k.completed is not null
    and (k.primary_document <> 'SSN_CARD') 
    and k.provider = 'SOCURE' -- non-doc completion
    and k.user_id not in (",socure_user_list,")
)
select
  ok.user_id,
  ok.provider_reference_id,
  'doc_v' as kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and u.main_account_status = 'OPEN'
  and u.first_fraud_flag is null -- non HRC
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
order by random()
limit 100
"), 'redshift-us')

non_doc <- queryDB(paste0("
-- Non-Doc completions
with ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn
  from km_processes k  
  where k.completed is not null
    and (k.primary_document = 'SSN_CARD' or k.primary_document IS null)
    and k.provider = 'SOCURE' -- non-doc completion
    and k.user_id not in (",socure_user_list,")
)
select
  ok.user_id,
  ok.provider_reference_id,
  'non_doc' as kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and u.main_account_status = 'OPEN'
  and u.first_fraud_flag is null -- non HRC
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
order by random()
limit 100
"), 'redshift-us')

kycc <- rbind(non_doc, doc_v)
# do a little formatting
kycc$address_line_2[is.na(kycc$address_line_2)] <- "" # replace NA with blank string
kycc$document_ssn <- sapply(kycc$document_ssn, function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros
kycc$postal_code <- sapply(kycc$postal_code,  function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros

write.csv(kycc, '~/kycc_alloy_sample.csv')
kycc_list <- paste(shQuote(kycc$user_id), collapse=", ")

# KYC rejects (no resubmits, ideally no technical failures) 
kyc_fail <- queryDB(paste0("
with socure_users as (
  select distinct
    user_id
  from km_processes
  where provider_reference_id in (",socure_txn_list,",",kycc_list,")
)
select distinct
  k.provider_reference_id
from km_processes k
left join socure_users s on s.user_id = k.user_id
where k.status = 'FAILED'
  and k.status_reason <> 'LIMIT EXCEEDED'
  and s.user_id is null
order by random()
limit 100
"), 'redshift-us')


# HRC
non_doc_hrc <- queryDB(paste0("
-- Non-Doc completions
with ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn
  from km_processes k  
  where k.completed is not null
    and (k.primary_document = 'SSN_CARD' or k.primary_document IS null)
    and k.provider = 'SOCURE' -- non-doc completion
    and k.user_id not in (",kycc_list,")
    and k.user_id not in (",socure_user_list,")
)
select
  ok.user_id,
  ok.provider_reference_id,
  'non_doc' as kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and u.first_fraud_flag is not null -- exclusive HRC
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
order by random()
limit 175
"), 'redshift-us')

socure_hrc <- queryDB(paste0("
-- Non-Doc completions
with ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn,
    case when (k.primary_document = 'SSN_CARD' or k.primary_document IS null) then 'non_doc' else 'doc_v' end as kyc_type
  from km_processes k  
  where k.completed is not null
    and k.provider = 'SOCURE' -- non-doc completion
    and k.user_id in (",socure_hrc_user_list,")
)
select
  ok.user_id,
  ok.provider_reference_id,
  ok.kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
"), 'redshift-us')

hrc <- rbind(non_doc_hrc, socure_hrc)
# do a little formatting
hrc$address_line_2[is.na(hrc$address_line_2)] <- "" # replace NA with blank string
hrc$document_ssn <- sapply(hrc$document_ssn, function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros
hrc$postal_code <- sapply(hrc$postal_code,  function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros

hrc_list <- paste(shQuote(hrc$user_id), collapse=", ")


# Highly valuable customers - "first bank account MAUs" , MAUs (6+ months), highly active app users
valuable <- queryDB(paste0("
with stg_logins as (
-- enumerate rows per event_id (there may be multiple entries)
  select user_id,
    event_id,
    row_number() over (partition by event_id order by user_id, collector_tstamp) as row_num,
    collector_tstamp
  from dbt.stg_logins
  order by 1,2,3
),
first_signin as (
-- define first signin date
  select
    user_id,
    min(collector_tstamp)::timestamp first_signin
    from stg_logins
  group by 1
),
main_login_characteristics as (
-- retrieve main login characteristics
  select 
    sl.user_id,
    fs.first_signin first_signin,
    datediff('day', first_signin, current_date) days_since_first_signin,
    count(distinct sl.event_id) num_signins
  from stg_logins sl
  left join first_signin fs
    on fs.user_id = sl.user_id
  group by 1,2
),
active_days_logins as (
-- get number of logins on the active days
  select
    user_id,
    to_char(collector_tstamp, 'YYYY-MM-DD') year_month,
    count(1)::float daily_logins -- so that stats are calculated as decimals
  from stg_logins
  where row_num = 1
  group by 1,2
  ),
active_days_trs as (
-- get number of transactions on the active days
  select
    user_id,
    to_char(completed_tstamp, 'YYYY-MM-DD') year_month,
    count(1)::float daily_trs -- so that stats are calculated as decimals
  from dbt.f_transactions
  group by 1,2
),
active_days_agg as (
-- take stats over the active days
  select
    adl.user_id,
    max(daily_logins) max_logins_per_day,
    avg(daily_logins) avg_logins_per_active_day,
    count(daily_logins) num_active_days,
    sum(case when (daily_logins is not null and daily_trs is not null) then 1 else 0 end)::float / num_active_days pct_active_days_with_tr
  from active_days_logins adl
  left outer join active_days_trs adt
    on (adl.user_id = adt.user_id and adl.year_month = adt.year_month)
  group by 1
    
),
-- mau
mau as (
    select
        a.user_id,
        count(distinct d.date) as days_mau,
        days_mau/35::float as months_mau
    from dbt.f_user_activity_txn a
    join dbt.dim_dates d
        on d.date between a.activity_start::date and a.activity_end
    where a.activity_type = '1_tx_35'
    group by 1
)
-- as a last step, put all of the above together
, valuable_users as (
  select
    mlc.user_id,
    mlc.first_signin,
    mau.months_mau,
    mlc.days_since_first_signin,
    case
        when days_since_first_signin = 0 then null
        else ada.num_active_days::decimal / mlc.days_since_first_signin
    end pct_active_days,
    case
        when days_since_first_signin = 0 then null
        else mlc.num_signins::decimal / mlc.days_since_first_signin
    end avg_logins_per_day,
    mlc.num_signins,
    ada.max_logins_per_day,
    ada.num_active_days,
    ada.pct_active_days_with_tr,
    ada.avg_logins_per_active_day
  from main_login_characteristics mlc
  join active_days_agg ada on mlc.user_id = ada.user_id
  join mau on mau.user_id = mlc.user_id
  where avg_logins_per_day > 1 -- avg logins per day more than 1
    and mau.months_mau > 6 -- at least 6 months mau
    and mlc.user_id not in (",socure_user_list,")
    and mlc.user_id not in (",hrc_list,")
    and mlc.user_id not in (",kycc_list,")
)
-- Non-Doc completions
, ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn
  from km_processes k 
  join valuable_users v on v.user_id = k.user_id
  where k.completed is not null
    and (k.primary_document = 'SSN_CARD' or k.primary_document IS null)
    and k.provider = 'SOCURE' -- non-doc completion
)
select
  ok.user_id,
  ok.provider_reference_id,
  'non_doc' as kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and u.first_fraud_flag is null -- non HRC
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
order by random()
limit 195
"), 'redshift-us')


unbanked_users <- queryDB(paste0("
-- KYC completions
with ordered_kyc as (
  select
    k.*,
    row_number() over (partition by k.user_id order by k.created desc) as rn,
    case when (k.primary_document = 'SSN_CARD' or k.primary_document IS null) then 'non_doc' else 'doc_v' end as kyc_type
  from km_processes k  
  where k.completed is not null
    --and k.provider = 'SOCURE' -- non-doc completion
    and k.user_id in ('d191bea9-3b0e-4a73-8966-71e6fceec80c',
'3f929bbb-c475-4ffc-a24d-538c6ff69eb9',
'c618a16e-442b-4e8e-a587-4a090ef390a2',
'7f201f96-e946-4614-a296-136439799dbe',
'27f0ef74-e591-4494-99b5-44628b153225',
'90d9bf6f-085e-494f-8e33-a73773e0ecc1',
'b1efbe12-063e-4001-9779-437a12dcb058',
'1bc2bae5-aa96-4905-89a2-1817fd8579fe',
'dca5e286-9e6b-4528-a5a0-d329f36e74f0',
'ec40a63b-c182-475d-aa2a-92e00730a18d',
'950c14f0-665f-48ed-b788-c1775c972b04',
'349bbf8c-3ee7-4d4b-9d61-fc4d350633f8',
'96ce2639-e654-4701-92bb-b357e8a295a6',
'81f748d2-8383-4cf6-a89f-db56751b8998',
'1a03b313-e041-4648-ab61-5f3452f1b584',
'1578939e-2c75-4fe0-a3b2-320b25aac848',
'b42e6eac-ee7e-4c06-b55f-ef4dacb184c3',
'60973c4b-efd3-4dcd-bbdf-00302e107392',
'067dcdf1-139c-4202-93b0-a2ada0c515b1',
'cfeeaa6e-6c0d-480f-a80d-28da5b0ade4c',
'b83fe6be-2fcf-4113-87c0-ca1f66f13975',
'1cc58284-7d2f-465c-9c7a-1c04645966a0',
'8ea49a3f-efe1-4376-8c6c-8484add021ac',
'e0b026d2-e14f-4cf8-82de-e3c5cd23fee8',
'2b665989-8d67-45d0-b79a-8bb8844faaa0',
'9f61a214-2acf-4ace-8e73-7d41cf1ef764',
'd8f0041a-0c11-4e11-910b-149386093bc5',
'581cc896-6add-4090-8d7d-54ceba8c91ff',
'7b039569-1b84-4c7a-a55d-7c5ba00fd3d4',
'1693068a-34b1-45df-8a2c-a18b993b926e',
'8c0fa4fc-406e-466e-9916-9a080a14e2e5',
'31eeaa64-f22b-4674-8511-94671e792163',
'134bd367-e0fc-4891-ad4e-612aeaccbfd1',
'e6d678c2-437b-4428-b5f3-ccf5a05cdd77')
)
select
  ok.user_id,
  ok.provider_reference_id,
  ok.kyc_type,
  lower(cu.first_name) as name_first,
  lower(cu.last_name) as name_last,
  replace(cu.national_identification_number,'-','') as document_ssn,
  cu.birth_date,
  lower(cu.email) as email_address,
  replace(cu.phone_number,'+1','') as phone_number,
  a.house_number_block || ' ' || initcap(a.street) as address_line_1,
  initcap(a.address_line1) as address_line_2,
  initcap(a.city) as address_city,
  upper(a.state) as address_state,
  a.zip_code as postal_code,
  a.country as address_country_code,
  cu.first_ip as ip_address_v4
from ordered_kyc ok
join dbt.dim_users u on u.user_id = ok.user_id
join etl_reporting.cmd_users cu on cu.id = ok.user_id
join etl_reporting.cmd_address a on a.user_id = ok.user_id and lower(a.type) = 'first_shipping'
left join dbt.dim_socure_kyc_redo kyc on kyc.user_id = ok.user_id
where 1=1
  and kyc.user_id is null -- exclude users who went thrugh the re-KYC process
  and ok.rn = 1 -- most recent KYC txn
  and u.main_account_status = 'OPEN'
"), 'redshift-us')

full_valuable <- rbind(valuable, unbanked_users)

# do a little formatting
full_valuable$address_line_2[is.na(full_valuable$address_line_2)] <- "" # replace NA with blank string
full_valuable$document_ssn <- sapply(full_valuable$document_ssn, function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros
full_valuable$postal_code <- sapply(full_valuable$postal_code,  function(x) paste0("'",x)) # add leading single quote to numbers with leading zeros


write.csv(full_valuable, '~/group4_customers_alloy_sample.csv')




