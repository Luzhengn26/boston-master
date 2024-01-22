#####
# Author: Dani Mermelstein
# Date: 20200827
# Description: User lists for Ekata risk scoring analysis
#
#####

library(n26)
library(data.table)
library(tidyverse)

# import external lists
# external files can be found here: https://drive.google.com/drive/u/0/folders/1lLGZD8_fppqirSV0DIoDq7sf4StTg5HG
atm_incident <- read.csv("/Users/danielmermelstein/Downloads/3. ATM incident - users to investigate for YP (2).csv", stringsAsFactors = FALSE)
ip_fraud <- read.csv("/Users/danielmermelstein/Downloads/2. IP related fraud (MS 08.11.2020).csv", stringsAsFactors = FALSE)

atm_incident_users <- paste(shQuote(atm_incident$user_id), collapse=", ")
ip_fraud_codes <- paste(shQuote(ip_fraud$provider_reference_id), collapse=", ")

 # This query returns users who are KYCC (with AML flags for fraud or not), plus the highest risk users as scored by Socure
# also making sure users that AML has manually flagged as fraudsters are flagged
base <- queryDB(paste0("
with base as (
  -- base of all users
  select distinct
    kp.user_id,
    cu.user_created::date as signup_date,
    date_trunc('month', kp.created)::date as month,
    cu.first_name,
    cu.last_name,
    a.house_number_block,
    a.street,
    a.address_line1,
    a.address_line2,
    a.city,
    a.zip_code,
    a.country,
    cu.phone_number,
    cu.email,
    cu.first_ip,
    case when zu.kyc_first_completed is not null then 1 else 0 end as kycc,
    case when kp.user_id in (", atm_incident_users ,") then 1 else 0 end as atm_fraud_flag
  from km_processes kp 
  join dbt.zrh_users zu on zu.user_id = kp.user_id
  join etl_reporting.cmd_users cu on cu.id = kp.user_id
  join etl_reporting.cmd_address a on a.user_id = kp.user_id and a.type = 'FIRST_SHIPPING'
  where kp.created between '2019-09-01' and '2020-06-30'
    and kp.status <> 'TIMED_OUT'
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
, socure_fails as (
  -- Users who immediately fail Socure (highest risk)
  select distinct
    u.id as user_id
  from cmd_users u
  join km_processes k on k.user_id = u.id
  join cmd_kyc_process c on k.id = c.id
  where k.initiated is not null 
    and k.status = 'FAILED'
    and k.primary_document = 'SSN_CARD'
  group by 1
)
, ip_fraud as (
  select distinct 
    user_id
  from km_processes
  where provider_reference_id in (",ip_fraud_codes,")
)
, list as (
  select 
    b.*,
    case when fl.user_id is not null then 1 else 0 end as aml_fraud_flag,
    case when sf.user_id is not null then 1 else 0 end as socure_fraud_flag,
    case when if.user_id is not null then 1 else 0 end as ip_fraud_flag
  from base b 
  left join fraud_labels fl on fl.user_id = b.user_id
  left join socure_fails sf on sf.user_id = b.user_id
  left join ip_fraud if on if.user_id = b.user_id
)
select 
  *
from list 
"), 'postgres-us')


base <- as.data.table(base)
base[, .(cnt = .N, atm_fraud=sum(atm_fraud_flag), aml_flag=sum(aml_fraud_flag), ip_fraud=sum(ip_fraud_flag)), by=.(month)][order(month),]


# subset data for Ekata
export <- base[,c("user_id",
                  "signup_date",
                  "first_name",
                  "last_name",
                  "house_number_block",
                  "street",
                  "address_line1",
                  "address_line2",
                  "city",
                  "zip_code",
                  "country",
                  "phone_number",
                  "email",
                  "first_ip"
                  )]

# write file locally, follow encryption instructions before sending to an external party: https://number26-jira.atlassian.net/wiki/spaces/SEC/pages/1809652637/Encrypted+file+transfer+procedure#Prepare-the-file-for-sending
write.csv(export, "~/ekata_user_file.csv")


# write the labeled version for internal users
write.csv(base, "~/internal_ekata_test_data.csv")


# write a labeled version for Ekata
base$fraudster <- apply(base[,c("atm_fraud_flag","aml_fraud_flag","ip_fraud_flag")], 1, max)


export <- base[,c("user_id",
                  "signup_date",
                  "first_name",
                  "last_name",
                  "house_number_block",
                  "street",
                  "address_line1",
                  "address_line2",
                  "city",
                  "zip_code",
                  "country",
                  "phone_number",
                  "email",
                  "first_ip",
                  "fraudster"
                  )]

write.csv(export, "~/labeled_ekata_test_data.csv")
