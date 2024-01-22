library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/marketing/split_tests/kyc_yellow_path_pushes_20200923","data")


# CSVs available here: https://drive.google.com/drive/u/0/folders/1N2v2Z3aTvQmXD1KsqhYYqL9cRycRq71h
ssn_control <- read.csv('/Users/danielmermelstein/Downloads/UserID_SSN-KYC_CONTROL20201211.csv', stringsAsFactors = FALSE)
id_control <- read.csv('/Users/danielmermelstein/Downloads/KYC_ID_FINALCONTROL20201211.csv', stringsAsFactors = FALSE)

ssn_control_list <- paste(shQuote(ssn_control$UserID__c), collapse=", ")
id_control_list <- paste(shQuote(id_control$UserID__c), collapse=", ")

users <- queryDB(paste0(" 
with control_id as (
  select
    'Control_ID' as email,
      u.user_id,
      u.user_created,
      u.user_created as first_date_sent
  from dbt.zrh_users u
  where u.user_id in (", id_control_list,")
)
, control_ssn as (
  select
    'Control_SSN' as email,
    u.user_id,
    u.user_created,
    u.user_created + interval '2 hours' as first_date_sent 
  from dbt.zrh_users u
  where u.user_id in (",ssn_control_list ,")
)
, emails as (
  select 
    sfe.name as email,
    sfe.user_id::uuid as user_id,
    u.user_created,
    min(sfe.date_sent) as first_date_sent
  from public.sf_individual_email sfe
  left join dbt.zrh_users u on u.user_created = sfe.user_created    
  where sfe.name in ('CRM_JRN_KYCC_SSN_PKYC_Y_2hours',
    'CRM_JRN_KYCC_SSN_PKYC_Y_24hours',
    'CRM_JRN_KYCC_id-verify_PKYC_N',
    '20201009_CRM_JRN_KYCC_ssn_PKYC_Y_Batch-Send',
    '20201009_CRM_JRN_KYCC_id-verify_PKYC_Batch-Send',
    'CRM_JRN_KYCC_SSN_PKYC_N-SEND',
    'CRM_JRN_KYCC_id-verify_PKYC_N-SEND')
  group by 1,2,3
  
  union 
  
  select 
    ci.email,
    ci.user_id,
    ci.user_created,
    ci.first_date_sent
  from control_id ci
  
  union 
  
  select 
    cs.email,
    cs.user_id,
    cs.user_created,
    cs.first_date_sent
  from control_ssn cs
)
, final as (
  select 
    u.user_id,
    u.user_created,
    e.email,
    min(e.first_date_sent) as first_date_sent,
    min(case when kp.primary_document = 'SSN_CARD' and not kp.status = 'TIMED_OUT' then kp.created else null end) as first_ssn_submitted,
    min(case when kp.primary_document <> 'SSN_CARD' then ksd.created else null end) as first_document_submitted,
    min(u.kyc_first_completed) as kyc_first_completed 
  from dbt.zrh_users u 
  join emails e on e.user_id = u.user_id
  left join km_processes kp on kp.user_id = u.user_id 
  left join km_submitted_documents ksd on ksd.process_id = kp.id
  where u.user_created between '2020-10-20' and current_date
  group by 1,2,3
)
select 
  user_id,
  user_created,
  email,
  case when email = 'Control_ID' then first_ssn_submitted + '2 hours' else first_date_sent end as first_date_sent,
  first_ssn_submitted,
  first_document_submitted,
  kyc_first_completed
from final
"),'postgres-us')

users <- as.data.table(users)

save(users,
     file = file.path(kDataPath,"user_data.RData"))
