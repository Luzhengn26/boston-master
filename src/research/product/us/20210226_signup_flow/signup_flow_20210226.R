library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/us/signup_flow_20210226","data")

flow <- queryDB(paste0("
with actions as (
  select 
    zuf.domain_userid,
    case when zuf.os_family = 'ios' then 'ios'
      when zuf.os_family = 'android' then 'android'
      else 'web' end as signup_device,
    min(case when zuf.step = 'signup-start' then zuf.created else null end) as signup_start,
    min(case when zuf.step = 'personal-information' then zuf.created else null end) as personal_information,
    min(case when zuf.step = 'phone-number' then zuf.created else null end) as phone_number,
    min(case when zuf.step = 'address' then zuf.created else null end) as address,
    min(case when zuf.step = 'address-confirmation' then zuf.created else null end) as address_confirmation,
    min(case when zuf.step = 'social-security-number' then zuf.created else null end) as social_security_number,
    min(case when zuf.step = 'additional-information' then zuf.created else null end) as additional_information,
    min(case when zuf.step = 'create-password' then zuf.created else null end) as create_password,
    min(case when zuf.step = 'create-account' then zuf.created else null end) as create_account,
    min(case when zuf.step = 'email-confirmation' then zuf.created else null end) as email_confirmation,
    max(case when u.kyc_first_completed is not null then 1 else 0 end) as kycc,
    max(case when t.is_first_time_mau is true then 1 else 0 end) as ft_mau
  from dev_dbt.zrh_upper_funnel zuf 
  left join dev_dbt.zrh_users u on u.user_id = zuf.user_id
  left join dev_dbt.zrh_transactions t on t.user_id = u.user_id
  where zuf.country = 'USA'
    and zuf.step in ('signup-start',
    'personal-information',
    'phone-number',
    'address',
    'address-confirmation',
    'social-security-number',
    'additional-information',
    'create-password',
    'create-account',
    'email-confirmation')
  group by 1,2
)
, flow as (
  select 
    signup_device,
    count(distinct case when signup_start is not null then domain_userid else null end) as signup_start,
    count(distinct case when personal_information > signup_start then domain_userid else null end) as personal_information,
    count(distinct case when phone_number > personal_information then domain_userid else null end) as phone_number,
    count(distinct case when address > phone_number then domain_userid else null end) as address,
    count(distinct case when address_confirmation > address then domain_userid else null end) as address_confirmation,
    count(distinct case when social_security_number > address_confirmation then domain_userid else null end) as social_security_number,
    count(distinct case when additional_information > social_security_number then domain_userid else null end) as additional_information,
    count(distinct case when create_password > additional_information then domain_userid else null end) as create_password,
    count(distinct case when create_account > create_password then domain_userid else null end) as create_account,
    count(distinct case when email_confirmation > create_account then domain_userid else null end) as email_confirmation,
    sum(kycc) as kycc,
    sum(ft_mau) as ft_mau
  from actions
  where signup_start is not null
  group by 1
)
select 
  *
from flow
where 1=1
"), "redshift-us")


# set as datatable so we can easily aggregate
flow <- as.data.table(flow)

save(flow,
     file = file.path(kDataPath,"signup_flow_data.RData"))

