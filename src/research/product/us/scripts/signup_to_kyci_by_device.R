#####
# Author: Dani Mermelstein
# Date: 20200508
# Description: This query pulls the signup to KYC Initiated rate, split by signup device and webview/native.
#               Can be useful when troubleshooting the signup flow
#
#####

library(n26)
library(ggplot2)
library(data.table)

kyc_rates <- queryDB("
-- check signup to KYC initiated rate
with kyc as (
  select
  user_created,
  user_id,
  min(initiated) as kyc_i,
  min(completed) as kyc_c
  from cmd_kyc_process
  where true
  and initiated is not null 
  AND status != 'TIMED_OUT'
  group by 1,2
)
, signup_platform as (
  select distinct 
  c.user_id,
  w.os_family,
  w.webview,
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type 
  where c.derived_tstamp::date >= '2020-05-01'
  and t.se_action = 'signup.account_created'
)
select
u.user_created::date as cohort,
sp.os_family,
sp.webview,
count(distinct u.id) as cohort_size,
count(distinct kyc.kyc_i) as metric,
count(distinct kyc.kyc_i)/count(distinct u.id)::float as pct
from cmd_users u
left join kyc on kyc.user_id = u.id and kyc.kyc_i::date = u.user_created::date 
left join cmd_shadow_user s on s.user_id = u.id
left join signup_platform sp on sp.user_id = s.id
where u.user_created::date >= '2020-05-01'
group by 1,2,3
order by 1,2
", 'postgres-us')

kyc_rates_complete <- kyc_rates[complete.cases(kyc_rates$os_family),]

kyc_rates_complete$signup_device <- ifelse(kyc_rates_complete$os_family == 'ios', 'ios', ifelse(kyc_rates_complete$os_family == 'android', 'android', 'web'))
kyc_rates_complete <- as.data.table(kyc_rates_complete)
calc <- kyc_rates_complete[, .(cohort_size = sum(cohort_size),
                               metric = sum(metric),
                               pct = sum(metric)/sum(cohort_size)
                               ), 
                           by = .(signup_device, webview, cohort)]

ggplot(calc[which(cohort < '2020-05-08'),], aes(x=cohort, y=pct, group=signup_device)) +
  geom_line(aes(color=signup_device)) +
  # geom_smooth(method='loess', level=.2, span=.9) +
  scale_y_continuous(limits= c(0,1), labels = scales::percent) +
  labs(title="Signup to KYC Initiated",
       x ="Signup Cohort", 
       y = "Percent")+
  facet_wrap(~webview)

