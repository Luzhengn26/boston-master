#####
# Author: Dani Mermelstein
# Date: 20200508
# Description: This query pulls the signup to product selection rate, split by signup device and webview/native.
#               Can be useful when troubleshooting the signup flow
#
#####

library(n26)
library(ggplot2)
library(data.table)

product_selection_rates <- queryDB("
 with product as ( 
    select
      user_created,
      min(created)
    from lp_user_product 
    where product_id = 'STANDARD'
    group by 1
)
, signup_platform as (
  select distinct 
  c.user_id,
  w.os_family,
  w.webview
  from public.ksp_event_core c 
  join public.ksp_event_crab kec on kec.event_id = c.event_id
  join etl_reporting.ksp_web_crab w on w.event_id = c.event_id 
  join public.ksp_event_types t on t.event_type = c.event_type 
  where c.derived_tstamp::date >= '2020-05-01'
  and t.se_action = 'signup.account_created'
)
select
u.user_created::date as cohort,
sp.webview,
sp.os_family,
count(distinct u.id) as cohort_size,
count(distinct p.user_created) as metric,
count(distinct p.user_created)/count(distinct u.id)::float as pct
from cmd_users u
left join cmd_shadow_user s on s.user_id = u.id
left join signup_platform sp on sp.user_id = s.id
left join product p on p.user_created = u.user_created 
where u.user_created::date >= '2020-05-01'
group by 1,2,3
order by 1,2,3
", 'postgres-us')


product_rates_complete <- product_selection_rates[complete.cases(product_selection_rates$os_family),]

product_rates_complete$signup_device <- ifelse(product_rates_complete$os_family == 'ios', 'ios', ifelse(product_rates_complete$os_family == 'android', 'android', 'web'))
product_rates_complete <- as.data.table(product_rates_complete)
calc <- product_rates_complete[, .(cohort_size = sum(cohort_size),
                                   metric = sum(metric),
                                   pct = sum(metric)/sum(cohort_size)
                               ),
                               by = .(signup_device, webview, cohort)]


ggplot(calc[which(cohort < '2020-05-08'),], aes(x=cohort, y=pct, group=signup_device)) +
  geom_line(aes(color=signup_device)) +
  geom_smooth(method='loess', level=.2, span=.9) +
  scale_y_continuous(limits= c(0,1), labels = scales::percent) +
  labs(title="Signup to Product Selection",
       x ="Signup Cohort", 
       y = "Percent")+
  facet_wrap(~webview)