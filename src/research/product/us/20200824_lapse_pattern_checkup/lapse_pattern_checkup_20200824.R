#####
# Author: Dani Mermelstein
# Date: 20200824
# Description: Lapse analysis: are certain lapse behaviors increasing, or are recent cohorts exhibiting unexpected behavior?
#
#####

library(n26)
library(data.table)
library(ggplot2)


mau_status <- queryDB("
-- lapse investigation
WITH temp_dwh_cohort_months AS (
    SELECT DISTINCT
        date_trunc('month', start_time)::date AS month
      , start_time
      , end_time
    FROM dwh_cohort_months m
), act as (
    select distinct
        A.user_created,
        B.id as user_id,
        A.activity_start,
        min(A.activity_start) over (partition by A.user_created) = activity_start as first_activity,
        min(a.activity_end) over (partition by a.user_created) = activity_end as first_lapse,
        A.activity_end >= coalesce(B.closed_at, '2100-01-01') as account_closed, -- if the account closed field is populated = TRUE
        least(coalesce(B.closed_at, '2100-01-01'), A.activity_end) as activity_end -- if account closed field is populated = Account Closed Date
    FROM dbt.zrh_user_activity_txn A
    JOIN cmd_users B ON A.user_created = B.user_created
    -- exclude fraudsters
    join public.cr_user_account cua on cua.user_id = B.id
    join public.cr_account ca on ca.id = cua.account_id and ca.status != 'SEIZED'
    WHERE A.activity_type = '1_tx_35'
)
, users as (
select 
  c.month,
  a.user_id,
  min(case when a.first_activity then a.activity_start else null end) as ft_mau,
  min(case when a.activity_start between c.start_time and c.end_time and a.first_activity then a.activity_start else null end) as new_first,
  min(case when a.activity_start between c.start_time and c.end_time and not a.first_activity then a.activity_start else null end) as new_reactivated,
  min(case when a.activity_end between c.start_time and c.end_time and a.account_closed then a.activity_end else null end) as lost_closed,
  min(case when a.activity_end between c.start_time and least(c.end_time, current_date) and not a.account_closed and a.first_lapse then a.activity_end else null end) as first_lapser,
  min(case when a.activity_end between c.start_time and least(c.end_time, current_date) and not a.account_closed and not a.first_lapse then a.activity_end else null end) as relapser
from temp_dwh_cohort_months c 
cross join act a --on a.activity_start between c.start_time and c.end_time
where c.start_time > '2019-12-31'
  and c.start_time < current_date
group by 1,2
order by 1,2
)
select 
  *,
date_trunc('month', ft_mau)::date as mau_cohort,
floor(date_part('day', first_lapser - ft_mau)/30) as first_lapse_month,
floor(date_part('day', relapser - ft_mau)/30) as relapse_month,
floor(date_part('day', new_reactivated - ft_mau)/30) as reactivation_month
from users
",'postgres-us')

mau_status <- as.data.table(mau_status)

# let's look at first lapse patterns
first_lapse <- mau_status[, .(users=length(unique(user_id))), by=.(month, mau_cohort, first_lapse_month)][which(first_lapse_month > 0),]
first_lapse <- merge(first_lapse, mau_status[, .(total_users=length(unique(user_id))), by=.(mau_cohort)], by="mau_cohort")
first_lapse$lapse_rate <- first_lapse$users/first_lapse$total_users
first_lapse <- first_lapse[which(total_users >= 100),]
first_lapse$highlight <- ifelse(first_lapse$mau_cohort %in% c('2020-08-01', '2020-07-01'), "recent_cohorts", "others")

first_lapse$first_lapse_month <- as.factor(first_lapse$first_lapse_month)
first_lapse$mau_cohort <- as.factor(first_lapse$mau_cohort)
first_lapse$highlight <- as.factor(first_lapse$highlight)
ggplot(first_lapse, aes(x=first_lapse_month, y=lapse_rate, fill=mau_cohort))+
  geom_dotplot(binaxis='y', stackdir='center')+
  ylim(0,1)+
  facet_wrap(~mau_cohort)+
  labs(title="First Lapse Rate by MAU cohort, activity for January 2020 - August 2020",
       x ="Months since FT MAU", 
       y = "First Lapse Pct")


# condensed to show overlay
overlay <- first_lapse[, .(lapse_rate=sum(users)/mean(total_users)), by= .(first_lapse_month, mau_cohort)]
overlay$highlight <- ifelse(overlay$mau_cohort %in% c('2020-06-01', '2020-07-01'), "recent_cohorts", "others")
ggplot(overlay, aes(x=first_lapse_month, y=lapse_rate, fill=highlight))+
  geom_dotplot(binaxis='y', stackdir='center')+
  # geom_line(aes(color=highlight))+
  ylim(0,1)+
  # facet_wrap(~mau_cohort)+
  labs(title="First Lapse Rate by MAU cohort, activity for January 2020 - August 2020 (overlay)",
       x ="Months since FT MAU", 
       y = "First Lapse Pct")

# let's look at relapse patterns
relapsers <- mau_status[, .(users=length(unique(user_id))), by=.(month, mau_cohort, relapse_month)][which(relapse_month > 0),]
relapsers <- merge(relapsers, mau_status[, .(total_users=length(unique(user_id))), by=.(mau_cohort)], by="mau_cohort")
relapsers$relapse_rate <- relapsers$users/relapsers$total_users
relapsers <- relapsers[which(total_users >= 100),]

relapsers$relapse_month <- as.factor(relapsers$relapse_month)
relapsers$mau_cohort <- as.factor(relapsers$mau_cohort)
ggplot(relapsers, aes(x=relapse_month, y=relapse_rate, fill=mau_cohort))+
  geom_dotplot(binaxis='y', stackdir='center')+
  ylim(0,.4)+
  facet_wrap(~mau_cohort)+
  labs(title="Relapse Rate by MAU cohort, activity for January 2020 - August 2020",
       x ="Months since FT MAU", 
       y = "Relapse Pct")


# let's look at reactivation patterns
reactivations <- mau_status[, .(users=length(unique(user_id))), by=.(month, mau_cohort, reactivation_month)][which(reactivation_month > 0),]
reactivations <- merge(reactivations, mau_status[, .(total_users=length(unique(user_id))), by=.(mau_cohort)], by="mau_cohort")
reactivations$relapse_rate <- reactivations$users/reactivations$total_users
reactivations <- reactivations[which(total_users >= 100),]

reactivations$reactivation_month <- as.factor(reactivations$reactivation_month)
reactivations$mau_cohort <- as.factor(reactivations$mau_cohort)
ggplot(reactivations, aes(x=reactivation_month, y=relapse_rate, fill=mau_cohort))+
  geom_dotplot(binaxis='y', stackdir='center')+
  ylim(0,1)+
  facet_wrap(~mau_cohort)+
  labs(title="Reactivation Rate by MAU cohort, activity for January 2020 - August 2020",
       x ="Months since FT MAU", 
       y = "Reactivation Pct")


# Are the numerical increases in relapsers and reactivations correlated to the total number of FT MAU?
# ie are these increases just due to the growth of our funded userbase?

ft_maus <- queryDB("
-- FT MAU running sum
with cnt as (
  select 
    date_trunc('month', first_time_mau_tstamp)::date as month,
    count(distinct user_id) as ft_mau
  from dbt.zrh_users
  where first_time_mau_tstamp is not null
  group by 1
  order by 1
)
select 
  month,
  ft_mau,
  sum(ft_mau) over (order by month) as total_ft_mau
from cnt
",'postgres-us')

ft_maus <- merge(mau_status[, .(reactivations=length(unique(new_reactivated)), relapsed=length(unique(relapser))), by=.(month)][which(month < '2020-08-01'),], ft_maus, by="month")

# cor.test(x, y, method=c("pearson", "kendall", "spearman"))
cor.test(ft_maus$reactivations, ft_maus$total_ft_mau, method=c("pearson"))
cor.test(ft_maus$relapsed, ft_maus$total_ft_mau, method=c("pearson"))
