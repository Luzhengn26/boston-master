setwd('/Users/wendyvu/Documents/transaction_tag_usage/')
library(n26)
library(data.table)


tags <- queryDB("
drop table if exists events;
create temp table events as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.se_value,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
where ec.collector_tstamp >= current_date - interval '7 days'
	and et.se_action in ('transactions.details_tag_actions','login',
			'transactions.details_category_change_clicked', 'transactions.details_photo_added', 
			'search.search_used', 'transactions.transaction_details_viewed', 'search_viewed',
			'feed.search.tag_selected','search.search_used','support.searched')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

with tags as (
select 
	distinct user_created
from dev.events
where se_action = 'feed.search.tag_selected'
), txn as (
select 
	user_created,
	sum(n_ext_total) as total_txns
from dbt.zrh_txn_day z 
where txn_date > current_date - interval '35 days'
group by 1
order by 2 desc 
)
select 
	avg(total_txns),
	median(total_txns)
from tags
join txn using (user_created)

" , "redshift-eu")

search <- queryDB("
drop table if exists events;
create temp table events as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.se_value,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
where ec.collector_tstamp >= current_date - interval '7 days'
	and et.se_action in ('transactions.details_tag_actions','login',
			'transactions.details_category_change_clicked', 'transactions.details_photo_added', 
			'search.search_used', 'transactions.transaction_details_viewed', 'search_viewed',
			'feed.search.tag_selected','search.search_used','support.searched')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

-- Of all the people who viewed transactions how many of them through the search function

with tags as (
select 
	distinct user_created
from events
where se_action = 'transactions.transaction_details_viewed'
	and se_value = 0
), txn as (
select 
	user_created,
	sum(n_ext_total) as total_txns
from dbt.zrh_txn_day z 
where txn_date > current_date - interval '35 days'
group by 1
order by 2 desc 
)
select 
	avg(total_txns),
	median(total_txns)
from tags
join txn using (user_created);
                
", "redshift-eu")


smart_tags <- queryDB("
drop table if exists events2;
create temp table events2 as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
--where ec.collector_tstamp >= date_trunc('month',current_date) - interval '7 days'
	where et.se_action in ('transactions.details_tag_actions','transactions.details_category_change_clicked','transactions.details_photo_added')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

--what smart category transactions are used the most with tags?
drop table if exists smart;
create temp table smart as 
select
	e.user_created,
	e.se_action,
	e.se_label,
	s.id,
	s.type,
	s.amount_cents,
	s.system_smart_category as sys_cat,
	s.user_smart_category as user_cat,
	case when sys_cat is not null and user_cat is not null then user_cat 
		when sys_cat is null and user_cat is not null then user_cat
		when sys_cat is not null and user_cat is null then sys_cat
		else null end as smart_category
from events2 e 
left join ddb_smart_transaction s 
	on e.se_property = s.id; 
	
with cat as (
select 
	id,
	se_action,
	smart_category,
	count(smart_category)
from smart
group by 1,2,3
)
select
	smart_category,
	count(smart_category)
from cat
where se_action = 'transactions.details_tag_actions'
group by 1
order by 2 desc;
                
", "redshift-eu")


tag_users <- queryDB("
drop table if exists events2;
create temp table events2 as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
--where ec.collector_tstamp >= date_trunc('month',current_date) - interval '7 days'
	where et.se_action in ('transactions.details_tag_actions','transactions.details_category_change_clicked','transactions.details_photo_added')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

--  Is this a feature that is mostly used by new users or long term users? 
with tags as (
select 
	user_created,
	event_type,
	se_category,
	se_action,
	se_label,
	se_property,
	collector_tstamp,
	date_trunc('months',user_created) as user_cohort_month
from events2 e
where se_action in ('transactions.details_tag_actions','transactions.details_category_change_clicked','transactions.details_photo_added')
order by 1
)
select 
count(distinct user_created)
from tags 
limit 500;
                
", "redshift-eu")


user_activity_tags <- queryDB("
drop table if exists events2;
create temp table events2 as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
--where ec.collector_tstamp >= date_trunc('month',current_date) - interval '7 days'
	where et.se_action in ('transactions.details_tag_actions','transactions.details_category_change_clicked','transactions.details_photo_added')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

-- Are tag users more financially engaged users?
with tag_count as (
select 
	user_created,
	count(se_property) as tag_count
from events2
group by 1
order by 2 desc 
), wau as (
select 
	user_created,
	sum(date_diff('weeks',activity_start,activity_end)) as weeks_wau,
	date_diff('weeks',user_created, current_date) as total_weeks,
	(weeks_wau/total_weeks::float) as percent_weeks_active
from dwh_user_activity 
where activity_type = 4 and user_created < current_date - interval '1 day'
group by 1
)
select 
	avg(t.tag_count) as avg_tags,
	avg(w.percent_weeks_active) as avg_percent_weeks_active,
	median(w.percent_weeks_active) as med_percent_weeks_active
from wau w
left join tag_count t 
	on w.user_created = t.user_created
where tag_count is null
order by 1;
                
", "redshift-eu")


sau_tags <- queryDB("
drop table if exists events2;
create temp table events2 as 
select 
	eu.user_id,
	cs.user_created,
	ec.event_type,
	et.se_category,
	et.se_action,
	ec.se_label,
	ec.se_property,
	ec.collector_tstamp
from public.ksp_event_crab ec 
inner join public.ksp_event_userid eu using (event_id)
join cmd_shadow_user cs on eu.user_id = cs.id
inner join public.ksp_event_types et on ec.event_type = et.event_type
--where ec.collector_tstamp >= date_trunc('month',current_date) - interval '7 days'
	where et.se_action in ('transactions.details_tag_actions','transactions.details_category_change_clicked','transactions.details_photo_added')
	--and et.se_category='authentication' and et.se_action='login'
	--and area != 'error'
order by 1, ec.collector_tstamp;

-- What proportion of tag users are currently SAU?
with tag_count as (
select 
	user_created,
	count(se_property) as tag_count
from events2
group by 1
order by 2 desc 
), sau as (
select 
	user_created,
	activity_type
from dwh_user_activity 
where activity_type = 3 
	and user_created < current_date - interval '1 day'
	and current_date between activity_start and activity_end
group by 1,2
)
select 
	count(distinct case when activity_type is not null then t.user_created end) as sau,
	count(distinct t.user_created) as total
from tag_count t
left join sau s
	on s.user_created = t.user_created
;

", "redshift-eu")


save(tags,
     search,
     smart_tags,
     tag_users,
     user_activity_tags,
     sau_tags,
     file = file.path("transaction_tag_usage.RData"))