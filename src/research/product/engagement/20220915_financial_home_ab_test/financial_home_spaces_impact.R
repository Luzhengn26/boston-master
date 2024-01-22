setwd('/Users/wendyvu/Documents/financial_home_spaces_impact/')
library(n26)
library(data.table)

event_views <- queryDB("
--How many customers are viewing the new fin home? - # fin home impressions
with finhome as (
select 
	group_,
	user_type,
	status_spaces,
	--collector_date,
	case when se_action = 'financial_home.long.press' then 'fh_longpress'
		when se_action = 'financial_home.main_account.clicked' then 'fh_main_acct_click'
		when se_action = 'financial_home.spaces.clicked' then 'fh_space_details_click'
		when se_action = 'financial_home.spaces.shared.certification.clicked' then 'fh_shared_certify'
		when se_action = 'financial_home.error_retry_clicked' then 'fh_error_retry'
		when se_action = 'financial_home.spaces.createspace.clicked' then 'fh_create_space'
		when se_action = 'financial_home.tab.viewed' then 'fh_view'
		when se_action = 'financial_home.spaces.transfer.clicked' then 'fh_transfer_click_dnd'
		when se_action = 'financial_home.drag.drop' then 'fh_dnd' --drag_drop
		when se_action = 'financial_home.spaces.shared.invitation.clicked' then 'fh_shared_invite'
		when se_action = 'financial_home.spaces.emptystate.createspace.clicked' then 'fh_emptystate_create_space'
		when se_action is null then 'no_fh_view'
			else se_action end as feature,
	tot_user_grp_type,
	count(distinct u.user_created) as n_users,
	round(n_users::float/tot_user_grp_type,4) as perc_users
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) u
left join (select * 
			from dbt.snowplow 
			where collector_date >= '2022-02-22'::date 
				and se_action ilike 'financial_home%'
				) s 
	 on u.user_created = s.user_created 
	 and collector_date >= start_date
group by 1,2,3,4,5
)
select group_,
	user_type,
	feature,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	tot_user_grp_type,
	n_users,
	round(n_users::float/tot_user_grp_type,4) as perc_users
from finhome
where group_ ilike 'test%'
group by 1,2,3,4,5,6
order by 1,2,3
;
                       
                       ","redshift-eu")

create_spaces <- queryDB("
--Can customers still understand how to create a space? - # of space creation
-- control group created spaces more often than test group who saw FH
with space_created as (
select --u.*,
	u.user_created,
	start_date,
	group_,
	user_type,
	status_spaces,
	tot_user_grp_type,
	a.activity_type,
	a.space_id,
	a.created
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) u
join dbt.zrh_users z 
	using(user_created)
join w_activity_log a 
	on a.initiator_user_id = z.user_id 
	and a.created >= u.start_date 
	and activity_type = 'SPACE_CREATED'
)
select 
	start_date,
	user_type,
	group_,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	tot_user_grp_type,
	--date_trunc('week',created) as week,
	count(distinct user_created) as n_users,
	count(distinct space_id) as n_space_created,
	round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created 
group by 1,2,3,4,5
order by 1,2,3,4
;
                         ","redshift-eu")

dragndrop <- queryDB("
with finhome as (
select 
	u.user_created,
	start_date,
	group_,
	user_type,
	status_spaces,
	--collector_date,
	case when se_action ilike 'financial%' then 'fh_dragdrop' else se_action end as feature,
	tot_user_grp_type
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) as u
left join (select * 
			from dbt.snowplow 
			where collector_date >= '2022-02-22'::date 
				and se_action in ('spaces.dadmovemoney_viewed' --,'financial_home.spaces.transfer.clicked','financial_home.drag.drop'
									)
				) s 
	 on u.user_created = s.user_created 
	 and collector_date >= start_date
group by 1,2,3,4,5,6,7
)
select
	case when start_date = '2022-02-22' then '3k' 
		else '35k' end as sampling,
	user_type,
	group_,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	--feature,
	tot_user_grp_type,
	count(distinct user_created) as n_users,
	round(n_users::float/tot_user_grp_type,4) as perc_users
from finhome
where feature is not null
group by 1,2,3,4,5
order by 1,2,3,4
limit 500;


                     
                     ","redshift-eu")

spaces_txns <- queryDB("
--Can customers understand how to do money transfer? - # of space tx/user, # of tx performed by DnD
-- no significant difference between test and control group when it comes to the avg amount of txns and users initiaiting spaces txns
with space_created as (
select u.user_created,
	user_type,
	group_,
	tot_user_grp_type,
	status_spaces,
	--z.txn_date,
	sum(n_spaces) as n_spaces
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) u
join dbt.zrh_txn_day z  
	on u.user_created = z.user_created 
	and z.txn_date >= start_date
	and n_spaces > 0
group by 1,2,3,4,5
)
select 
	user_type,
	group_,
	tot_user_grp_type,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	--date_trunc('week',created) as week,
	--median(n_spaces) as med_spaces_txn
	count(distinct user_created) as n_users,
	round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created 
group by 1,2,3,4
order by 1,2,3
limit 500;
                       ","redshift-eu")

sampling <- queryDB("
select 
	group_,
	user_type,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	percentile_cont(0.2) within group (order by n_spaces_txns) as perc_20,
	percentile_cont(0.3) within group (order by n_spaces_txns) as perc_25,
    percentile_cont(0.5) within group (order by n_spaces_txns) as perc_50,
    percentile_cont(0.8) within group (order by n_spaces_txns) as perc_80,
    percentile_cont(0.9) within group (order by n_spaces_txns) as perc_90,
    avg(n_spaces_txns) as mean
from dev.fh_users 
group by 1,2,3 
order by 1, 2 
limit 500;
                    ","redshift-eu")

num_txns <- queryDB("
with space_created as (
select u.user_created,
	user_type,
	group_,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	tot_user_grp_type,
	--z.txn_date,
	coalesce(sum(n_spaces),0) as n_spaces
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) u
left join dbt.zrh_txn_day z  
	on u.user_created = z.user_created 
	and z.txn_date >= start_date
	--and n_spaces > 0
group by 1,2,3,4,5
)
select 
	user_type,
	group_,
	tot_user_grp_type,
	status_spaces,
	--date_trunc('week',created) as week,
	median(n_spaces) as med_spaces_txn,
	avg(n_spaces) as avg_spaces_txn,
	sum(n_spaces) as tot_spaces_txn
--	count(distinct user_created) as n_users,
--	round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created 
group by 1,2,3,4
order by 1,2,3

                    ","redshift-eu")


txn_users <- queryDB("
with space_created as (
select u.user_created,
	user_type,
	group_,
	case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
	tot_user_grp_type,
	--z.txn_date,
	sum(n_spaces) as n_spaces
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) u
join dbt.zrh_txn_day z  
	on u.user_created = z.user_created 
	and z.txn_date >= start_date
	and n_spaces > 0
group by 1,2,3,4,5
)
select * from space_created 
                     ","redshift-eu")

dragndrop_users <- queryDB("
with finhome as (
select 
	u.user_created,
	start_date,
	group_,
	user_type,
	status_spaces,
	--collector_date,
	case when se_action is null then 0 else 1 end as actions,
	tot_user_grp_type
from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from dev.fh_users) as u
left join (select * 
			from dbt.snowplow 
			where collector_date >= '2022-02-22'::date 
				and se_action in ('spaces.dadmovemoney_viewed' --,'financial_home.spaces.transfer.clicked','financial_home.drag.drop'
									)
				) s 
	 on u.user_created = s.user_created 
	 and collector_date >= start_date
group by 1,2,3,4,5,6,7
)
select start_date, group_, user_type, status_spaces, tot_user_grp_type, sum(actions) as actions 
from finhome 
group by 1,2,3,4,5
order by 1,2,3,4
limit 500;
                           ","redshift-eu")

save(event_views,
     create_spaces,
     dragndrop,
     spaces_txns,
     sampling,
     num_txns,
     txn_users,
     dragndrop_users,
     file = file.path("financial_home_spaces_impact.RData"))
 