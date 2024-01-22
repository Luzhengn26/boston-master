#setwd('/Users/wendyvu/Documents/transaction_tag_usage/')
library(n26)
library(data.table)


logins <- queryDB("

--- Activity groupings based on mau defined by >= 1 deposit per 35 day period
drop table if exists mau_dep;
create temp table mau_dep as 
select 
	k.user_created,
	k.kyc_first_completed as kycc,
	k.card_first_activated as card_act,
	is_premium,
	z.txn_date,
	z.value,
	datediff('days',kycc,z.txn_date) as days_kycc_ct,
	ceil(days_kycc_ct::float/35) as period --- period corresponds to 35 days, which is considered a month
from dbt.zrh_users k
left join cmd_user_closure c 
	on k.user_created = c.user_created
join dbt.zrh_txn_day_rows z
	on k.user_created = z.user_created 
		--and feature in ('amount_cents_ct','amount_cents_cash26_in','amount_cents_ft_in','amount_cents_stripetopup_in')
		and feature = 'n_ext_total_in'
where z.txn_date between kycc and kycc + interval '210 days'
	and kycc between '2019-01-01' and '2019-03-31'
	and c.closed_at is null 
order by 2,5;

--- Total monthly active groups
drop table if exists mau_act;
create temp table mau_act as 
select 
	user_created,
	kycc,
	card_act,
	is_premium,
	count(period) as mau_act --- total number of months active groupings
from (select user_created, kycc, card_act, is_premium, period from mau_dep group by 1,2,3,4,5)  
group by 1,2,3,4;

-- 	WAU
drop table if exists activity;
create temp table activity as
select 
	k.user_created,
	k.kycc,
	t.activity_type,
	t.activity_start,
	t.activity_end,
	case when t.activity_end > kycc + interval '210 days' then kycc + interval '210 days' else t.activity_end end as act_end,
	date_diff('days',activity_start,act_end) as days
from (select user_created,kycc from mau_dep group by 1,2) as k 
join dbt.zrh_user_activity_txn t 
	on k.user_created = t.user_created
where activity_start between kycc and kycc + interval '210 days'
order by 1,4,5;


-- IN-APP FEATURE EVENTS
-- what features are the most popular in the first 35 days of kycc?
-- what features stick and people keep going back to?
drop table if exists events;
create temp table events as
select 
	k.user_created,
	k.kycc,
	is_premium,
	k.mau_act,
	a.feature,
	--ceil(date_diff('day',k.kycc,a.event_dt)::float/35) as period,
	sum(a.value) as feat_cnt
from mau_act as k
left join dbt.zrh_main_events a  
	on a.user_created = k.user_created
where a.event_dt between kycc and kycc + interval '210 days'
group by 1,2,3,4,5;

--LOGINS AND TOTAL MONTHS ACTIVE
drop table if exists logins_txn;
create temp table logins_txn as
select 
	k.user_created,
	k.kycc,
	k.card_act,
	k.is_premium,
	k.mau_act,
	a.act_date,
	row_number() over (partition by k.user_created order by act_date) as rn_log_date,
	date_diff('days',k.kycc,k.card_act) as kycc_card_days,
	date_diff('days',k.kycc,a.act_date) as kycc_log_days,
	date_diff('days',k.card_act,a.act_date) as card_log_days,
	ceil(kycc_log_days::float/35) as period, --- period corresponds to 35 days, which is considered a month
	a.n_logins,
	a.n_act_txns 
from mau_act as k 
left join dbt.zrh_act_day a 
	on k.user_created = a.user_created 
where a.act_date between kycc and kycc + interval '210 days';

drop table if exists login_txn_monthly;
create temp table login_txn_monthly as 
select 
	user_created,
	kycc,
	card_act,
	is_premium,
	mau_act,
	period,
	sum(n_logins) as monthly_login_sum,
	round(monthly_login_sum::float/30,1) as login_daily_avg,
	round(monthly_login_sum::float/5,1) as login_weekly_avg,
	sum(n_act_txns) as monthly_txns_sum
from logins_txn
group by 1,2,3,4,5,6;

select 
	a.user_created,
	to_char(date_trunc('months',kycc),'YYYY-MM') as cohort,
	card_act,
	is_premium,
	kycc_card_days,
	kycc_log_days,
	card_log_days,
	mau_act,
	count(period) as log_txn_act,
	sum(monthly_login_sum) as login_total,
	sum(monthly_txns_sum) as txns_total,
	round(avg(monthly_login_sum),1) as login_monthly_avg,
	round(avg(monthly_txns_sum),1) as txns_monthly_avg,
	round(avg(login_daily_avg),1) as login_daily_avg,
	round(avg(login_weekly_avg),1) as login_weekly_avg
from login_txn_monthly a 
join (
	select 
		user_created,
		kycc_card_days,
		kycc_log_days,
		card_log_days
	from logins_txn 
	where rn_log_date = 1
	) as b
	on a.user_created = b.user_created
group by 1,2,3,4,5,6,7,8
order by 1;

" , "redshift-eu")

save(logins,
     file = file.path("early_topup_behavior.RData"))