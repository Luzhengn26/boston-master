-- First we check for daily usage in H1 2022
with od_users as (
select
	end_time,
	user_created,
	outstanding_balance_eur,
	max_amount_cents::numeric/100 as max_amount_eur,
	outstanding_balance_eur::numeric/max_amount_eur as perc_usage
from dbt.bp_overdraft_users ou
where od_enabled_flag
	and timeframe = 'day'
	and end_time between '2022-01-01' and '2022-06-30'
),
--Then we aggregate it on a monthly basis and find how many days in a month users use OD
monthly_usage as (
select
	to_char(end_time, 'YYYY-MM') as month,
	user_created,
	avg(lsa.rating_class::float) as rating_class,
	count(case when outstanding_balance_eur is not null then 1 end) as n_days,
	min(max_amount_eur) as min_limit,
	avg(perc_usage) as perc_usage
from od_users ou
inner join dbt.zrh_users u using (user_created)
left join dbt_pii.lisbon_score_aud lsa 
	on u.user_id = lsa.user_id  
	and ou.end_time between lsa.audit_rev_timestamp and lsa.end_timestamp 
group by 1, 2
),
-- Then we group the avg number of days per month into our 4 buckets
days_buckets as (
select 
	user_created,
	case when avg(n_days) = 0 then ' not using od'
	when avg(n_days) <= 10 then '<=10'
	when avg(n_days) <= 27 then '>=11 and <= 27'
	else '>=28' end as days_bucket,
	case when avg(n_days) = 0 then 0 else avg(perc_usage) end as avg_perc_usage --excluding outliers that use od a couple of days but avg is 0
from monthly_usage mu
group by 1
)
select
	month, 
	user_created,
	days_bucket,
	case when avg_perc_usage >1 then 'in arrears'
		when days_bucket = ' not using od' then ' 0%'
		when avg_perc_usage < 0.2 then ' <=20%'
		when avg_perc_usage between 0.2 and 0.4 then '20% to 40%'
		when avg_perc_usage between 0.4 and 0.6 then '40% to 60%'
		when avg_perc_usage between 0.6 and 0.8 then '60% to 80%'
		when avg_perc_usage between 0.8 and 0.9 then '80% to 90%'
		when avg_perc_usage > 0.9 then '> 90%'
		end as usage_buckets,
	avg(rating_class) as avg_rating_class,
	avg(perc_usage) as avg_perc_usage,
	avg(n_days) as avg_n_days
from monthly_usage 
inner join days_buckets using (user_created)
group by 1, 2, 3, 4