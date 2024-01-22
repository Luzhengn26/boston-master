with min_enabled as(
select
	user_created,
	min(end_time)::date as min_enabled
from dbt.bp_overdraft_users bou 
	where timeframe = 'day'
	and od_enabled_flag 
group by 1
), 
prev_reduced_users as (
	select
		distinct user_created,
		date_trunc('quarter', min(created)) as first_reduced_month
	from pu_overdraft_history
	where changed_by = 'monitoring'
		and created > '2021-06-01'
        and created <= '2021-12-31' 
    group by 1
),
user_base as (
	select 
		user_created,
		min_enabled,
		pru.user_created is not null as reduced_or_cancelled
		from min_enabled mg
		left join prev_reduced_users pru using (user_created)
),
n_days_in_month as ( 
	select 
		user_created,
		min_enabled,
		to_char(end_time, 'YYYY-MM') as month,
		count(*) as n_days_enabled,
		count(case when outstanding_balance_eur is not null then 1 end) as n_days_using
	from dbt.bp_overdraft_users
	inner join user_base using (user_created)
	where od_enabled_flag
		and timeframe = 'day'
		and od_enabled_flag
        and end_time between min_enabled and '2021-12-31'   
	group by 1, 2, 3
),
pd as (
	select 
		lsa.user_created,
		calculated_at as rev_timestamp,
		coalesce(lead(calculated_at - interval '0.000001 second', 1) over (partition by lsa.user_created order by calculated_at), '2100-01-01') as end_timestamp,
		pd,
		rating_class
	from etl_reporting.ls_score_aud lsa
	inner join user_base ub 
		on lsa.user_created = ub.user_created 
		and purpose = 'OVERDRAFT'
		and score_status = 'VALID'
),
write_offs as (
	select user_created,
		sum(eur_written_off) as eur_written_off,
		date_trunc('month',max(write_off_dt))::date as write_off_month
	from dbt.write_off wo  
	where reason = 'Arranged Overdraft'
	group by 1
),
transactions as (
	select 
		user_created,
		txn_month::date, 
		n_ext_total_out,
		n_ext_total_in,
		amount_cents_ext_total_out::numeric/100 as total_volume_eur_out,  
		amount_cents_ext_total_in::numeric/100 as total_volume_eur_in 
	from dbt.zrh_txn_month 
),
totals as (
select 
	user_id,
	to_char(ub.min_enabled, 'YYYY-MM') as enabled_month,
    reduced_or_cancelled,
    n_days_enabled,
    n_days_using,
	to_char(end_time, 'YYYY-MM') as month,
	od_enabled_flag,
	od_cancellation_flag,
	max_amount_cents::numeric/100 as max_amount_eur,
	outstanding_balance_eur,
	case when max_amount_eur::numeric = 0 then 0 else outstanding_balance_eur::numeric/max_amount_eur::numeric end as perc_usage,
	rating_class,
	pd,
	n_ext_total_out,
	n_ext_total_in,
	total_volume_eur_out,  
	total_volume_eur_in,
	eur_written_off
from dbt.bp_overdraft_users ou
inner join dbt.zrh_users using (user_created)
inner join user_base ub 
	on ou.user_created = ub.user_created 
	and timeframe = 'month'
	and end_time between min_enabled and '2021-12-31'  
left join n_days_in_month dim 
	on ou.user_created = dim.user_created
	and to_char(end_time, 'YYYY-MM') = month
left join pd 
	on ou.user_created = pd.user_created
	and end_time between rev_timestamp and end_timestamp 
left join write_offs wo
	on ou.user_created = wo.user_created 
	and write_off_month::date <= date_trunc('month', end_time)::date --cumulative write-offs
left join transactions t
	on ou.user_created = t.user_created 
	and txn_month::date = date_trunc('month', end_time)::date
)
select t.*, 
    gc.quarter as enabled_quarter,
    q.quarter,
    to_char(gc.end_time, 'YYYY-MM') as enabled_quarter_date,
    to_char(q.end_time, 'YYYY-MM') as quarter_date,
    datediff('month',(enabled_month|| '-'|| '01')::Date,(month|| '-'|| '01')::Date) as month_diff,
    datediff('quarter',(enabled_month|| '-'|| '01')::Date,(month|| '-'|| '01')::Date) as quarter_diff
from totals t
left join dwh_cohort_quarters gc
    on enabled_month || '-01' between gc.start_time and gc.end_time 
left join dwh_cohort_quarters q
    on month || '-01' between q.start_time and q.end_time 
order by 1,2,3,4,5,6