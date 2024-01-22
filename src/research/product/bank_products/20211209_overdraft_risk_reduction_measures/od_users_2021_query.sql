with prev_reduced_users as (
	select
		distinct user_created
	from pu_overdraft_history
	where changed_by ilike '%monitoring%'
		and created > '2021-06-01'
    and created <= '2021-12-31' 
), 
dec_users as (
    select
        distinct user_created
    from dbt.bp_overdraft_users 
    where od_enabled_flag 
        and timeframe = 'day'
        and end_time = '2020-12-31'
    group by 1
),
user_base as (
	select 
		user_created,
		du.user_created is not null as od_enabled_in_dec,
		pru.user_created is not null as reduced_or_cancelled
	from prev_reduced_users pru
	full outer join dec_users du using (user_created)
),
n_days_in_month as ( 
	select 
		user_created,
		to_char(end_time, 'YYYY-MM') as month,
		count(*) as n_days_enabled,
		count(outstanding_balance_eur) as n_days_using
	from dbt.bp_overdraft_users
	inner join user_base using (user_created)
	where od_enabled_flag
		and timeframe = 'day'
		and od_enabled_flag
        and end_time between '2020-12-31' and '2021-12-31'   
	group by 1, 2
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
	where txn_month::date between '2020-12-31' and '2021-12-31'  
)
select 
	user_id,
	od_enabled_in_dec,
	reduced_or_cancelled,
	n_days_enabled,
	n_days_using,
	to_char(end_time, 'YYYY-MM') as month,
	od_enabled_flag,
	od_cancellation_flag,
	max_amount_cents::numeric/100 as max_amount_eur,
	outstanding_balance_eur,
	case when max_amount_eur::numeric = 0 then 0 else outstanding_balance_eur::numeric/max_amount_eur::numeric end as perc_usage, 
	n_ext_total_out,
	n_ext_total_in,
	total_volume_eur_out,  
	total_volume_eur_in
from dbt.bp_overdraft_users ou
inner join dbt.zrh_users using (user_created)
inner join user_base ub 
	on ou.user_created = ub.user_created 
	and timeframe = 'month'
	and end_time between '2020-12-31' and '2021-12-31'
left join n_days_in_month dim 
	on ou.user_created = dim.user_created
	and to_char(end_time, 'YYYY-MM') = month
left join transactions t
	on ou.user_created = t.user_created 
	and to_char(txn_month, 'YYYY-MM') = month