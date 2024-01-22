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
	date_trunc('month', end_time) as month,
	user_created,
	count(case when outstanding_balance_eur is not null then 1 end) as n_days,
	min(max_amount_eur) as min_limit,
	avg(perc_usage) as avg_perc_usage
from od_users
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
	case when avg(n_days) = 0 then 0 else avg(avg_perc_usage) end as avg_perc_usage --excluding outliers that use od a couple of days but avg is 0
from monthly_usage
group by 1
),
txn as (
select
	u.user_created,
	case when avg_perc_usage >1 then 'in arrears'
		when days_bucket = ' not using od' then '0%'
		when avg_perc_usage < 0.2 then '<=20%'
		when avg_perc_usage between 0.2 and 0.4 then '20% to 40%'
		when avg_perc_usage between 0.4 and 0.6 then '40% to 60%'
		when avg_perc_usage between 0.6 and 0.8 then '60% to 80%'
		when avg_perc_usage between 0.8 and 0.9 then '80% to 90%'
		when avg_perc_usage > 0.9 then '> 90%'
		end as usage_buckets,
	days_bucket,
    mcc_category,
	merchant_name,
	count(*) as n_txns,
    count(case when t.amount_cents > 0 then 1 end) as n_incoming_txns,
	count(case when t.amount_cents < 0 then 1 end) as n_outgoing_txns,
	sum(t.amount_cents::numeric/100) as txn_volume,
    case when sum(n_txns::numeric) over(partition by u.user_created) = 0  then 0 else n_txns::numeric/sum(n_txns::numeric) over(partition by u.user_created) end as perc_n_txns_per_user,
    case when sum(txn_volume::numeric) over(partition by u.user_created) = 0 then 0 else txn_volume::numeric/sum(txn_volume::numeric) over(partition by u.user_created) end as perc_txn_volume_per_user,
    case when sum(n_incoming_txns::numeric) over(partition by u.user_created) = 0 then 0 else n_incoming_txns::numeric/sum(n_incoming_txns::numeric) over(partition by u.user_created) end as perc_incoming_txns_per_user,
    case when sum(n_outgoing_txns::numeric) over(partition by u.user_created) =0 then 0 else n_outgoing_txns::numeric/sum(n_outgoing_txns::numeric) over(partition by u.user_created) end as perc_outgoing_txns_per_user
from od_users u
inner join days_buckets mu
	on u.user_created = mu.user_created
inner join dbt.zrh_card_transactions t
	on u.user_created = t.user_created
	and t.created::date between '2022-01-01' and '2022-06-30'
	and u.end_time = t.created::date
	and type != 'AA'
    and mcc_category in ('gambling_gaming', 'money_cash_financial')
group by 1, 2, 3, 4, 5
),
totals as (
select
	mcc_category,
	merchant_name,
	sum(n_txns) as sum_n_txns,
	count(distinct user_created) as n_users,
	row_number() over (partition by mcc_category order by sum_n_txns desc) as rn
from txn
where mcc_category in ('gambling_gaming', 'money_cash_financial')
group by 1, 2
)
select * from totals where rn <= 10
order by 1, 3 desc