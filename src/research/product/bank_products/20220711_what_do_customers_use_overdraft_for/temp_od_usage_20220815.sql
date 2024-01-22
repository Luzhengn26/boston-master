{{
  config(
    materialized = "table"
        )
}}

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
amherst as (
select
	id, 
	category, 
	created_ts,
	row_number() over (partition by id order by created_ts desc) as rn
from amh_categories
where created::date between '2022-01-01' and '2022-06-30'
),
totals as (
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
	case when t.reference_to_original_operation = 'ConsumerCredit' then 'Loan Repayment'
		when fee_label like 'memberships_%' then 'memberships' 
		when t.type = 'WEE' and fee_label is not null then fee_label
		when t.type = 'WEE' then 'Uncategorized fee'
		when payment_scheme = 'SPACES' and account_role = 'SECONDARY' then 'SPACES'
		when t.type = 'PT' then 'Card PT - ' || mcc_category
		when t.type in ('DT', 'CT', 'DD') and a.category is not null then a.category
		when t.type = 'DT' then 'Uncategorized Direct Transfer'
		when t.type = 'CT' then 'Uncategorized Credit Transfer'
		when t.type = 'DD' then 'Uncategorized Direct Debit'
		when t.type = 'FT' then 'Moneybeam'
		when t.type = 'WU' then 'Reward Transfer'
		when t.type = 'DR' then 'Direct Debit Reversal'
		when t.type = 'TUB' then 'Wise Transfer (Foreign Currency)'
		else t.type end as type,
	count(*) as n_txns,
    count(case when t.amount_cents > 0 then 1 end) as n_incoming_txns, 
	count(case when t.amount_cents < 0 then 1 end) as n_outgoing_txns,
	sum(t.abs_amount_cents::numeric/100) as txn_volume,
	sum(case when t.amount_cents > 0 then t.abs_amount_cents end) as incoming_txn_volume, 
	sum(case when t.amount_cents < 0 then t.abs_amount_cents end) as outgoing_txn_volume,
    case when sum(n_txns::numeric) over(partition by u.user_created) = 0  then 0 else n_txns::numeric/sum(n_txns::numeric) over(partition by u.user_created) end as perc_n_txns_per_user, 
    case when sum(txn_volume::numeric) over(partition by u.user_created) = 0 then 0 else txn_volume::numeric/sum(txn_volume::numeric) over(partition by u.user_created) end as perc_txn_volume_per_user, 
    case when sum(n_incoming_txns::numeric) over(partition by u.user_created) = 0 then 0 else n_incoming_txns::numeric/sum(n_incoming_txns::numeric) over(partition by u.user_created) end as perc_incoming_txns_per_user, 
    case when sum(incoming_txn_volume::numeric) over(partition by u.user_created) = 0 then 0 else incoming_txn_volume::numeric/sum(incoming_txn_volume::numeric) over(partition by u.user_created) end as perc_incoming_txn_volume_per_user, 
    case when sum(n_outgoing_txns::numeric) over(partition by u.user_created) =0 then 0 else n_outgoing_txns::numeric/sum(n_outgoing_txns::numeric) over(partition by u.user_created) end as perc_outgoing_txns_per_user, 
    case when sum(outgoing_txn_volume::numeric) over(partition by u.user_created) = 0 then 0 else outgoing_txn_volume::numeric/sum(outgoing_txn_volume::numeric) over(partition by u.user_created) end as perc_outgoing_txn_volume_per_user
from od_users u
inner join days_buckets mu
	on u.user_created = mu.user_created
inner join dbt.zrh_transactions t
	on u.user_created = t.user_created
	and txn_ts::date between '2022-01-01' and '2022-06-30'
	and u.end_time = t.txn_ts::date
	and type != 'AA'
left join dbt.zrh_card_transactions ct
	on t.txn_id = ct.id
	and created::date between '2022-01-01' and '2022-06-30'
left join dbt.ucm_fees 
	on txn_id = fee_id
left join amherst a 
	on a.id = t.txn_id
group by 1, 2, 3, 4
),
txns_per_user as (
select 
	user_created,
	sum(n_txns) as n_txns_per_user
from totals 
group by 1
)
select 
	'All usage buckets'::varchar as usage_buckets, 
	days_bucket, 
	type,
	count(distinct user_created) as n_users, 
	sum(n_txns) as n_txns,
	sum(txn_volume) as txn_volume,
	sum(incoming_txn_volume) as incoming_txn_volume, 
	sum(outgoing_txn_volume) as outgoing_txn_volume,
	sum(n_incoming_txns) as n_incoming_txns, 
	sum(n_outgoing_txns) as n_outgoing_txns,
    avg(perc_n_txns_per_user) as perc_n_txns_per_user,
    avg(perc_txn_volume_per_user) as perc_txn_volume_per_user,
    avg(perc_incoming_txns_per_user) as perc_incoming_txns_per_user,
    avg(perc_incoming_txn_volume_per_user) as perc_incoming_txn_volume_per_user,
    avg(perc_outgoing_txns_per_user) as perc_outgoing_txns_per_user,
    avg(perc_outgoing_txn_volume_per_user) as perc_outgoing_txn_volume_per_user
from totals
inner join txns_per_user using (user_created)
where n_txns_per_user >= 6 --at least 6 txns in the selected 6 months
group by 1, 2, 3
union all 
select 
	usage_buckets, 
	'All days buckets'::varchar, 
	type,
	count(distinct user_created) as n_users, 
	sum(n_txns) as n_txns,
	sum(txn_volume) as txn_volume,
	sum(incoming_txn_volume) as incoming_txn_volume, 
	sum(outgoing_txn_volume) as outgoing_txn_volume,
	sum(n_incoming_txns) as n_incoming_txns, 
	sum(n_outgoing_txns) as n_outgoing_txns,
    avg(perc_n_txns_per_user) as perc_n_txns_per_user,
    avg(perc_txn_volume_per_user) as perc_txn_volume_per_user,
    avg(perc_incoming_txns_per_user) as perc_incoming_txns_per_user,
    avg(perc_incoming_txn_volume_per_user) as perc_incoming_txn_volume_per_user,
    avg(perc_outgoing_txns_per_user) as perc_outgoing_txns_per_user,
    avg(perc_outgoing_txn_volume_per_user) as perc_outgoing_txn_volume_per_user
from totals
inner join txns_per_user using (user_created)
where n_txns_per_user >= 6 --at least 6 txns in the selected 6 months
group by 1, 2, 3
union all 
select 
	usage_buckets, 
	days_bucket, 
	type,
	count(distinct user_created) as n_users, 
	sum(n_txns) as n_txns,
	sum(txn_volume) as txn_volume,
	sum(incoming_txn_volume) as incoming_txn_volume, 
	sum(outgoing_txn_volume) as outgoing_txn_volume,
	sum(n_incoming_txns) as n_incoming_txns, 
	sum(n_outgoing_txns) as n_outgoing_txns,
    avg(perc_n_txns_per_user) as perc_n_txns_per_user,
    avg(perc_txn_volume_per_user) as perc_txn_volume_per_user,
    avg(perc_incoming_txns_per_user) as perc_incoming_txns_per_user,
    avg(perc_incoming_txn_volume_per_user) as perc_incoming_txn_volume_per_user,
    avg(perc_outgoing_txns_per_user) as perc_outgoing_txns_per_user,
    avg(perc_outgoing_txn_volume_per_user) as perc_outgoing_txn_volume_per_user
from totals
inner join txns_per_user using (user_created)
where n_txns_per_user >= 6 --at least 6 txns in the selected 6 months
group by 1, 2, 3