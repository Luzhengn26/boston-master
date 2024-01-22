with eligible_txn as (
select  
user_id,
min(transaction_date) as first_eligible_txn, 
max(transaction_date) as last_eligible_txn, 
count(*) as eligible_transaction_count
from nh_eligible_operation 
where transaction_date <= '2021-01-31' -- locking end of the analysis
group by 1
),
info_view as (
select 
user_id, 
min(collector_date) as min_viewed,
max(collector_date) as max_viewed,
count(s.event_id) as n_view
from mcv_infocard_template t 
inner join mcv_infocard i
on (i.template_id = t.id)
and name in ('TRANSACTION_BASED_INSTALMENT_LOAN')
inner join  eligible_txn using (user_id)
left join dbt.snowplow s
on i.id = s.se_property
and s.event_type in ('-894')
and i.created between '2020-10-21' and '2021-01-31'-- start of beta and end of analysis
and collector_date between '2020-10-21' and '2021-01-31' -- start of beta and end of analysis
group by 1
), 
daily_balance as (
select 
u.user_id,
date,
sum(balance_eur) as total_balance_eur 
from dbt.mmb_daily_balance_aud 
inner join dbt.zrh_users u using(user_created)
inner join nh_whitelisted_users wu
on u.user_id = wu.user_id 
and product_key_group != 'SAVINGS' -- Excludes internal accounts 
and user_role = 'OWNER'
and date between '2020-08-01' and '2021-01-31' -- last 6 months to the end of the analysis 
group by 1, 2
), 
monthly_balance as (
select 
user_id,
date_trunc('month', date) as month,
avg(total_balance_eur) as month_avg_balance_eur,
min(total_balance_eur) as month_min_balance_eur,
max(total_balance_eur) as month_max_balance_eur
from daily_balance 
group by 1,2 
), bank_balance as (
select user_id, 
avg(month_avg_balance_eur) as avg_month_balance_eur, 
avg(month_min_balance_eur) as min_month_balance_eur, 
avg(month_max_balance_eur) as max_month_balance_eur
from monthly_balance 
group by 1
)
select 
user_id, 
u.user_created, 
kyc_first_completed,
wu.created as whitelisted_date,
coalesce(up.product_id, 'STANDARD') as product_id,
case when a.user_created is not null then true else false end as is_mau,
case when pa.user_created is not null then true else false end as has_p_account,
has_od_enabled, 
using_od_status, 
has_consumer_credit,
case when risk_provider_group like 'Group 1%' then 'Group 1'
when risk_provider_group like 'Group 2%' then 'Group 2'
when risk_provider_group like 'Group 3%' then 'Group 3'
else 'Other'
end as risk_provider_group,
wave,
first_eligible_txn, 
last_eligible_txn, 
eligible_transaction_count, 
case when iv.user_id is not null then true else false end as received_infocard,
case when n_view > 0 then true else false end as viewed_infocard,
min_viewed,
max_viewed,
n_view,
avg_month_balance_eur, 
min_month_balance_eur, 
max_month_balance_eur
from nh_whitelisted_users wu 
inner join dbt.zrh_users u using (user_id)
left join eligible_txn et using (user_id)
left join bank_balance bb using(user_id)
left join info_view iv using(user_id)
left join dbt.zrh_user_product up
on u.user_created = up.user_created
and wu.created between subscription_valid_from  and subscription_valid_until
left join pa_account_aud_anonymized pa
on u.user_created = pa.user_created
and wu.created between rev_timestamp and end_timestamp
left join dbt.bank_products_users bpu
on u.user_created = bpu.user_created
and date_trunc('month', wu.created) = date_trunc('month', bpu.end_time)
left join dbt.zrh_user_activity_txn as a
on a.user_created = u.user_created
and wu.created between a.activity_start and least(u.closed_at,a.activity_end)
and activity_type = '1_tx_35'
where wu.created <= '2021-01-31'-- end of analysis