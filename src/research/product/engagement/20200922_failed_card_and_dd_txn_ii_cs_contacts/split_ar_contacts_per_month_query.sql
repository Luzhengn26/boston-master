with ar as (
select 
'card' as transaction_group,
case when response_code = 5 then 'Do not honour'
when response_code = 55 then 'Incorrect PIN'
when response_code = 59 then 'Suspected fraud'
when response_code = 54 then 'Expired card'
when response_code = 75 then 'PIN tries'
when response_code = 57 then 'Cardholder Not allowed' 
when response_code = 65 then 'Count limit exceeded'
when response_code = 41 then 'Lost card pick up'
when (reject_reason = 'INSUFFICIENT_FUNDS' or response_code = 51) then 'Insufficient funds'
when reject_reason = 'LIMIT_EXCEEDED' then 'Card limits'
when reject_reason = 'NOT_ACCEPTABLE_ENTRY_MODE' or reject_reason = 'NOT_ACCEPTABLE_COUNTRY' then 'Settings'
when reject_reason = 'ACCOUNT_SEIZED' then 'Account seized'
when reject_reason = 'CARD_DISABLED' then 'Card disabled'
Else 'other' end
as reject_reason,
user_created,
created::date as txn_date, 
count(*) as txn_count
from dwh_sneaky_transaction st
where type = 'AR'
and created between '2020-06-01' and '2020-08-31'
group by 1, 2, 3, 4
union all 
select 
'card' as transaction_group,
'ALL AR' as reject_reason, 
user_created,
created::date as txn_date, 
count(*) as txn_count
from dwh_sneaky_transaction st
where type = 'AR'
and created between '2020-06-01' and '2020-08-31'
group by 1, 2, 3, 4
),
dr as (
select
settlement_date,
reason,
original_transaction_id,
original_settlement_amount
from ch_direct_debit_negative_response -- more recent cases are held here
union
select
settlement_date,
reason,
original_transaction_id,
amount_cents as original_settlement_amount
from ch_direct_debit_rejection_outgoing  -- some older cases held here
union
select
settlement_date,
reason,
original_transaction_id,
amount_cents as original_settlement_amount
from ch_direct_debit_reject_after_settlement_outgoing  -- some older cases held here
), failed_dd as (
select 
'direct debit' as transaction_group,
reason as reject_reason,
user_created,
created::date as txn_date,
count(distinct d.original_transaction_id) as txn_count
from  dr d 
join dwh_sneaky_transaction st
on st.transaction_id = d.original_transaction_id
where created between '2020-06-01' and '2020-08-31'
and type = 'DR'
group by 1, 2, 3, 4
union all 
select 
'direct debit' as transaction_group,
'ALL FAILED DD' as reject_reason,
user_created,
created::date as txn_date,
count(distinct d.original_transaction_id) as txn_count
from dr d
join dwh_sneaky_transaction st
on st.transaction_id = d.original_transaction_id
where created between '2020-06-01' and '2020-08-31'
and type = 'DR'
group by 1, 2, 3, 4
),
all_failed_txns as (
select * from failed_dd 
union all 
select * from ar 
),
contacts as (
select user_created, 
contact_date::date as contact_date,
count(*) as contact_count
from dbt.sf_all_contacts 
inner join dbt.zrh_users u
using(user_id)
where contact_date between '2020-06-01' and '2020-08-31'
and cs_tag in ('limits', 'pos/e_commerce', 'nfc', 'apple/google_pay', 'atm/fair_use', 'cash26', 'chargeback', 'transferwise', 'dt/standing_order', 'moneybeam', 'ct/missing_ct', 'direct_debit', 'top_up', 'fx_account_funding', 'payment_investigation')
and c_level_report = True
group by 1, 2
), sums as (
select  
to_char(txn_date, 'YYYY-MM') as txn_month,
reject_reason,
transaction_group,
count(distinct ft.user_created) as distinct_txn_users,
count(distinct c.user_created) as distinct_contact_users,
sum(txn_count) as sum_txn, 
sum(contact_count) as sum_contact,
distinct_contact_users::numeric/ distinct_txn_users::numeric as perc_users_with_cs_contact,
sum_txn::numeric/ distinct_txn_users::numeric as txns_per_user, 
sum_contact::numeric/ distinct_contact_users::numeric  as contacts_per_cs_user
from all_failed_txns ft
left join contacts c 
on ft.user_created = c.user_created 
and ft.txn_date = contact_date 
group by 1, 2, 3
)
select 
txn_month, 
reject_reason,
transaction_group,
'distinct_txn_users' as label, 
distinct_txn_users as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'distinct_contact_users' as label, 
distinct_contact_users as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'sum_txn' as label, 
sum_txn as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'sum_contact' as label, 
sum_contact as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'perc_users_with_cs_contact' as label, 
perc_users_with_cs_contact as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'txns_per_user' as label, 
txns_per_user as value
from sums
union all 
select 
txn_month, 
reject_reason,
transaction_group,
'contacts_per_cs_user' as label, 
contacts_per_cs_user  as value
from sums