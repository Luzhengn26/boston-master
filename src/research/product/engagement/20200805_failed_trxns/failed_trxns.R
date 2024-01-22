setwd('/Users/wendyvu/Documents/')
library(n26)
library(data.table)


ar_reasons <- queryDB("
Select
        case when response_code = 5 then 'Do not honour'
            when response_code = 55 then 'Incorrect PIN'
            when response_code = 59 then 'Suspected fraud'
            when response_code = 54 then 'Expired card'
            when response_code = 75 then 'PIN tries'
            when reject_reason = 'INSUFFICIENT_FUNDS' then 'Insufficient funds'
            when reject_reason = 'LIMIT_EXCEEDED' then 'Card limits'
            when reject_reason = 'NOT_ACCEPTABLE_ENTRY_MODE' or reject_reason = 'NOT_ACCEPTABLE_COUNTRY' then 'Settings'
            when reject_reason = 'ACCOUNT_SEIZED' then 'Account seized'
            when reject_reason = 'CARD_DISABLED' then 'Card disabled'
            Else 'other' end
            as rejection_reason, 
        count(*) as ar_cnt
from dwh_sneaky_transaction
where type = 'AR'
and created >= current_date - INTERVAL '30 days'
group by 1
order by 2 desc;
                     
" , "redshift-eu")

dr_reasons <- queryDB("
with dr as (
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
)
select d.reason,
	count(distinct d.original_transaction_id) as dr_cnt
from dr d 
join dwh_sneaky_transaction st 
	on st.transaction_id = d.original_transaction_id
where settlement_date > current_date - interval '30 days' 
group by 1
order by 2 desc;
                     
" , "redshift-eu")

reversal_months <- queryDB("
With ar as 
(Select date_trunc('months',created) as month,
--        response_code, 
--        reject_reason, 
        count(case when type = 'AR' then id end) as ar_cnt,-- authorized rejection
        count(case when type = 'AA' then id end) as aa_cnt, -- authorized card transaction
        round(ar_cnt::float/aa_cnt,2) as percent_ar
from dwh_sneaky_transaction
where type in ('AR','AA')
and created >= current_date - INTERVAL '12 months'
group by 1
), dr as ( 
select 
	date_trunc('months',user_certified) as month,
	count(distinct case when type = 'DR' then transaction_id end) as dr_cnt,
	count(distinct case when type = 'DD' then transaction_id end) as dd_cnt,
	round(dr_cnt::float/dd_cnt,2) as percent_dr
from dwh_sneaky_transaction 
where type in ('DD','DR')
	and user_certified >= current_date - interval '12 months'
group by 1
order by 1
)
select a.month,
	ar_cnt,
	aa_cnt,
	dr_cnt,
	dd_cnt,
	percent_ar,
	percent_dr
from ar a 
join dr d 
	on a.month = d.month
order by 1;
                     
" , "redshift-eu")

reversal_users <- queryDB("
With ar as 
(Select user_created,
		date_trunc('months',created) as month,
--        response_code, 
--        reject_reason, 
        count(case when type = 'AR' then id end) as ar_cnt,-- authorized rejection
        count(case when type = 'AA' then id end) as aa_cnt, -- authorized card transaction
        case when ar_cnt = 0 or aa_cnt = 0 then 0 else round(ar_cnt::float/aa_cnt,2) end as percent_ar
from dwh_sneaky_transaction
where type in ('AR','AA')
and created >= current_date - INTERVAL '12 months'
group by 1,2
), dr as ( 
select 
	user_created,
	date_trunc('months',user_certified) as month,
	count(distinct case when type = 'DR' then transaction_id end) as dr_cnt,
	count(distinct case when type = 'DD' then transaction_id end) as dd_cnt,
	round(dr_cnt::float/dd_cnt,2) as percent_dr
from dwh_sneaky_transaction 
where type in ('DD','DR')
	and user_certified >= current_date - interval '12 months'
group by 1,2
order by 1
)
select 
	month,
	count(distinct case when ar_cnt > 0 then user_created end) as rev_users,
    count(distinct case when aa_cnt > 0 then user_created end) as all_users,
	round(rev_users::float/all_users,2) as percent_rev_users,
	avg(case when ar_cnt > 0 then ar_cnt end) as avg_rev_nonzero_users,
	avg(ar_cnt) as avg_rev_allusers,
	'AR' as type
from ar 
group by 1
union all 
select 
	month,
	count(distinct case when dr_cnt > 0 then user_created end) as rev_users,
    count(distinct case when dd_cnt > 0 then user_created end) as all_users,
	round(rev_users::float/all_users,2) as percent_rev_users,
	avg(case when dr_cnt > 0 then dr_cnt end) as avg_rev_nonzero_users,
	avg(dr_cnt) as avg_rev_allusers,
	'DR' as type
from dr 
group by 1
order by 1;
                     
" , "redshift-eu")

cluster_35daykycc_query <- queryDB("

                     
" , "redshift-eu")

save(ar_reasons,
     dr_reasons,
     reversal_months,
     reversal_users,
     file = file.path("failed_txns_20200805.RData"))


