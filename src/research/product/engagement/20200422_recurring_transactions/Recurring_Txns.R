library(n26)
library(data.table)

setwd('/Users/wendyvu/Documents/Enable_Analysis/')
kDataPath <- file.path("/Users/wendyvu/Documents/Enable_Analysis","data")

direct_debits_query <- queryDB("

with dd as (
select 
	d.user_id,
	c.completed as kycc,
	case when d.country in ('DEU', 'FRA','ITA', 'ESP', 'GBR', 'AUT') 
	then d.country else 'RoE' end as market,
	a.partner_iban,
	a.partner_name,
	a.user_certified,
	lead(a.user_certified) over (partition by d.user_id, partner_iban, partner_name order by a.user_certified) as lead_ts,
	a.bank_balance_impact as amount_euro,
	count(a.user_certified) over (partition by d.user_id, partner_iban, partner_name) as txn_cnt,
	sum(a.bank_balance_impact) over (partition by d.user_id) as total_dd_amt,
	count(a.user_certified) over (partition by d.user_id) as total_txn
	--e.closed_at
from etl_reporting.zr_transaction a 	
join cr_user_account b 
	on a.account_id = b.account_id
join cmd_kyc_process c
	on b.user_created = c.user_created
	and completed >= '2019-02-01'::date 
	and completed < '2019-03-01'::date
	--and status = 'COMPLETED'
join cmd_n26_tnc_address d 
	on b.user_id = d.user_id
join dev.
left join cmd_user_closure e 
	on b.user_created = e.user_created 
where user_certified >= '2019-02-01'::date
	and type = 'DD'
order by 1,partner_iban,partner_name, user_certified
)
select 
	user_id,
	user_certified,
	date_diff('days',user_certified::date,lead_ts::date) as date_diff,
	avg(date_diff) over (partition by user_id, partner_iban, partner_name) as mean_days,
	stddev(date_diff) over (partition by user_id,partner_iban, partner_name) as sd_days,
	amount_euro,
	avg(amount_euro) over (partition by user_id,partner_iban,partner_name) as mean_amt,
	stddev(amount_euro) over (partition by user_id,partner_iban, partner_name) as sd_amt,
	case when mean_amt = 0 then 0 else round(sd_amt/mean_amt::float,2) end as cv_amt,	
	case when mean_days = 0 then 0 else round(sd_days/mean_days::float,2) end as cv_days,
	partner_iban,
	partner_name,
	count(user_certified) over (partition by user_id,partner_iban,partner_name) as txn_cnt_filt,
	txn_cnt,
	total_dd_amt,
	total_txn
from dd 
where date_diff > 0 -- removing repeated trxns happening on the same day
order by 1,partner_iban,user_certified

" , "redshift-eu")


card_query <- queryDB("
with cards as (
select b.user_id,
	created,
	lead(a.created) over (partition by b.user_id, merchant_name order by created) as lead_ts,
	mcc,
	merchant_name,
	mcc_category,
	round(amount_cents_eur::float/100,2) as amount_euro,
	count(a.created) over (partition by b.user_id, merchant_name) as txn_cnt,
	sum(amount_euro) over (partition by b.user_id) as total_card_spend,
	count(a.created) over (partition by b.user_id) as total_txn
from dbt.zrh_card_transactions a 
join dbt.zrh_users b 
	on a.user_created = b.user_created 
	and b.kyc_first_completed between '2019-02-01' and '2019-03-01'		
where type = 'AA'
), cv as (
select 
	user_id,
	created,
	lead_ts,
	date_diff('days',created,lead_ts) as date_diff,
	avg(date_diff) over (partition by user_id, merchant_name) as mean_days,
	stddev(date_diff) over (partition by user_id,merchant_name) as sd_days,
	amount_euro,
	avg(amount_euro) over (partition by user_id,merchant_name) as mean_amt,
	stddev(amount_euro) over (partition by user_id,merchant_name) as sd_amt,
	case when mean_amt = 0 then 0 else round(sd_amt/mean_amt::float,2) end as cv_amt,	
	case when mean_days = 0 then 0 else round(sd_days/mean_days::float,2) end as cv_days,
	mcc,
	merchant_name,
	mcc_category,
	count(created) over (partition by user_id,merchant_name) as txn_cnt_filt,
	txn_cnt,
	total_card_spend,
	total_txn 
from cards 
where txn_cnt > 2 and date_diff > 0 
)
select * from cv --where ((cv_amt <= 0.8) or (cv_days <= 0.8))
" , "redshift-eu")


ct_query <- queryDB("
with ct as (
select 
	z.user_id,
	zt.id,
	zt.user_certified,
	lead(zt.user_certified) over (partition by z.user_id, zt.partner_bic order by zt.user_certified) as lead_ts,
	date_diff('days',zt.user_certified,lead_ts) as date_diff,
	round(zt.bank_balance_impact_cents::float/100,2) as amount_eur,
	zt.partner_bic,
	count(zt.user_certified) over (partition by z.user_id, zt.partner_bic) as txn_cnt,
	sum(amount_eur) over (partition by z.user_id, zt.partner_bic) as sum_ct,
	count(zt.user_certified) over (partition by z.user_id) as total_ct	
from dbt.zrh_users z 
join dbt.zr_transaction_user zt 
	on z.user_created = zt.user_created 
where type = 'CT' and zt.user_certified >= '2019-02-01'::date 
), cv as (
select 
	id,
	user_id,
	user_certified,
	date_diff,
	avg(date_diff) over (partition by user_id, partner_bic) as mean_days,
	stddev(date_diff) over (partition by user_id, partner_bic) as std_days,
	amount_eur,
	avg(amount_eur) over (partition by user_id, partner_bic) as mean_amt,
	stddev(amount_eur) over (partition by user_id, partner_bic) as std_amt,
	case when mean_amt = 0 then 0 else round(std_amt/mean_amt::float,2) end as cv_amt,	
	case when mean_days = 0 then 0 else round(std_days/mean_days::float,2) end as cv_days,
	partner_bic,
	count(user_certified) over (partition by user_id,partner_bic) as txn_cnt_filt,
	txn_cnt,
	sum_ct,
	total_ct
from ct
where txn_cnt > 2 and date_diff > 0
)
select * 
from cv 
--where ((cv_amt <= 0.5) or (cv_days <= 0.5))
order by 2,3
" , "redshift-eu")

save(direct_debits_query,
     card_query,
     ct_query,
     file = file.path("Recurring_txns.RData"))