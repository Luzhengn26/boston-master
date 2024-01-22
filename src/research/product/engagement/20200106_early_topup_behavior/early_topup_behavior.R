#setwd('/Users/wendyvu/Documents/transaction_tag_usage/')
library(n26)
library(data.table)


txns <- queryDB("

drop table if exists kycc;
create temp table kycc as
select 
	k.user_created,
	k.kyc_first_completed as kycc,
	c.closed_at
from dbt.zrh_users k 
left join cmd_user_closure c 
	on k.user_created = c.user_created
where kycc between '2019-01-01' and '2019-03-31'
	and c.closed_at is null 
order by 1;

-- measure of activity: MAU,
drop table if exists wau; 
create temp table wau as 
select 
	t.user_created,
	k.kycc,
	t.activity_type,
	datediff('days',t.activity_start,t.activity_end) as days_wau,
	sum(days_wau) over (partition by t.user_created) as days,
	ceil(days/7) as weeks
from kycc k 
join dbt.zrh_user_activity_txn t 
	on k.user_created = t.user_created
	and activity_type = '1_tx_7'
where t.activity_start between k.kycc and k.kycc + interval '210 days' 
group by 1,2,3,4
order by 1;

create temp table wau1 as 
select 
	user_created,
	weeks
from wau
group by 1,2;


drop table if exists mau_month;
create temp table mau_month as 
select 
	k.user_created,
	k.kycc,
	z.type,
	z.user_certified,
	datediff('days',k.kycc,z.user_certified) as days_kycc_ct,
	ceil(days_kycc_ct::float/35) as period,
	coalesce((z.type = 'WEE'
		or z.reference_to_original_operation ilike '%dash26%'
		or z.payment_scheme = 'SPACES'),false) as internal_txn_flg
from kycc k 
join dbt.zrh_transaction_user z
	on k.user_created = z.user_created and z.type = 'CT'
where z.user_certified between k.kycc and k.kycc + interval '210 days'
	and internal_txn_flg = 'false'
order by 1,5;

drop table if exists mau_period;
create temp table mau_period as
select 
	user_created,
	kycc,
	period,
	count(period) over (partition by user_created) as period_cnt
from mau_month
group by 1,2,3
order by 1,3;

create temp table mau_period1 as 
select 
	user_created,
	period_cnt
from mau_period
group by 1,2;


-- amount of first top up and days to first top up and number of ext CTs
drop table if exists ct_txns;
create temp table ct_txns as 
select 
	k.user_created,
	k.kycc,
	z.type,
	z.user_certified,
	datediff('days',k.kycc,z.user_certified) as days_kycc_ct,
	row_number() over (partition by z.user_created order by user_certified) as rn,
	count(z.user_certified) over (partition by z.user_created) as tot_ext_ct,
	z.bank_balance_impact_cents::float/100 as amount_euro,
	coalesce((z.type = 'WEE'
		or z.reference_to_original_operation ilike '%dash26%'
		or z.payment_scheme = 'SPACES'),false) as internal_txn_flg
--	coalesce((z.type = 'WEE'
--		or z.reference_to_original_operation ilike '%dash26%'),false) as n26_init_txn_flg
from kycc k 
join dbt.zrh_transaction_user z
	on k.user_created = z.user_created and z.type = 'CT'
where z.user_certified between k.kycc and k.kycc + interval '210 days'
	and internal_txn_flg = 'false'
;
	
create temp table ct_txns_1 as 
select *
from ct_txns 
where rn = 1
order by 1;


-- number of total ext txns
drop table if exists txns;
create temp table txns as 
select 
	k.user_created,
	k.kycc,
	z.type,
	z.user_certified,
	count(z.user_certified) over (partition by z.user_created) as tot_ext_txn,
	z.bank_balance_impact_cents::float/100 as amount_euro,
	coalesce((z.type = 'WEE'
		or z.reference_to_original_operation ilike '%dash26%'
		or z.payment_scheme = 'SPACES'),false) as internal_txn_flg
--	coalesce((z.type = 'WEE'
--		or z.reference_to_original_operation ilike '%dash26%'),false) as n26_init_txn_flg
from kycc k 
join dbt.zrh_transaction_user z
	on k.user_created = z.user_created
where z.user_certified between k.kycc and k.kycc + interval '210 days'
	and internal_txn_flg = 'false';
	
	
-- number of ct's and pt's trxn and avg,sum,cumsum amounts within 35 days
-- note there are WU's in this query
drop table if exists ext_txns35;
create temp table ext_txns35 as 
select 
	k.user_created,
	k.kycc,
	z.type,
	z.user_certified,
	row_number() over (partition by z.user_created, z.type order by user_certified) as rn,
	datediff('days',k.kycc,z.user_certified) as days_kycc_ct,
	count(z.user_certified) over (partition by z.user_created) as tot_ext_txn35,
	z.bank_balance_impact_cents::float/100 as amount_euro,
	avg(amount_euro) over (partition by z.user_created,z.type) as avg_amount,
	sum(amount_euro) over (partition by z.user_created,z.type) as sum_amount,
	sum(amount_euro) over (partition by z.user_created,z.type order by z.user_certified rows unbounded preceding) as cum_sum,
	coalesce((z.type = 'WEE'
		or z.reference_to_original_operation ilike '%dash26%'
		or z.payment_scheme = 'SPACES'),false) as internal_txn_flg
--	coalesce((z.type = 'WEE'
--		or z.reference_to_original_operation ilike '%dash26%'),false) as n26_init_txn_flg
from kycc k 
join dbt.zrh_transaction_user z
	on k.user_created = z.user_created
where z.user_certified between k.kycc and k.kycc + interval '35 days'
	and internal_txn_flg = 'false';
	
drop table if exists ext_txn35_1;
create temp table ext_txn35_1 as 
select 
	user_created,
	tot_ext_txn35
from ext_txns35
group by 1,2;


-- first and last CT amounts 
-- are the first CT's generally less than the last? 
drop table if exists ct_35;
create temp table ct_35 as 
select 
	user_created,
	rn,
	amount_euro,
	first_value(amount_euro) over (partition by user_created 
				order by rn rows between unbounded preceding and unbounded following) as first_ct_35, 
	nth_value(amount_euro,2) over (partition by user_created 
				order by rn rows between unbounded preceding and unbounded following) as sec_ct_35,
	count(rn) over (partition by user_created) as tot_ct_35,
	sum(amount_euro) over (partition by user_created) as sum_amount_ct_35
--	case when first_ct_35 between 0 and 100 then '0_100'
--		when first_ct_35 between 101 and 300 then '101_300'
--		when first_ct_35 between 301 and 500 then '301_500'
--		when first_ct_35 between 501 and 1000 then '501_1000'
--		when first_ct_35 > 1000 then '1000plus' 
--		when first_ct_35 is null then null end as first_ct_35cat,
--	case when sec_ct_35 between 0 and 500 then '0_100'
--		when sec_ct_35 between 101 and 300 then '101_300'
--		when sec_ct_35 between 301 and 500 then '301_500'
--		when sec_ct_35 between 501 and 1000 then '501_1000'
--		when sec_ct_35 > 1000 then '1000plus' 
--		when sec_ct_35 is null then null end as sec_ct_35cat	
from ext_txns35 
where type = 'CT' 
order by 1,rn;


drop table if exists ct_35_1;
create temp table ct_35_1 as 
select 
	user_created,
	first_ct_35,
	sec_ct_35,
	tot_ct_35,
	sum_amount_ct_35
from ct_35 
group by 1,2,3,4,5;


drop table if exists signup;
create temp table signup as 
select 
	user_created,
	kycc,
	datediff('hours',user_created, kycc) as hrs_signup_kycc
from kycc; 


select 
	t.user_created,
	t.kycc,
	date_trunc('month',t.kycc)::date as cohort,
	t.days_kycc_ct,
	t.amount_euro as first_ct_euro,
	t.rn,
	t.tot_ext_ct,
	tt.tot_ext_txn,
	w.weeks as weeks_wau,
	m.period_cnt as mau_periods,
	c.tot_ct_35,
	c.first_ct_35,
	c.sec_ct_35,
	c.sum_amount_ct_35,
	tot_ext_txn35,
	hrs_signup_kycc
from ct_txns_1 t 
join wau1 w 
	on w.user_created = t.user_created
join mau_period1 m 
	on m.user_created = t.user_created 
join (select user_created, tot_ext_txn from txns group by 1,2) tt 
	on tt.user_created = t.user_created
left join ct_35_1 c 
	on c.user_created = t.user_created
left join ext_txn35_1 e 
	on e.user_created = t.user_created
join signup s 
	on s.user_created = t.user_created 
where rn = 1
order by 1
;

" , "redshift-eu")

save(txns,
     file = file.path("early_topup_behavior.RData"))