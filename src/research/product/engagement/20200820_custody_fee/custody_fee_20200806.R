setwd('/Users/wendyvu/Documents/')
library(n26)
library(data.table)


txns <- queryDB("
--Custody Fees
drop table if exists balance;
create temp table balance as 
select 
	user_created,
	date,
	product_key_group,
	sum(balance_eur) as balance_eur
from dbt.mmb_monthly_balance_aud b 
group by 1,2,3;


drop table if exists bal_agg;
create temp table bal_agg as
select 
	b.user_created,
	b.date,
	s.n_spaces,
	round(s.amount_spaces_cents::float/100,2) as amount_spaces_bal,
	sum( case when product_key_group = 'PRIMARY' then balance_eur end ) as prim,
	coalesce(sum( case when product_key_group = 'SPACES' then balance_eur end ),0) as spaces,
	coalesce(sum( case when product_key_group = 'SAVINGS' then balance_eur end ),0) as savings,
	(prim + spaces + savings) as total_bal,
	case when total_bal >= 50000 then true else false end as is_50K
from balance b 
left join dbt.zrh_spaces s
	on b.user_created = s.user_created
	and b.date::date = last_day(s.month) 
group by 1,2,3,4;


-- txn activity and logins
create temp table activity as
select 
	user_created,
	date_trunc('months',act_date)::date as month,
	sum(n_act_txns) as n_act_txns,
	sum(n_logins) as n_logins
from dbt.zrh_act_day 
group by 1,2;

drop table if exists act_agg;
create temp table act_agg as 
select 
	user_created,
	count(distinct case when n_act_txns > 0 then month end) as n_month_act,
	avg(n_act_txns) as avg_act_txns,
	avg(n_logins) as avg_logins
from activity 
group by 1;


--Users with >50K balance for 1+ month since onboarding
--drop table if exists users50k;
--create temp table users50k as 
select z.user_created,
	date_trunc('months',z.kyc_first_completed)::date as kycc,
	case when country_tnc_legal in ('DEU', 'FRA','ITA', 'ESP', 'GBR', 'AUT') THEN country_tnc else 'RoE' end as market,
	z.closed_at,
	z.product_id,
	case when z.is_premium is true then 'premium' else 'standard' end as membership,
	case when z.product_id ilike '%business%' then 'business' else 'non_business' end as business,
	z.gender,
	z.age_group,
	z.has_overdraft_enabled,
	z.is_fraudster,
	z.is_blacklisted,
	case when z.is_expat is true then 'expat' else 'native' end as expat,
	avg_act_txns,
	avg_logins,
	n_month_act,
	date_diff('months',date_trunc('months',ft.ftmau_ts),current_date) as ft_ct_months,
	case when n_month_act > ft_ct_months then 1 else round(n_month_act::float/ft_ct_months,5) end as act_ratio, 
	count(distinct date) as n_bal_months,
	count(case when is_50k is true then date end) as n_50kbal_months, -- number of months with >=50k,
	round(n_50kbal_months::float/n_bal_months,5) as ratio_50kbal, -- ratio of (months >= 50K balance) / (total_months_bal)  
	ceil(12*ratio_50kbal) as months_year, -- number of months with 50K in a 12 month period 
	avg(n_spaces) as avg_n_spaces,
	round(avg(amount_spaces_bal)) as avg_space_bal,
	round(avg(prim)) as avg_primary,
	round(avg(total_bal)) as avg_tot_bal,
	round(stddev(total_bal),5) as sd_tot_bal 
from bal_agg a 
join (
	select 
		distinct user_created 
	from bal_agg 
	where is_50k is true
		) as b
	on a.user_created = b.user_created
join dbt.zrh_users z 
	on b.user_created = z.user_created
left join act_agg aa 
	on aa.user_created = a.user_created 
left join (select user_created, min(act_date) as ftmau_ts from dbt.zrh_act_day group by 1) as ft
	on ft.user_created = a.user_created 
where z.closed_at is null 
	and z.country_tnc_legal in ('DEU','AUT')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by 1;
                     
" , "redshift-eu")

save(txns,
     file = file.path("custody_fee_20200806.RData"))
