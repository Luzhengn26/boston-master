setwd('/Users/wendyvu/Documents/Engage_Analysis/Spaces_FreemiumModel/')
library(n26)
library(data.table)


pnl_retention <- queryDB("
-- PNL cumulative sum
-- pnl considers retention (includes cohort size/mau in calculation)
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), first_ct as (
select c.*,
	min(act_date) as first_ct
	--min(z.user_certified) over (partition by z.user_created) as first_ct
from clusters c  
left join dbt.zrh_act_day z 
	on c.user_created = z.user_created
where n_act_txns > 0
group by 1,2,3,4,5,6,7
), mau as (
select 
	cluster_new,
	cluster_new_size,
	date_diff('months',date_trunc('months',first_ct::date),date_trunc('months',act.act_date::date))+1 as month,
	count(distinct user_id) as maus
from first_ct ft 
left join dbt.zrh_act_day act 
	on ft.user_created = act.user_created 
	and act.n_act_txns > 0
group by 1,2,3
), pnl as (
select p.*,
	m.product_group 
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
), pnl_margin as (
select
	c.user_id,
	c.user_created,
	product_id,
	membership,
	c.cluster_new,
	month,
	cluster_new_size,
	round(sum(value::float/100),2) as value,
	row_number() over (partition by c.user_id order by month) as period
from pnl uc 
join clusters as c 
	on uc.user_created = c.user_created
where TO_DATE(month,'YYYY-MM-DD') >= date_trunc('months',c.kycc)::date 
group by 1,2,3,4,5,6,7
), sum_margin as ( -- accounting for retention of users
select
	--product_id,
	--membership,
	cluster_new,
	--month,
	period,
	--value,
	cluster_new_size,
	sum(value) as sum_value
from pnl_margin
group by 1,2,3
), pnl_excl_onboard as (
select
	c.user_id,
	c.user_created,
	product_id,
	membership,
	c.cluster_new,
	month,
	cluster_new_size,
	round(sum(value::float/100),2) as value,
	row_number() over (partition by c.user_id order by month) as period
from pnl uc 
join clusters as c 
	on uc.user_created = c.user_created
where TO_DATE(month,'YYYY-MM-DD') >= date_trunc('months',c.kycc)::date 
	and product_group != 'Onboarding'
group by 1,2,3,4,5,6,7
), sum_pnl_excl_onboard as ( -- accounting for retention of users
select
	--product_id,
	--membership,
	cluster_new,
	--month,
	period,
	--value,
	cluster_new_size,
	sum(value) as sum_value
from pnl_excl_onboard
group by 1,2,3
)
select sm.cluster_new,
	sm.period,
	sm.cluster_new_size,
	m.month as mau_month,
	m.maus,
	sum(sm.sum_value) over (partition by sm.cluster_new order by sm.period rows unbounded preceding) as pnl_marg,
	sum(so.sum_value) over (partition by so.cluster_new order by so.period rows unbounded preceding) as pnl_exob,
	round(pnl_marg::float/sm.cluster_new_size,2) as pnl_marg_user,
	round(pnl_exob::float/so.cluster_new_size,2) as pnl_exob_user,
	round(pnl_marg::float/m.maus,2) as pnl_marg_mau,
	round(pnl_exob::float/m.maus,2) as pnl_exob_mau 
from sum_margin sm 
join sum_pnl_excl_onboard so 
	on sm.cluster_new = so.cluster_new 
	and sm.period = so.period
join mau m 
	on sm.cluster_new = m.cluster_new 
	and sm.period = m.month
order by 1,2;          
" , "redshift-eu")


pnl_retention_membership <- queryDB("
-- pnl considers retention (includes cohort size/mau in calculation) split by memberships
-- pnl considers retention (includes cohort size/mau in calculation)
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), first_ct as (
select c.*,
	min(act_date) as first_ct
	--min(z.user_certified) over (partition by z.user_created) as first_ct
from clusters c  
left join dbt.zrh_act_day z 
	on c.user_created = z.user_created
where n_act_txns > 0
group by 1,2,3,4,5,6,7
), mau as (
select 
	membership,
	cluster_new,
	cluster_new_size,
	date_diff('months',date_trunc('months',first_ct::date),date_trunc('months',act.act_date::date))+1 as month,
	count(distinct user_id) as maus
from first_ct ft 
left join dbt.zrh_act_day act 
	on ft.user_created = act.user_created 
	and act.n_act_txns > 0
group by 1,2,3,4
), pnl as (
select p.*,
	m.product_group 
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
), pnl_margin as (
select
	c.user_id,
	c.user_created,
	product_id,
	membership,
	c.cluster_new,
	month,
	cluster_new_size,
	round(sum(value::float/100),2) as value,
	row_number() over (partition by c.user_id order by month) as period
from pnl uc 
join clusters as c 
	on uc.user_created = c.user_created
where TO_DATE(month,'YYYY-MM-DD') >= date_trunc('months',c.kycc)::date 
group by 1,2,3,4,5,6,7
), sum_margin as ( -- accounting for retention of users
select
	--product_id,
	membership,
	cluster_new,
	--month,
	period,
	--value,
	cluster_new_size,
	sum(value) as sum_value
from pnl_margin
group by 1,2,3,4
), pnl_excl_onboard as (
select
	c.user_id,
	c.user_created,
	product_id,
	membership,
	c.cluster_new,
	month,
	cluster_new_size,
	round(sum(value::float/100),2) as value,
	row_number() over (partition by c.user_id order by month) as period
from pnl uc 
join clusters as c 
	on uc.user_created = c.user_created
where TO_DATE(month,'YYYY-MM-DD') >= date_trunc('months',c.kycc)::date 
	and product_group != 'Onboarding'
group by 1,2,3,4,5,6,7
), sum_pnl_excl_onboard as ( -- accounting for retention of users
select
	--product_id,
	membership,
	cluster_new,
	--month,
	period,
	--value,
	cluster_new_size,
	sum(value) as sum_value
from pnl_excl_onboard
group by 1,2,3,4
)
select sm.cluster_new,
	sm.membership,
	sm.period,
	sm.cluster_new_size,
	m.month as mau_month,
	m.maus,
	sum(sm.sum_value) over (partition by sm.cluster_new, sm.membership order by sm.period rows unbounded preceding) as pnl_marg,
	sum(so.sum_value) over (partition by so.cluster_new, so.membership order by so.period rows unbounded preceding) as pnl_exob,
	round(pnl_marg::float/sm.cluster_new_size,2) as pnl_marg_user,
	round(pnl_exob::float/so.cluster_new_size,2) as pnl_exob_user,
	round(pnl_marg::float/m.maus,2) as pnl_marg_mau,
	round(pnl_exob::float/m.maus,2) as pnl_exob_mau 
from sum_margin sm 
join sum_pnl_excl_onboard so 
	on sm.cluster_new = so.cluster_new 
	and sm.period = so.period
	and sm.membership = so.membership
join mau m 
	on sm.cluster_new = m.cluster_new 
	and sm.period = m.month
	and sm.membership = m.membership
order by 1,2,3
;  
" , "redshift-eu")

retention <- queryDB("
with clusters as ( 
select 
	b.user_id,
	c.user_created,
	a.cluster_new,
	count(b.user_id) over (partition by cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), first_ct as (
select c.*,
	min(act_date) as first_ct
	--min(z.user_certified) over (partition by z.user_created) as first_ct
from clusters c  
left join dbt.zrh_act_day z 
	on c.user_created = z.user_created
where n_act_txns > 0
group by 1,2,3,4
), mau as (
select 
	ft.user_id,
	ft.user_created,
	ft.cluster_new,
	ft.cluster_new_size,
	ft.first_ct,
	act.act_date,
	date_diff('months',date_trunc('months',first_ct::date),date_trunc('months',act.act_date::date)) as month,
	act.n_act_txns
from first_ct ft 
left join dbt.zrh_act_day act 
	on ft.user_created = act.user_created 
	and act.n_act_txns > 0
)
select 
	cluster_new,
	month,
	cluster_new_size,
	count(distinct user_id) as users,
	round(users::float/cluster_new_size,2) as retained
from mau 
where month <= 20
group by 1,2,3
order by 1,2
                     
", "redshift-eu")


spaces_txns <- queryDB("

-- Number of users that complete 0, 1-2, 3-6, 7+ space txns per month
create temp table clusters as 
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id;


with txns as ( 
select 
	user_created,
	date_trunc('months',txn_date)::date as month,
	sum(n_spaces_ct) as n_spaces_ct,
	sum(n_spaces_dt) as n_spaces_dt,
	sum(n_spaces) as n_spaces_txn 
from dbt.zrh_txn_day
where n_spaces > 0
group by 1,2
), spaces as (
select 
	s.user_created,
	p.market,
	p.product_id,
	case when product_id in ('BLACK_CARD_MONTHLY','METAL_CARD_MONTHLY','BUSINESS_BLACK','BUSINESS_METAL') then 'premium'
		else 'standard' end as membership,
	p.enter_reason,
	s.month,
	n_spaces,
	n_reg_spaces,
	n_shared_spaces,
	n_reg_spaces_funded,
	n_shared_spaces_funded,
	(n_reg_spaces_funded + n_shared_spaces_funded) as n_spaces_funded,
	coalesce(n_spaces_ct,0) as n_spaces_ct,
	coalesce(n_spaces_dt,0) as n_spaces_dt,
	coalesce(n_spaces_txn,0) as n_spaces_txn
from dbt.zrh_spaces s 
join dbt.zrh_user_product p
	on s.user_created = p.user_created
	and s.month between p.subscription_valid_from and p.subscription_valid_until
left join txns t 
	on s.user_created = t.user_created 
	and s.month = t.month 
order by 1,2
)
select 
	month,
	case when n_spaces_txn= 0 then '0'
		when n_spaces_txn between 1 and 2 then '1-2'
		when n_spaces_txn between 3 and 6 then '3-6'
		when n_spaces_txn >= 7 then '7plus' end as txns,
	count(distinct case when membership = 'standard' then user_created end) as std_users,
	count(distinct case when membership = 'premium' then user_created end) as premium_users,
	count(distinct case when membership = 'standard' and n_spaces_funded > 0 then user_created end) as std_users_funded,
	count(distinct case when membership = 'premium' and n_spaces_funded > 0 then user_created end) as premium_users_funded,
	sum(std_users_funded) over (partition by month) as tot_std_users_funded,
	sum(premium_users_funded) over (partition by month) as tot_prem_users_funded,
	sum(std_users) over (partition by month) as tot_std_users,
	sum(premium_users) over (partition by month) as tot_prem_users	
from spaces 
group by 1,2
order by 1,2;
                     
" , "redshift-eu")

spaces_accts <- queryDB("
-- # of users with X # of spaces broken split by premium and standard users
create temp table clusters as 
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id;


with txns as ( 
select 
	user_created,
	date_trunc('months',txn_date)::date as month,
	sum(n_spaces_ct) as n_spaces_ct,
	sum(n_spaces_dt) as n_spaces_dt,
	sum(n_spaces) as n_spaces_txn 
from dbt.zrh_txn_day
where n_spaces > 0
group by 1,2
), spaces as (
select 
	s.user_created,
	p.market,
	p.product_id,
	case when product_id in ('BLACK_CARD_MONTHLY','METAL_CARD_MONTHLY','BUSINESS_BLACK','BUSINESS_METAL') then 'premium'
		else 'standard' end as membership,
	p.enter_reason,
	s.month,
	case when n_spaces > 10 then 10 else n_spaces end as n_spaces,
	n_reg_spaces,
	n_shared_spaces,
	n_reg_spaces_funded,
	n_shared_spaces_funded,
	(n_reg_spaces_funded + n_shared_spaces_funded) as n_spaces_funded,
	coalesce(n_spaces_ct,0) as n_spaces_ct,
	coalesce(n_spaces_dt,0) as n_spaces_dt,
	coalesce(n_spaces_txn,0) as n_spaces_txn
from dbt.zrh_spaces s 
join dbt.zrh_user_product p
	on s.user_created = p.user_created
	and s.month between p.subscription_valid_from and p.subscription_valid_until
left join txns t 
	on s.user_created = t.user_created 
	and s.month = t.month 
order by 1,2
)
select month,
	n_spaces,
	count(distinct user_created) as all_users,
	count(distinct case when membership = 'standard' then user_created end) as std_users,
	count(distinct case when membership = 'premium' then user_created end) as premium_users,
	count(distinct case when membership = 'standard' and n_spaces_funded > 0 then user_created end) as std_users_funded,
	count(distinct case when membership = 'premium' and n_spaces_funded > 0 then user_created end) as premium_users_funded,
--	sum(std_users) over (partition by month order by n_spaces rows unbounded preceding) as std_users_cs,
	sum(premium_users) over (partition by month order by n_spaces rows unbounded preceding) as prem_users_cs,
--	sum(std_users_funded) over (partition by month order by n_spaces rows unbounded preceding) as std_users_funded_cs,
	sum(premium_users_funded) over (partition by month order by n_spaces rows unbounded preceding) as prem_users_funded_cs,
	sum(std_users_funded) over (partition by month) as tot_std_users_funded,
	sum(premium_users_funded) over (partition by month) as tot_prem_users_funded,
	sum(std_users) over (partition by month) as tot_std_users,
	sum(premium_users) over (partition by month) as tot_prem_users
from spaces
group by 1,2
order by 1,2
;                    
" , "redshift-eu")

spaces_data <- queryDB("
create temp table clusters as 
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id;


with txns as ( 
select 
	user_created,
	date_trunc('months',txn_date)::date as month,
	sum(n_spaces_ct) as n_spaces_ct,
	sum(n_spaces_dt) as n_spaces_dt,
	sum(n_spaces) as n_spaces_txn,
	sum(n_ext_total) as n_ext_total
from dbt.zrh_txn_day t 
group by 1,2
), spaces as (
select 
	u.user_id,
	s.user_created,
	c.cluster_new,
	c.cluster_new_size,
	p.market,
	p.product_id,
	case when p.product_id in ('BLACK_CARD_MONTHLY','METAL_CARD_MONTHLY','BUSINESS_BLACK','BUSINESS_METAL') then 'premium'
		else 'standard' end as membership,
	p.enter_reason,
	s.month,
	case when n_spaces > 10 then 10 else n_spaces end as n_spaces,
	n_reg_spaces,
	n_shared_spaces,
	n_reg_spaces_funded,
	n_shared_spaces_funded,
	(n_reg_spaces_funded + n_shared_spaces_funded) as n_spaces_funded,
	coalesce(n_spaces_ct,0) as n_spaces_ct,
	coalesce(n_spaces_dt,0) as n_spaces_dt,
	coalesce(n_spaces_txn,0) as n_spaces_txn,
	coalesce(n_ext_total,0) as n_ext_total
from dbt.zrh_spaces s
join dbt.zrh_users u 
	on s.user_created = u.user_created 
join dbt.zrh_user_product p
	on s.user_created = p.user_created
	and s.month between p.subscription_valid_from and p.subscription_valid_until
left join txns t 
	on s.user_created = t.user_created 
	and s.month = t.month 
left join clusters c 
	on c.user_id = u.user_id 
order by 1,2
)
select *
from spaces
where month >= current_date - interval '6 months';                         

                         " , "redshift-eu")

clusters <- queryDB("
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size,
	n_space_dt,
	n_space_ct,
	n_spaces,
	avg_space_ct,
	avg_space_dt,
	avg_spaces_bal
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
)
select * from clusters;
                    
                    
                    " , "redshift-eu")

kyc_cohort_spaces <- queryDB("
-- kycc cohort from nov - feb 
-- focus only on users that topped up at least 1 time
with kyc as (
select 
	user_id,
	user_created,
	date_trunc('months',kyc_first_completed)::date as kycc_cohort,
	count(user_created) over (partition by kycc_cohort) as kycc_cohort_size,
	product_id
from dbt.zrh_users 
where kyc_first_completed between '2019-11-01'::date and '2020-03-01'::date
), ftmau as (
select 
  k.user_id,
	k.user_created,
	k.kycc_cohort,
	kycc_cohort_size,
	case when k.product_id in ('BLACK_CARD_MONTHLY','METAL_CARD_MONTHLY','BUSINESS_BLACK','BUSINESS_METAL') then 'premium'
		else 'standard' end as membership,
	count(k.user_created) over (partition by kycc_cohort) as ftmau_size,
	min(act_date) as first_ct
from dbt.zrh_act_day z 
join kyc k 
	on z.user_created = k.user_created
where n_act_txns > 0
group by 1,2,3,4,5
), txns as ( 
select 
	f.*,
	sum(n_spaces_ct) as n_spaces_ct,
	sum(n_spaces_dt) as n_spaces_dt,
	sum(n_spaces) as n_spaces_txn,
	sum(n_ext_total) as n_ext_total
from dbt.zrh_txn_day t
join ftmau f 
	on f.user_created = t.user_created 
where txn_date between f.first_ct and f.first_ct + interval '35 days'
group by 1,2,3,4,5,6,7
), sp as (
select t.*,
	count(distinct account_id) as n_spaces_acct
from txns t  
left join w_space_aud s
	on t.user_created = s.user_created
	and s.rev_timestamp between t.first_ct and t.first_ct + interval '35 days'
	and is_primary is false
group by 1,2,3,4,5,6,7,8,9,10,11
)
select * 
from sp --where n_spaces_acct > 0
order by 1;

" , "redshift-eu")

retention_membership <- queryDB("
with clusters as ( 
select 
	b.user_id,
	c.user_created,
	a.membership,
	a.cluster_new,
	count(b.user_id) over (partition by cluster_new) as cluster_new_size,
	count(b.user_id) over (partition by cluster_new, membership) as cluster_membership_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), first_ct as (
select c.*,
	min(act_date) as first_ct
	--min(z.user_certified) over (partition by z.user_created) as first_ct
from clusters c  
left join dbt.zrh_act_day z 
	on c.user_created = z.user_created
where n_act_txns > 0
group by 1,2,3,4,5,6
), mau as (
select 
	ft.user_id,
	ft.user_created,
	ft.membership,
	ft.cluster_new,
	ft.cluster_new_size,
	ft.cluster_membership_size,
	ft.first_ct,
	act.act_date,
	date_diff('months',date_trunc('months',first_ct::date),date_trunc('months',act.act_date::date)) as month,
	act.n_act_txns
from first_ct ft 
left join dbt.zrh_act_day act 
	on ft.user_created = act.user_created 
	and act.n_act_txns > 0
)
select 
	cluster_new,
	membership,
	month,
	cluster_new_size,
	cluster_membership_size,
	count(distinct user_id) as users,
	round(users::float/cluster_membership_size,2) as retained
from mau 
where month <= 20
group by 1,2,3,4,5
order by 1,2,3;                                
                                
" , "redshift-eu")


spaces_retention <- queryDB("
with clusters as ( 
select 
	b.user_id,
	c.user_created,
	a.membership,
	a.cluster_new,
	count(b.user_id) over (partition by cluster_new) as cluster_new_size,
	count(b.user_id) over (partition by cluster_new, membership) as cluster_membership_size,
	case when n_space_ct > 0 then 'space_user' else 'non_space_user' end as space_cat,
	count(b.user_id) over (partition by cluster_new,space_cat) as cluster_space_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), first_ct as (
select c.*,
	min(act_date) as first_ct
	--min(z.user_certified) over (partition by z.user_created) as first_ct
from clusters c  
left join dbt.zrh_act_day z 
	on c.user_created = z.user_created
where n_act_txns > 0
group by 1,2,3,4,5,6,7,8
), mau as (
select 
	ft.user_id,
	ft.user_created,
	ft.membership,
	ft.cluster_new,
	ft.cluster_new_size,
	ft.space_cat,
	ft.cluster_space_size,
	ft.first_ct,
	act.act_date,
	date_diff('months',date_trunc('months',first_ct::date),date_trunc('months',act.act_date::date)) as month,
	act.n_act_txns
from first_ct ft 
left join dbt.zrh_act_day act 
	on ft.user_created = act.user_created 
	and act.n_act_txns > 0
)
select 
	cluster_new,
	space_cat,
	month,
	cluster_new_size,
	cluster_space_size,
	count(distinct user_id) as users,
	round(users::float/cluster_space_size,2) as retained
from mau 
where month <= 20
group by 1,2,3,4,5
order by 1,2,3;
                            
", "redshift-eu")

travel <- queryDB("
drop table if exists clusters;
create temp table clusters as 
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id;
	
drop table if exists mau;
create temp table mau as 
select user_created,
	date_trunc('months',act_date)::date as month,
	sum(n_act_txns) as n_act_txns 
from dbt.zrh_act_day 
group by 1,2;

drop table if exists txns;
create temp table txns as 
select
	c.user_id,
	c.user_created,
	c.cluster_new,
	c.cluster_new_size,
	date_trunc('months',ct.created)::date as month,
	ct.type,
	ct.card_tx_type,
	ct.region_group,
	case when (card_tx_type = 'cardpresent' or card_tx_type = 'atm') and region_group = 'intra' then 'intra' 
		when (card_tx_type = 'cardpresent' or card_tx_type = 'atm') and region_group = 'inter' then 'inter'
		when (card_tx_type = 'cardpresent' or card_tx_type = 'atm') and region_group = 'dom' then 'dom'
		when card_tx_type = 'ecomm' then 'ecomm' end as spend_type,
	m.n_act_txns,
	count(*) as txn_count,
	round(sum(ct.amount_cents_eur::float/100),2) as amount_eur
from clusters c 
left join dbt.zrh_card_transactions ct 
	on c.user_created = ct.user_created 
	and created between '2018-11-01'::date and '2020-08-01'
	and type = 'PT' 
	--and c.cluster_new in (4,5)
left join mau m 
	on m.user_created = ct.user_created 
	and m.month = date_trunc('months',ct.created)::date
group by 1,2,3,4,5,6,7,8,9,10
order by 1,5;

with travel as (
select 
	user_id,
	user_created,
	cluster_new,
	cluster_new_size,
	month,
	n_act_txns,
	coalesce(sum(case when spend_type = 'inter' then txn_count end),0) as inter,
	coalesce(sum(case when spend_type = 'intra' then txn_count end),0) as intra,
	coalesce(sum(case when spend_type = 'dom' then txn_count end),0) as dom,
	coalesce(sum(case when spend_type = 'ecomm' then txn_count end),0) as ecomm,
	count(user_id) over (partition by cluster_new, month) as mau,
	case when inter > 0 then 'inter' -- indicates if the user had any international txns
		else 'no_inter' 
		end as inter_cat,
	case when intra > 0 then 'intra'-- indicates if the user had any intra-national txns
		else 'no_intra'
		end as intra_cat
from txns 
--where cluster_new in (1,2,3,4,5) 
group by 1,2,3,4,5,6
order by 1, 5 
)
select 
	cluster_new,
	cluster_new_size,
	mau,
	month,
	avg(n_act_txns) as avg_act_txns,
	avg(inter) as avg_inter,
	avg(intra) as avg_intra,
	avg(dom) as avg_dom,
	avg(ecomm) as avg_ecom,
	round(count(distinct case when inter_cat = 'inter' then user_id END)::float/mau,2) as inter_user,
	round(count(distinct case when inter_cat = 'no_inter' then user_id end)::float/mau,2) as no_inter_user,
	round(count(distinct case when intra_cat = 'intra' then user_id END)::float/mau,2) as intra_user,
	round(count(distinct case when intra_cat = 'no_intra' then user_id end)::float/mau,2) as no_intra_user
from travel 
where n_act_txns is not null
group by 1,2,3,4
order by 1,4;
                  
                  ","redshift-eu")


pnl_segment <- queryDB("
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), pnl as (
select
	c.user_id,
	c.membership,
	c.cluster_new,
	c.cluster_new_size,
	p.month,
	p.value,
	m.*,
	round(sum(value::float/100) over (partition by revenue_cost, segment)/20000,2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group != 'Onboarding' -- remove onboarding
), sum_pnl as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	--product,
	segment,
	value_all_user,
	round(sum(value::float/100)/cluster_new_size,2) as value_user
from pnl 
group by 1,2,3,4,5
order by 1,2,3,4
)
select *,
	sum(value_user) over(partition by cluster_new, revenue_cost) as total_value_user,
	round(value_user::float/total_value_user,2) as percent_total,
	sum(value_user) over(partition by cluster_new) as total_net_value_user
from sum_pnl --where revenue_cost = 'Revenue'
order by 1,2,3,4;
                       ","redshift-eu")

pnl_onboarding <- queryDB("
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), pnl as (
select
	c.user_id,
	c.membership,
	c.cluster_new,
	c.cluster_new_size,
	p.month,
	p.value,
	m.*,
	round(sum(value::float/100) over (partition by revenue_cost, segment)/20000,2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group = 'Onboarding' -- remove onboarding
), sum_pnl as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	--product,
	segment,
	value_all_user,
	round(sum(value::float/100)/cluster_new_size,2) as value_user
from pnl 
group by 1,2,3,4,5
order by 1,2,3,4
)
select *,
	sum(value_user) over(partition by cluster_new, revenue_cost) as total_value_user,
	round(value_user::float/total_value_user,2) as percent_total,
	sum(value_user) over(partition by cluster_new) as total_net_value_user
from sum_pnl --where revenue_cost = 'Revenue'
order by 1,2,3,4;
                          ","redshift-eu")

pnl_segment_label <- queryDB("
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), pnl as (
select
	c.user_id,
	c.membership,
	c.cluster_new,
	c.cluster_new_size,
	p.month,
	p.value,
	m.*,
	round(sum(value::float/100) over (partition by revenue_cost, segment)/20000,2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group != 'Onboarding' -- remove onboarding
), sum_pnl as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	--product,
	segment,
	label,
	value_all_user,
	round(sum(value::float/100)/cluster_new_size,2) as value_user
from pnl 
group by 1,2,3,4,5,6
order by 1,2,3,4
), label as (
select *,
	sum(value_user) over(partition by cluster_new, revenue_cost) as total_value_user,
	round(value_user::float/total_value_user,2) as percent_total,
	sum(value_user) over(partition by cluster_new) as total_net_value_user 
from sum_pnl --where revenue_cost = 'Revenue'
order by 1,2,3,4,7
)
select *,
row_number() over (partition by cluster_new,revenue_cost,segment order by percent_total desc) as rank_label
from label 
--where revenue_cost = 'Revenue'
order by 1,2,3,4, rank_label;
                             
", "redshift-eu")

pnl_segment_label_onboarding <- queryDB("
with clusters as (
select 
	b.user_id,
	c.user_created,
	a.kycc,
	a.membership,
	a.product_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id
join dbt.zrh_users c 
	on b.user_id = c.user_id
), pnl as (
select
	c.user_id,
	c.membership,
	c.cluster_new,
	c.cluster_new_size,
	p.month,
	p.value,
	m.*,
	round(sum(value::float/100) over (partition by revenue_cost, segment)/20000,2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group = 'Onboarding' -- remove onboarding
), sum_pnl as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	--product,
	segment,
	label,
	value_all_user,
	round(sum(value::float/100)/cluster_new_size,2) as value_user
from pnl 
group by 1,2,3,4,5,6
order by 1,2,3,4
), label as (
select *,
	sum(value_user) over(partition by cluster_new, revenue_cost, segment) as total_value_user,
	case when value_user = 0 or total_value_user = 0 then 0 else round(value_user::float/total_value_user,2) end as percent_total,
	sum(value_user) over(partition by cluster_new) as total_net_value_user 
from sum_pnl --where revenue_cost = 'Revenue'
order by 1,2,3,4,7
), rank as (
select *,
row_number() over (partition by cluster_new,revenue_cost,segment order by percent_total desc) as rank_label
from label 
--where revenue_cost = 'Revenue'
order by 1,2,3,4, rank_label
)
select * 
from rank 
--where rank_label < 3 
--order by 3,4,5,1;
                                        
", "redshift-eu")

save(pnl_retention,
     pnl_retention_membership,
     pnl_segment,
     pnl_onboarding,
     pnl_segment_label,
     retention,
     retention_membership,
     spaces_txns,
     spaces_accts,
     spaces_data,
     clusters,
     kyc_cohort_spaces,
     spaces_retention,
     travel,
     file = file.path("spaces_freemium.RData"))
