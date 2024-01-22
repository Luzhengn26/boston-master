setwd('/Users/wendyvu/Documents/')
library(n26)
library(data.table)


pnl_labels <- queryDB("
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
	round(sum(value::float/100) over (partition by revenue_cost),2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group != 'Onboarding' -- remove onboarding
), value_labels as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	segment,
	(product_group || '_' || product) as label,
	value_all_user,
	round(sum(value),2) as value_cluster,
	sum(value_cluster) over (partition by cluster_new, revenue_cost) as total_rev_cost,
	round(value_cluster::float/total_rev_cost,2) as perc_rev_cost
from pnl 
group by 1,2,3,4,5,6
--order by 1,3,perc_rev_cost desc
), rank as(
select *,
	row_number() over (partition by cluster_new, revenue_cost order by perc_rev_cost desc) as rank
from value_labels 
where label is not null
order by 1, 3, perc_rev_cost desc
)
select *
from rank
;
                     
" , "redshift-eu")

pnl_labels_onboarding_avg_user <- queryDB("
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
	round(p.value::float/100,2) as value,
	m.*,
	round(sum(value) over (partition by revenue_cost),2) as value_all_user
from dbt.ucm_pnl p 
left join dbt.ucm_mapping m 
	on p.label = m.label
join clusters c 
	on c.user_created = p.user_created 
where product_group = 'Onboarding' -- remove onboarding
), value_labels as (
select 
	cluster_new,
	cluster_new_size,
	revenue_cost,
	segment,
	(product_group || '_' || product) as labels,
	value_all_user,
	round(sum(value),2) as value_cluster,
	sum(value_cluster) over (partition by revenue_cost, labels) as total_rev_cost_label_all,
	sum(value_cluster) over (partition by cluster_new, revenue_cost,labels) as total_rev_cost_label_cluster,
	round(total_rev_cost_label_cluster::float/cluster_new_size) as avg_value_cluster_user,
	round(total_rev_cost_label_all::float/20000) as avg_value_all_user
	--round(value_cluster::float/total_rev_cost,2) as perc_rev_cost
from pnl 
group by 1,2,3,4,5,6
--order by 1,3,perc_rev_cost desc
)
select * 
from value_labels
order by 1,4,5;

                     
" , "redshift-eu")

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

dau_mau <- queryDB("
with cluster as ( -- from cluster groups analysis
select 
	b.user_id,
	a.cluster_new,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id 
), act as (
select 	
	c.user_id,
	u.user_created, 
	c.cluster_new,
	c.cluster_new_size,
	z.act_date,
	n_act_txns
--	n_logins
from cluster c
join dbt.zrh_users u 
	on c.user_id = u.user_id
left join dbt.zrh_act_day z
	on u.user_created = z.user_created 
	and n_act_txns > 0
), dau as (
select 
	cluster_new,
	cluster_new_size,
	act_date,
	count(*) as dau
from act 
group by 1,2,3
), avg_dau as (
select 
	cluster_new,
	cluster_new_size,
	date_trunc('months',act_date) as month,
	avg(dau) as avg_dau
from dau 
group by 1,2,3
), mau as (
select 
	cluster_new,
	cluster_new_size,
	date_trunc('months',act_date) as month,
	count(distinct user_id) as mau 
from act
group by 1,2,3
order by 1,3
)
select m.cluster_new,
	m.cluster_new_size,
	m.month::date,
	d.avg_dau,
	m.mau,
	round(d.avg_dau::float/m.mau,2) as dau_mau
from mau m 
join avg_dau d 
	on m.cluster_new = d.cluster_new
	and m.month = d.month
order by 1,3;
                   ","redshift-eu")

spend <- queryDB("
--are users spending differently? Restaurants vs grocery

with cluster as ( -- from cluster groups analysis
select 
	b.user_id,
	u.user_created,
	a.cluster_new,
	--u.country_tnc_legal as market,
	count(b.user_id) over (partition by a.cluster_new) as cluster_new_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id 
join dbt.zrh_users u 
	on b.user_id = u.user_id
), mcc as (
select 	
	c.user_id,
	c.user_created, 
	c.cluster_new,
	c.cluster_new_size, 
	date_trunc('months',z.created)::date as month,
	case when z.mcc_category in ('bakeries','bars_clubs','fast_food','restaurants') then 'restaurants_bars' else z.mcc_category end as mcc,
	count(*) as n_txn,
	round(sum(z.amount_cents_eur::float/100),2) as amount_eur
from cluster c
left join dbt.zrh_card_transactions z
	on c.user_created = z.user_created	
where type = 'PT'
group by 1,2,3,4,5,6
), mcc_piv as (
select 
	user_id,
	user_created,
	cluster_new,
	cluster_new_size,
	month,
	coalesce(sum(case when mcc = 'grocery_market' then n_txn end),0) as n_grocery,
	coalesce(sum(case when mcc = 'grocery_market' then amount_eur end),0) as amt_grocery,
	coalesce(sum(case when mcc = 'restaurants_bars' then n_txn end),0) as n_restaurant_bar,
	coalesce(sum(case when mcc = 'restaurants_bars' then amount_eur end),0) as amt_restaurant_bar
from mcc 
group by 1,2,3,4,5
)
select 
	cluster_new,
	month,
	count(distinct user_created) as users,
	round(count(distinct case when n_grocery > 0 then user_created end)::float/users,2) as grocery_users,
	round(count(distinct case when n_restaurant_bar > 0 then user_created end)::float/users,2) as restaurant_bar_users,
	round(avg(amt_grocery),2) as avg_grocery_amt,
	round(avg(amt_restaurant_bar),2) as avg_restaurant_bar_amt
from mcc_piv
group by 1,2
order by 1,2
                ","redshift-eu")

spend_type <- queryDB("
-- are users spending differently? ATM vs Card Spend
with cluster as ( -- from cluster groups analysis
select 
	b.user_id,
	u.user_created,
	a.cluster_new,
	--u.country_tnc_legal as market,
	count(b.user_id) over (partition by a.cluster_new) as cluster_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id 
join dbt.zrh_users u 
	on b.user_id = u.user_id
), card as (
select 
	c.user_id,
	c.user_created, 
	c.cluster_new,
	c.cluster_size,
	--c.market,
	date_trunc('months',z.txn_date)::date as month,
	sum(n_card_cardpresent) as n_cardpres,
	round(sum(amount_cents_card_cardpresent)::float/100,2) as amt_cardpres,
	sum(n_card_ecomm) as n_ecom,
	round(sum(amount_cents_card_ecomm)::float/100,2) as amt_ecom,
	sum(n_card_atm) as n_atm,
	round(sum(amount_cents_card_atm)::float/100,2) as amt_atm
from cluster c
left join dbt.zrh_txn_day z
	on c.user_created = z.user_created
--where c.market in ('DEU','FRA')
group by 1,2,3,4,5
)
select 
	cluster_new,
	month,
	cluster_size,
	avg(n_cardpres) as avg_cardpres,
	round(avg(amt_cardpres)) as avg_cardpres_amt,
	avg(n_ecom) as avg_ecom,
	round(avg(amt_ecom)) as avg_ecom_amt,
	avg(n_atm) as avg_atm,
	round(avg(amt_atm)) as avg_atm_amt,
	count(distinct user_created) as users,
	round(count(distinct case when n_cardpres > 0 then user_created end)::float/users,2) as cardpres_users,
	round(count(distinct case when n_ecom > 0 then user_created end)::float/users,2) as ecom_users,
	round(count(distinct case when n_atm > 0 then user_created end)::float/users,2) as atm_users
from card 
group by 1,2,3
order by 1,2,3;                      
                      
                      ","redshift-eu")

travel_overlap <- queryDB("
with cluster as ( -- from cluster groups analysis
select 
	b.user_id,
	u.user_created,
	a.cluster_new,
	(a.n_pt_inter + a.n_pt_inter_atm) as n_inter, 
	(a.pt_inter_atm_sum + a.pt_inter_sum) as inter_sum,
	--u.country_tnc_legal as market,
	count(b.user_id) over (partition by a.cluster_new) as cluster_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id 
join dbt.zrh_users u 
	on b.user_id = u.user_id
)
select 
	cluster_new,
	cluster_size,
	count(distinct case when n_inter > 0 then user_id end ) as users,
	round(users::float/cluster_size,2) as perc_users
from cluster 
group by 1,2
order by 1
limit 500;
                          ","redshift-eu")

travel_time <- queryDB("
with cluster as ( -- from cluster groups analysis
select 
	b.user_id,
	u.user_created,
	u.kyc_first_completed as kycc,
	a.cluster_new,
	(a.n_pt_inter + a.n_pt_inter_atm) as n_inter, 
	(a.pt_inter_atm_sum + a.pt_inter_sum) as inter_sum,
	--u.country_tnc_legal as market,
	count(b.user_id) over (partition by a.cluster_new) as cluster_size
from dev_dbt.user_clusters a 
join dev_dbt.user_clusters_mapping b 
	on a.id = b.id 
join dbt.zrh_users u 
	on b.user_id = u.user_id
), travel as (
select 	
	c.*,
	date_trunc('months',created)::date as month,
	count(id) as n_txn,
	round(sum(amount_cents_eur::float/100),2) as amt
from cluster c 
left join dbt.zrh_card_transactions t 
	on c.user_created = t.user_created
	and type = 'PT' 
	and card_tx_type = 'cardpresent' 
	and region_group = 'inter'
	--and mcc_category != 'utilities'
--where created between kycc and current_date - interval '420 days'
group by 1,2,3,4,5,6,7,8
order by 1,month
), travel_agg as (
select 
	user_id,
	user_created,
	cluster_new,
	cluster_size,
	n_inter,
	datediff('months',kycc, current_date - interval '420 days') as kyc_months,
	count(month) as n_months,
	round(n_months::float/kyc_months,2) as travel_ratio,
	avg(n_txn) as avg_txn_month,
	round(avg(amt),2) as avg_amt
from travel 
group by 1,2,3,4,5,6
)
select 
	cluster_new,
	cluster_size,
	avg(travel_ratio) as avg_travel_ratio,
	ceil(avg_travel_ratio*12) as avg_months_year,
	avg(case when n_months > 0 then n_months end) as avg_n_months,
	round(avg(case when n_months > 0 then avg_amt end),2) as avg_amt
	--count(distinct case when n_months > 0 then user_created end) as users,
	--round(users::float/cluster_size,2) as perc_travelers
from travel_agg 
--where n_months > 0
group by 1,2
order by 1
limit 500;                       
                       ","redshift-eu")

save(pnl_labels,
     pnl_labels_onboarding_avg_user,
     travel,
     spaces_retention,
     retention,
     pnl_retention,
     pnl_retention_membership,
     dau_mau,
     spend,
     spend_type,
     travel_overlap,
     travel_time,
     file = file.path("Mature_User_Cluster_Part2.RData"))
