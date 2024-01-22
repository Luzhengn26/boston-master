library(n26)
library(data.table)

kDataPath <- file.path("/Users/wendyvu/Documents/Engage_Analysis","data")

infocards <- queryDB("
-- gathering all the infocard user information
create temp table infocard_txns as 
select 
	mi.user_id, 
	zr.user_created,
	case 
        when zr.country_tnc_legal in ('DEU','AUT','FRA','ESP','ITA','GBR') then zr.country_tnc_legal
        when zr.country_tnc_legal in ('POL', 'NOR', 'SWE','DNK', 'ISL', 'LIE') then 'NON-EUR'
        when zr.country_tnc_legal is null then 'NONE'
	else 'GrE' end as market, 
	mt.name, 
	case when name = 'FIRST_ATM_TRANSACTION' then 'daily_limits' 
    	when name = 'SECOND_ATM_TRANSACTION' then 'premium_upgrade'
    	when name = 'FIRST_CARD_TRANSACTION' then 'lock_card'
    	when name = 'FIRST_TRANSFER_TRANSACTION' then 'moneybeam'
    	when name = 'FIRST_INCOME_TRANSACTION' then 'spaces'
    end as cta,
	mi.transaction_id,
	mi.created,
	mi.updated,
	mi.dismissed,
	case when ec.se_label = 'cta1' then 'PRIMARY_CLICKED'
    	when ec.se_label = 'cta2' then 'DISMISSED_CLICKED'
    	when et.se_action = 'feed.tx_infocard_viewed' and ec.se_label is null then 'VIEWED'
    	--when et.se_action = 'referral.friend_invited' then 'friend_refer'
    	else ec.se_label 
    end as action_tracked,
    ec.collector_tstamp,
  	et.se_action as action,
	ce.customer_group,
	mt.location,
	mt.priority,
	ce.created as exp_entry_ts
	--count(mt.name) over (partition by mi.user_id) as cnt_info_user
from nb_customer_experiment as ce 
join mcv_infocard mi 
	on mi.user_id = ce.customer_id 
join mcv_infocard_template mt
	on mi.template_id = mt.id
	and mt.provider = 'EARLY_ENGAGEMENT'
	and mt.name in ('SECOND_ATM_TRANSACTION','FIRST_TRANSFER_TRANSACTION','FIRST_ATM_TRANSACTION','FIRST_CARD_TRANSACTION','FIRST_INCOME_TRANSACTION')
join dbt.zrh_users zr 
	on ce.customer_id = zr.user_id 
join ksp_event_crab as ec
	on ec.se_property = mi.id
	and ec.collector_tstamp >= '2020-01-14'
join ksp_event_types as et
	on et.event_type = ec.event_type
where ce.experiment_id = 'c3d82bd9-4f83-4e06-8062-87796792bf7c';	

--friend referral 
create temp table friend_ref as 
select 
	id.user_id,
	u.user_created,
	min(ec.collector_tstamp) as friend_ts_min,
	max(ec.collector_tstamp) as friend_ts_max,
	count(*)
from ksp_event_crab ec 
join ksp_event_userid id 
	on ec.event_id = id.event_id
	and id.collector_tstamp >= '2020-01-14' 
join ksp_event_types et 
	on et.event_type = ec.event_type
join cmd_shadow_user u 
	on u.id = id.user_id 
where ec.collector_tstamp >= '2020-01-14' 
	and ec.event_type in (485,490) -- friend referral
group by 1,2;


-- info card actions table prepped
with ic as (
select 
	zr.user_id,
	zr.user_created,
	case 
        when zr.country_tnc_legal in ('DEU','AUT','FRA','ESP','ITA','GBR') then zr.country_tnc_legal
        when zr.country_tnc_legal in ('POL', 'NOR', 'SWE','DNK', 'ISL', 'LIE') then 'NON-EUR'
        when zr.country_tnc_legal is null then 'NONE'
	else 'GrE' end as market, 
	ce.customer_group,
	ce.created as exp_entry_ts,
	b.created as ic_ts,
	name,
	cta,
	dismissed,
	action_tracked,
	min(collector_tstamp) as first_action_ts
from nb_customer_experiment as ce
join dbt.zrh_users zr 
	on ce.customer_id = zr.user_id 
left join dev.infocard_txns b 
	on ce.customer_id = b.user_id
group by 1,2,3,4,5,6,7,8,9,10
--order by 1,5,6
), ic_agg as (
select 
	user_id,
	user_created,
	market,
	customer_group,
	exp_entry_ts,
	ic_ts,
	name,
	cta,
	dismissed,
	min(first_action_ts) as first_act_ts,
	sum(case when action_tracked = 'DISMISSED_CLICKED' then 1 else 0 end) as ic_dis,
	sum(case when action_tracked = 'VIEWED' then 1 else 0 end) as ic_view,
	sum(case when action_tracked = 'PRIMARY_CLICKED' then 1 else 0 end) as ic_click,
	count(name) over (partition by user_created) as ic_num
from ic 
group by 1,2,3,4,5,6,7,8,9
order by 1,4,5
), spaces as (
select 
    user_created,
    'spaces' as feature,
    min(rev_timestamp) as first_space_ts
from w_space_aud
where rev_timestamp > '2020-01-14'::date
    and status = 'ACTIVE'
    and is_primary is false
group by 1
), card as ( 
select 
	user_created, 
	case when feature = 'card_lock' then 'lock_card'
		when feature = 'card_atm_limit_change' then 'daily_limits' 
		end as feature,
	min(event_dt) as card_ts
from dbt.stg_card_settings_daily_rows 
where feature in ('card_atm_limit_change','card_lock')
	and event_dt > '2020-01-14'::date 
group by 1,2
--order by 1,2 
), upgrade as ( 
select 
	user_created,
	case when product_id in ('STANDARD','BUSINESS_CARD') then 'STANDARD' 
		else 'PREMIUM' end as membership,
	--status,
	enter_reason,
	'premium_upgrade' as feature,
	min(product_start) as product_start,
	count(*) over (partition by user_created) as cnt
	--enter_reason,
	--min(subscription_valid_from) as subscription_valid_from,
	--min(subscription_valid_until) as subscription_valid_until
from dbt.zrh_user_product
where enter_reason = 'UPGRADED'
	and product_start > '2020-01-14'::date
group by 1,2,3,4
--order by 1
), ft as (
select 
	user_created,
	min(txn_date) as first_ft_ts
from dbt.zrh_txn_day_rows 
where feature = 'n_ft'
	and txn_date > '2020-01-14'::date
group by 1
)
select 
	a.user_id,
	a.market,
	g.newsletter_opt_in,
	g.facebook_audience_consent,
	a.customer_group,
	a.name,
	a.cta,
	a.dismissed,
	a.first_act_ts,
	a.ic_dis,
	a.ic_view,
	a.ic_click,
	a.ic_num,
	a.ic_ts,
	case when b.product_start is not null then 1 else 0 end as upgrade,
	case when c.first_space_ts is not null then 1 else 0 end as spaces,
	case when d.card_ts is not null then 1 else 0 end as lock_card,
	case when e.card_ts is not null then 1 else 0 end as daily_limit,
	case when f.first_ft_ts is not null then 1 else 0 end as moneybeam,
	case when cta = 'premium_upgrade' then datediff(day,ic_ts::date,b.product_start::date) 
		else null end as upgrade_cta_mins,
	case when cta = 'spaces' then datediff(day,ic_ts::date,c.first_space_ts::date)
		else null end as spaces_cta_mins,
	case when cta = 'lock_card' then datediff(day,ic_ts::date,d.card_ts::date) 
		else null end as lock_card_mins,
	case when cta = 'daily_limits' then datediff(day,ic_ts::date,e.card_ts::date) 
		else null end as daily_limit_mins,
	case when cta = 'moneybeam' then datediff(day,ic_ts::date,f.first_ft_ts::date) 
		else null end as moneybeam_mins,
	h.friend_ts_min,
	h.friend_ts_max,
	h.count as num_friend_ref
from ic_agg a 
left join upgrade b 
	on a.user_created = b.user_created
left join spaces c 
	on a.user_created = c.user_created 
left join card d 
	on a.user_created = d.user_created
	and d.feature = 'lock_card'
left join card e
	on a.user_created = e.user_created
	and e.feature = 'daily_limits'
left join ft f 
	on f.user_created = e.user_created
join cmd_user_preferences g 
	on a.user_created = g.user_created
left join friend_ref h 
	on a.user_created = h.user_created
order by 1;

", "redshift-eu")

info_cards_txns <- queryDB("

select 
	zr.user_id,
	customer_group,
	case when date_diff('days',created::date,current_date) >= 70 then '2months' else null end as length_time,
	ceil(date_diff('days',created::date,current_date)::float/35) as period,
	ceil(date_diff('days',created::date - interval '1 days',txn_date)::float/35) as period_txn,
	sum(value) as total_txn
from nb_customer_experiment ce 
join dbt.zrh_users zr 
	on ce.customer_id = zr.user_id 
left join dbt.zrh_txn_day_rows zt 
	on zr.user_created = zt.user_created
	and feature in ('n_ext_total')
	and txn_date > '2020-01-14'::date
where experiment_id = 'c3d82bd9-4f83-4e06-8062-87796792bf7c'
	and txn_date::date >= created::date
group by 1,2,3,4,5
order by 1,5;

", "redshift-eu")

info_card_txn_triggers <- queryDB("
-- get trxn trigger data for control group                                 
with txn as (
select 
	b.user_created,
	b.user_id,
	a.customer_group,
	created,
	txn_date,
	case when feature = 'n_dt' or feature = 'n_dd' then 'trans' 
		when feature = 'n_pt' then 'card' 
		when feature = 'n_card_atm' then 'atm'
		else 'topup' end as type,
	row_number() over (partition by c.user_created, type order by txn_date) as rn
from nb_customer_experiment a 
join dbt.zrh_users b
	on a.customer_id = b.user_id
left join dbt.zrh_txn_day_rows c 
	on c.user_created = b.user_created
	and feature in ('n_pt','n_dd','n_dt','n_ext_total_in','n_card_atm')
	and txn_date >= '2020-01-14'::date
where a.experiment_id = 'c3d82bd9-4f83-4e06-8062-87796792bf7c'
	and txn_date >= created::date
order by 1,2
), trigger as (
select 
	user_id,
	customer_group,
	created,
	txn_date,
	date_diff('days',created::date,txn_date) as date_diff_ic,
	case when type = 'card' then 'FIRST_CARD_TRANSACTION'
		when type = 'topup' then 'FIRST_INCOME_TRANSACTION'
		when type = 'trans' then 'FIRST_TRANSFER_TRANSACTION'
		end as name
from txn 
where rn = 1 and type in ('card','topup','trans')
union all
select 
	user_id,
	customer_group,
	created,
	txn_date,
	date_diff('days',created::date,txn_date) as date_diff_ic,
	case when rn = 1 then 'FIRST_ATM_TRANSACTION' 
		when rn = 2 then 'SECOND_ATM_TRANSACTION'
	end as name
from txn 
where rn <= 2 and type = 'atm'
order by 1,3,4
)
select 
	user_id,
	customer_group,
	created,
	txn_date,
	date_diff_ic,
	name,
	count(name) over (partition by user_id) as ic_num
from trigger 
order by 1,3,5;

" , "redshift-eu")

save(infocard_query,
     info_cards_txns,
     info_card_txn_triggers,
     file = file.path("Engage_5infocards_20200416.RData"))
