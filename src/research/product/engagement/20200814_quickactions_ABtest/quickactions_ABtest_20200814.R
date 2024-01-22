setwd('/Users/wendyvu/Documents/')
library(n26)
library(data.table)


txns <- queryDB("
--Gathering all events needed for the analysis
drop table if exists events;
create temp table events as 
select 
	user_created,
	event_dt::date as event_dt,
	case when se_action in ('actions.paymentmethod_moneybeamrequest.clicked',
							'actions.paymentmethod_moneybeam_request.clicked',
							'actions.paymentmethod_moneybeam.clicked') then 'actions.moneybeam'
		when se_action in ('actions.paymentmethod_cash_26deposit.clicked') then 'actions.cash26dep'
		when se_action in ('actions.paymentmethod_transferwise.clicked') then 'actions.tub'
		when se_action in ('actions.paymentmethod_stripe_card.clicked',
							'actions.paymentmethod_stripe_wallet.clicked') then 'actions.stripe'
		when se_action in ('actions.paymentmethod_sepa.clicked') then 'actions.sepa' --dt and ct
		when se_action in ('actions.paymentmethod_eudirectdeposit.clicked') then 'actions.dd'
		when se_action in ('feed.paymentmethod_moneybeam_request.clicked',
							'feed.paymentmethod_moneybeam.clicked') then 'feed.moneybeam'
		when se_action in ('feed.paymentmethod_cash_26deposit.clicked') then 'feed.cash26dep'
		when se_action in ('feed.paymentmethod_transferwise.clicked') then 'feed.tub' 
		when se_action in ('feed.paymentmethod_stripe_card.clicked','feed.paymentmethod_stripe_wallet.clicked') then 'feed.stripe'
		when se_action in ('feed.paymentmethod_sepa.clicked') then 'feed.sepa' --dt and ct
		when se_action in ('feed.paymentmethod_eudirectdeposit.clicked') then 'feed.dd' else se_action
		end as event_type,
	count(*) as n_event
from dbt.stg_txn_events 
where app_version like '%3.47%' 
	and se_action in ('actions.paymentmethod_moneybeamrequest.clicked', -- make sure to combined these request mb events
					'actions.paymentmethod_moneybeam_request.clicked',
					'actions.paymentmethod_moneybeam.clicked',
					--'actions.paymentmethod_cash_26.clicked',
					'actions.paymentmethod_cash_26deposit.clicked',
					'actions.paymentmethod_transferwise.clicked',
					'actions.paymentmethod_stripe_card.clicked',
					'actions.paymentmethod_stripe_wallet.clicked',
					'actions.paymentmethod_sepa.clicked',
					'actions.paymentmethod_eudirectdeposit.clicked',
					'feed.paymentmethod_moneybeam_request.clicked',
					'feed.paymentmethod_moneybeam.clicked',
					'feed.paymentmethod_cash_26deposit.clicked',
					'feed.paymentmethod_transferwise.clicked',
					'feed.paymentmethod_stripe_card.clicked',
					'feed.paymentmethod_stripe_wallet.clicked',
					'feed.paymentmethod_sepa.clicked',
					'feed.paymentmethod_eudirectdeposit.clicked',
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login'
					)
group by 1,2,3;


-- Transactions
-- make sure to only focus on cash26 deposits 

--What proportion of users completed X txns split by cohort week?
with qa as (
select user_created,
	event_dt,
	event_type,
	n_event,
	min(event_dt) over (partition by user_created) as ft_ts
from events  
where (event_type = 'feed.quickactions.viewed' or event_type = 'login')
	and event_dt between '2020-07-02'::date and '2020-07-26'::date
), cohort_groups as (
select 
	user_created,
	date_trunc('weeks',ft_ts)::date as cohort_week,
	event_dt,
	sum(case when event_type = 'login' then n_event end) as login,
	sum(case when event_type = 'feed.quickactions.viewed' then n_event end) as quickactions,
	case when quickactions is not null then 'test' else 'control' end as exp_group
from qa
where event_dt between cohort_week and cohort_week + interval '7 days' 
group by 1,2,3
), txn_events as (
select 
	c.*,
	e.event_type,
	e.event_dt,
	t.txn_date,
	n_ft,
	n_cash26_in,
	n_tub,
	n_stripetopup_in,
	(n_dt + n_ct) as n_sepa,
	n_dd,
	(n_ft + n_cash26_in + n_tub + n_stripetopup_in + n_sepa + n_dd) as n_total
	--sum(case when event_type = 'actions.moneybeam' then 1 end) as a.moneybeam,
	--sum(case when event_type = 'actions.cash26dep' then 1 end) as a.cash26dep
from cohort_groups c 
left join dbt.zrh_txn_day t 
	on c.user_created = t.user_created 
	and c.event_dt = t.txn_date
left join events e 
	on c.user_created = e.user_created 
	and c.event_dt = e.event_dt
	and event_type in ('actions.moneybeam',
						'actions.cash26dep',
						'actions.tub',
						'actions.stripe',
						'actions.sepa',
						'actions.dd',
						'feed.moneybeam',
						'feed.cash26dep',
						'feed.tub',
						'feed.stripe',
						'feed.sepa',
						'feed.dd')
--where exp_group = 'test'
)
select
	cohort_week,
	exp_group,
	round(count(distinct case when n_ft > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as ft_users, 
	round(count(distinct case when n_cash26_in > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as cash26dep_users,
	round(count(distinct case when n_tub > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as tub_users,
	round(count(distinct case when n_stripetopup_in > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as stripe_users,
	round(count(distinct case when n_sepa > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as sepa_users,
	round(count(distinct case when n_dd > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as dd_users,
	round(count(distinct case when n_total > 0 then user_created end)::float/count(distinct case when 1=1 then user_created end),5) as all_txn_users,
	count(distinct case when 1=1 then user_created end) as total_users,
	count(distinct case when n_ft > 0 then user_created end) as n_ft,
	count(distinct case when n_cash26_in > 0 then user_created end) as n_cash26,
	count(distinct case when n_tub > 0 then user_created end) as n_tub,
	count(distinct case when n_stripetopup_in > 0 then user_created end) as n_stripetopup,
	count(distinct case when n_sepa > 0 then user_created end) as n_sepa,
	count(distinct case when n_dd > 0 then user_created end) as n_dd
from txn_events 
group by 1,2
order by 1,2;
                     
" , "redshift-eu")

mau <- queryDB("
with events as (
select user_created,
	event_dt::date as date,
	se_action,
	count(*)
from dbt.stg_txn_events 
where app_version like '%3.47%'
	and event_dt::date >= '2020-07-02'::date
	and (se_action = 'feed.quickactions.viewed' or se_action = 'login')
group by 1,2,3
), events_piv as (
select 
	user_created,
	date,
	sum(case when se_action = 'login' then count end) as login,
	sum(case when se_action = 'feed.quickactions.viewed' then count end) as quickactions,
	case when quickactions is not null then 'test' else 'control' end as exp_group  
from events
group by 1,2
), mau as (
select e.*,
	z.act_date,
	n_act_txns
from events_piv e 
left join dbt.zrh_act_day z 
	on e.user_created = z.user_created
	and e.date = z.act_date 
	and n_act_txns > 0
)
select 
	date_trunc('weeks',date)::date as week,
	exp_group,
	count(distinct case when n_act_txns is not null then user_created end) as mau,
	count(distinct user_created) as total_users
from mau 
group by 1,2
order by 1,2;

", "redshift-eu")

entry <- queryDB("
--Gathering all events needed for the analysis
drop table if exists events;
create temp table events as 
select 
	user_created,
	event_dt::date as event_dt,
	case when se_action in ('actions.paymentmethod_moneybeamrequest.clicked',
							'actions.paymentmethod_moneybeam_request.clicked',
							'actions.paymentmethod_moneybeam.clicked') then 'actions.moneybeam'
		when se_action in ('actions.paymentmethod_cash_26deposit.clicked') then 'actions.cash26dep'
		when se_action in ('actions.paymentmethod_transferwise.clicked') then 'actions.tub'
		when se_action in ('actions.paymentmethod_stripe_card.clicked',
							'actions.paymentmethod_stripe_wallet.clicked') then 'actions.stripe'
		when se_action in ('actions.paymentmethod_sepa.clicked') then 'actions.sepa' --dt and ct
		when se_action in ('actions.paymentmethod_eudirectdeposit.clicked') then 'actions.dd'
		when se_action in ('feed.paymentmethod_moneybeam_request.clicked',
							'feed.paymentmethod_moneybeam.clicked') then 'feed.moneybeam'
		when se_action in ('feed.paymentmethod_cash_26deposit.clicked') then 'feed.cash26dep'
		when se_action in ('feed.paymentmethod_transferwise.clicked') then 'feed.tub' 
		when se_action in ('feed.paymentmethod_stripe_card.clicked','feed.paymentmethod_stripe_wallet.clicked') then 'feed.stripe'
		when se_action in ('feed.paymentmethod_sepa.clicked') then 'feed.sepa' --dt and ct
		when se_action in ('feed.paymentmethod_eudirectdeposit.clicked') then 'feed.dd' else se_action
		end as event_type,
	count(*) as n_event
from dbt.stg_txn_events 
where app_version like '%3.47%' 
	and se_action in ('actions.paymentmethod_moneybeamrequest.clicked', -- make sure to combined these request mb events
					'actions.paymentmethod_moneybeam_request.clicked',
					'actions.paymentmethod_moneybeam.clicked',
					--'actions.paymentmethod_cash_26.clicked',
					'actions.paymentmethod_cash_26deposit.clicked',
					'actions.paymentmethod_transferwise.clicked',
					'actions.paymentmethod_stripe_card.clicked',
					'actions.paymentmethod_stripe_wallet.clicked',
					'actions.paymentmethod_sepa.clicked',
					'actions.paymentmethod_eudirectdeposit.clicked',
					'feed.paymentmethod_moneybeam_request.clicked',
					'feed.paymentmethod_moneybeam.clicked',
					'feed.paymentmethod_cash_26deposit.clicked',
					'feed.paymentmethod_transferwise.clicked',
					'feed.paymentmethod_stripe_card.clicked',
					'feed.paymentmethod_stripe_wallet.clicked',
					'feed.paymentmethod_sepa.clicked',
					'feed.paymentmethod_eudirectdeposit.clicked',
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login'
					)
group by 1,2,3;



-- Transactions
-- make sure to only focus on cash26 deposits 

--What proportion of users completed X txns split by cohort week?
with qa as (
select user_created,
	event_dt,
	event_type,
	n_event,
	min(event_dt) over (partition by user_created) as ft_ts
from events  
where (event_type = 'feed.quickactions.viewed' or event_type = 'login')
	and event_dt between '2020-07-02'::date and '2020-07-26'::date
), cohort_groups as (
select 
	user_created,
	date_trunc('weeks',ft_ts)::date as cohort_week,
	event_dt,
	sum(case when event_type = 'login' then n_event end) as login,
	sum(case when event_type = 'feed.quickactions.viewed' then n_event end) as quickactions,
	case when quickactions is not null then 'test' else 'control' end as exp_group
from qa
where event_dt between cohort_week and cohort_week + interval '7 days' 
group by 1,2,3
), txn_events as (
select 
	c.user_created,
	c.cohort_week,
	quickactions,
	exp_group,
	coalesce(e.event_type,'other_entry') as event_type,
	e.event_dt,
	t.txn_date,
	n_ft,
	n_cash26_in,
	n_tub,
	n_stripetopup_in,
	(n_dt + n_ct) as n_sepa,
	n_dd,
	(n_ft + n_cash26_in + n_tub + n_stripetopup_in + n_sepa + n_dd) as n_total
	--sum(case when event_type = 'actions.moneybeam' then 1 end) as a.moneybeam,
	--sum(case when event_type = 'actions.cash26dep' then 1 end) as a.cash26dep
from cohort_groups c 
left join dbt.zrh_txn_day t 
	on c.user_created = t.user_created 
	and c.event_dt = t.txn_date
left join events e 
	on c.user_created = e.user_created 
	and c.event_dt = e.event_dt
	and event_type in ('actions.moneybeam',
						'actions.cash26dep',
						'actions.tub',
						'actions.stripe',
						'actions.sepa',
						'actions.dd',
						'feed.moneybeam',
						'feed.cash26dep',
						'feed.tub',
						'feed.stripe',
						'feed.sepa',
						'feed.dd')
--where exp_group = 'test'
), entry as (
select 
	cohort_week,
	exp_group,
	case when event_type ilike 'feed%' then 'feed_entry' 
		when event_type ilike 'actions%' then 'actions_entry'
		when event_type ilike 'other%' then 'other_entry' end as entry,
	count(case when n_total > 0 then user_created end) as users
from txn_events 
where exp_group = 'test' --and event_type = 'other_entry'
group by 1,2,3
order by 1,2,3
)
select *,
	sum(users) over (partition by cohort_week) as total_users,
	round(users::float/total_users,2) as percent_users
from entry
where entry != 'other_entry'
;
                 ", "redshift-eu")

stats <- queryDB("
--Gathering all events needed for the analysis
drop table if exists events;
create temp table events as 
select 
	user_created,
	collector_date::date as event_dt,
	case 
		when se_action in ('feed.recurring_transactions_viewed') then 'feed.recurring_viewed'
		when se_action in ('feed.quickactions.scheduled.clicked') then 'quickaction.recurrring_viewed'
		when se_action in ('statistics_viewed') then 'feed.statistics_viewed'
		when se_action in ('feed.quickactions.statistics.clicked') then 'quickaction.statistics_viewed'
		else se_action
		end as event_type,
	count(*) as n_event
from dbt.snowplow 
where app_version like '%3.47%' 
	and collector_date between '2020-07-02'::date and '2020-07-26'::date
	and se_action in (
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login',
					--'Recurring_SetasScheduledTransfer',
					'feed.recurring_transactions_viewed',
					'statistics_viewed'
					)
group by 1,2,3;


-- schedule payments and statistics
-- make sure to only focus on cash26 deposits 

--What proportion of users completed X txns split by cohort week?
with qa as (
select user_created,
	event_dt,
	event_type,
	n_event,
	min(event_dt) over (partition by user_created) as ft_ts
from events  
where (event_type = 'feed.quickactions.viewed' or event_type = 'login')
	and event_dt between '2020-07-02'::date and '2020-07-26'::date
), cohort_groups as (
select 
	user_created,
	date_trunc('weeks',ft_ts)::date as cohort_week,
	event_dt,
	sum(case when event_type = 'login' then n_event end) as login,
	sum(case when event_type = 'feed.quickactions.viewed' then n_event end) as quickactions,
	case when quickactions is not null then 'test' else 'control' end as exp_group
from qa
where event_dt between cohort_week and cohort_week + interval '7 days' 
group by 1,2,3
), stats_events as (
select 
	c.user_created,
	c.cohort_week,
	login,
	quickactions,
	exp_group,
	e.event_type,
	e.event_dt
from cohort_groups c 
left join dev.events e 
	on c.user_created = e.user_created 
	and c.event_dt = e.event_dt
	and e.event_type in ('feed.statistics_viewed',
						'feed.recurring_viewed',
						'quickaction.recurrring_viewed',
						'quickaction.statistics_viewed')
)
select 
	cohort_week,
	exp_group,
	count(distinct case when event_type ilike '%statistic%' then user_created end) as users_statistics,
	count(distinct case when event_type ilike '%recur%' then user_created end) as users_schedule_pay,
	count(distinct case when 1=1 then user_created end) as total_users,
	round(users_statistics::float/total_users,3) as percent_statistics,
	round(users_schedule_pay::float/total_users,3) as percent_schedule_pay
from stats_events 
group by 1,2
order by 1,2,3;
                 ","redshift-eu")

so <- queryDB("
drop table if exists dev.events;
create table events as 
select 
	user_created,
	collector_date::date as event_dt,
	case 
		when se_action in ('feed.recurring_transactions_viewed') then 'feed.recurring_viewed'
		when se_action in ('feed.quickactions.scheduled.clicked') then 'quickaction.recurrring_viewed'
		when se_action in ('statistics_viewed') then 'feed.statistics_viewed'
		when se_action in ('feed.quickactions.statistics.clicked') then 'quickaction.statistics_viewed'
		else se_action
		end as event_type,
	count(*) as n_event
from dbt.snowplow 
where app_version like '%3.47%' 
	and collector_date between '2020-07-02'::date and '2020-07-26'::date
	and se_action in (
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login',
					--'Recurring_SetasScheduledTransfer',
					'feed.recurring_transactions_viewed',
					'statistics_viewed'
					)
group by 1,2,3;

-- schedule payments and statistics
-- make sure to only focus on cash26 deposits 

--What proportion of users completed X txns split by cohort week?
with qa as (
select user_created,
	event_dt,
	event_type,
	n_event,
	min(event_dt) over (partition by user_created) as ft_ts
from events  
where (event_type = 'feed.quickactions.viewed' or event_type = 'login')
	and event_dt between '2020-07-02'::date and '2020-07-26'::date
), cohort_groups as (
select 
	user_created,
	date_trunc('weeks',ft_ts)::date as cohort_week,
	event_dt,
	sum(case when event_type = 'login' then n_event end) as login,
	sum(case when event_type = 'feed.quickactions.viewed' then n_event end) as quickactions,
	case when quickactions is not null then 'test' else 'control' end as exp_group
from qa
where event_dt between cohort_week and cohort_week + interval '7 days' 
group by 1,2,3
), so as (
select user_created,
	user_certified::date as user_certified, -- ts of the day user authorized SO
	count(distinct id) as n_so
from lr_standing_order_aud
where user_certified between '2020-07-02'::date and '2020-07-26'::date
group by 1,2
--order by 1,user_certified
), stats_events as (
select 
	c.user_created,
	c.cohort_week,
	login,
	quickactions,
	exp_group,
	e.event_type,
	e.event_dt,
	s.user_certified
from cohort_groups c
left join so s 
	on c.user_created = s.user_created 
	and c.event_dt = s.user_certified 
left join dev.events e 
	on c.user_created = e.user_created 
	and c.event_dt = e.event_dt
	and e.event_type in ('feed.statistics_viewed',
						'feed.recurring_viewed',
						'quickaction.recurrring_viewed',
						'quickaction.statistics_viewed')
)
select cohort_week,
	exp_group,
	count(distinct case when user_certified is not null then user_created end) as so_users,
	count(distinct case when 1=1 then user_created end) as total_users,
	round(so_users::float/total_users,5) as percent_users
from stats_events 
group by 1,2
order by 1,2;
              
              
              ", "redshift-eu")

entry_2 <- queryDB("
--Gathering all events needed for the analysis
drop table if exists events;
create temp table events as 
select 
	user_created,
	event_dt::date as event_dt,
	case when se_action in ('actions.paymentmethod_moneybeamrequest.clicked',
							'actions.paymentmethod_moneybeam_request.clicked',
							'actions.paymentmethod_moneybeam.clicked') then 'actions.moneybeam'
		when se_action in ('actions.paymentmethod_cash_26deposit.clicked') then 'actions.cash26dep'
		when se_action in ('actions.paymentmethod_transferwise.clicked') then 'actions.tub'
		when se_action in ('actions.paymentmethod_stripe_card.clicked',
							'actions.paymentmethod_stripe_wallet.clicked') then 'actions.stripe'
		when se_action in ('actions.paymentmethod_sepa.clicked') then 'actions.sepa' --dt and ct
		when se_action in ('actions.paymentmethod_eudirectdeposit.clicked') then 'actions.dd'
		when se_action in ('feed.paymentmethod_moneybeam_request.clicked',
							'feed.paymentmethod_moneybeam.clicked') then 'feed.moneybeam'
		when se_action in ('feed.paymentmethod_cash_26deposit.clicked') then 'feed.cash26dep'
		when se_action in ('feed.paymentmethod_transferwise.clicked') then 'feed.tub' 
		when se_action in ('feed.paymentmethod_stripe_card.clicked','feed.paymentmethod_stripe_wallet.clicked') then 'feed.stripe'
		when se_action in ('feed.paymentmethod_sepa.clicked') then 'feed.sepa' --dt and ct
		when se_action in ('feed.paymentmethod_eudirectdeposit.clicked') then 'feed.dd' else se_action
		end as event_type,
	count(*) as n_event
from dbt.stg_txn_events 
where app_version like '%3.47%' 
	and se_action in ('actions.paymentmethod_moneybeamrequest.clicked', -- make sure to combined these request mb events
					'actions.paymentmethod_moneybeam_request.clicked',
					'actions.paymentmethod_moneybeam.clicked',
					--'actions.paymentmethod_cash_26.clicked',
					'actions.paymentmethod_cash_26deposit.clicked',
					'actions.paymentmethod_transferwise.clicked',
					'actions.paymentmethod_stripe_card.clicked',
					'actions.paymentmethod_stripe_wallet.clicked',
					'actions.paymentmethod_sepa.clicked',
					'actions.paymentmethod_eudirectdeposit.clicked',
					'feed.paymentmethod_moneybeam_request.clicked',
					'feed.paymentmethod_moneybeam.clicked',
					'feed.paymentmethod_cash_26deposit.clicked',
					'feed.paymentmethod_transferwise.clicked',
					'feed.paymentmethod_stripe_card.clicked',
					'feed.paymentmethod_stripe_wallet.clicked',
					'feed.paymentmethod_sepa.clicked',
					'feed.paymentmethod_eudirectdeposit.clicked',
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login'
					)
group by 1,2,3;


with txn_events as (
select 
	e.user_created,
	coalesce(e.event_type,'other_entry') as event_type,
	e.event_dt,
	t.txn_date,
	n_ft,
	n_cash26_in,
	n_tub,
	n_stripetopup_in,
	(n_dt + n_ct) as n_sepa,
	n_dd,
	(n_ft + n_cash26_in + n_tub + n_stripetopup_in + n_sepa + n_dd) as n_total
	--sum(case when event_type = 'actions.moneybeam' then 1 end) as a.moneybeam,
	--sum(case when event_type = 'actions.cash26dep' then 1 end) as a.cash26dep
from events e   
left join dbt.zrh_txn_day t
	on e.user_created = t.user_created 
	and e.event_dt = t.txn_date
where event_type in ('actions.moneybeam',
						'actions.cash26dep',
						'actions.tub',
						'actions.stripe',
						'actions.sepa',
						'actions.dd',
						'feed.moneybeam',
						'feed.cash26dep',
						'feed.tub',
						'feed.stripe',
						'feed.sepa',
						'feed.dd')
--where exp_group = 'test'
), entry as (
select 
	date_trunc('weeks',event_dt) as week,
	case when event_type ilike 'feed%' then 'feed_entry' 
		when event_type ilike 'actions%' then 'actions_entry'
		when event_type ilike 'other%' then 'other_entry' end as entry,
	count(case when n_total > 0 then user_created end) as users
from txn_events 
group by 1,2
order by 1,2
)
select *,
	sum(users) over (partition by week) as total_users,
	round(users::float/total_users,2) as percent_users
from entry
where week >= '2020-06-29'::date
order by 1,2
;                   
                   
", "redshift-eu")

duration <- queryDB("
drop table if exists events;
create temp table events as 
select 
	user_created,
	event_dt as event_ts,
	event_dt::date as event_dt,
	case when se_action in ('actions.paymentmethod_moneybeamrequest.clicked',
							'actions.paymentmethod_moneybeam_request.clicked',
							'actions.paymentmethod_moneybeam.clicked') then 'actions.moneybeam'
		when se_action in ('actions.paymentmethod_cash_26deposit.clicked') then 'actions.cash26dep'
		when se_action in ('actions.paymentmethod_transferwise.clicked') then 'actions.tub'
		when se_action in ('actions.paymentmethod_stripe_card.clicked',
							'actions.paymentmethod_stripe_wallet.clicked') then 'actions.stripe'
		when se_action in ('actions.paymentmethod_sepa.clicked') then 'actions.sepa' --dt and ct
		when se_action in ('actions.paymentmethod_eudirectdeposit.clicked') then 'actions.dd'
		when se_action in ('feed.paymentmethod_moneybeam_request.clicked',
							'feed.paymentmethod_moneybeam.clicked') then 'feed.moneybeam'
		when se_action in ('feed.paymentmethod_cash_26deposit.clicked') then 'feed.cash26dep'
		when se_action in ('feed.paymentmethod_transferwise.clicked') then 'feed.tub' 
		when se_action in ('feed.paymentmethod_stripe_card.clicked','feed.paymentmethod_stripe_wallet.clicked') then 'feed.stripe'
		when se_action in ('feed.paymentmethod_sepa.clicked') then 'feed.sepa' --dt and ct
		when se_action in ('feed.paymentmethod_eudirectdeposit.clicked') then 'feed.dd' else se_action
		end as event_type
	--count(*) as n_event
from dbt.stg_txn_events 
where app_version like '%3.47%' 
	and se_action in ('actions.paymentmethod_moneybeamrequest.clicked', -- make sure to combined these request mb events
					'actions.paymentmethod_moneybeam_request.clicked',
					'actions.paymentmethod_moneybeam.clicked',
					--'actions.paymentmethod_cash_26.clicked',
					'actions.paymentmethod_cash_26deposit.clicked',
					'actions.paymentmethod_transferwise.clicked',
					'actions.paymentmethod_stripe_card.clicked',
					'actions.paymentmethod_stripe_wallet.clicked',
					'actions.paymentmethod_sepa.clicked',
					'actions.paymentmethod_eudirectdeposit.clicked',
					'feed.paymentmethod_moneybeam_request.clicked',
					'feed.paymentmethod_moneybeam.clicked',
					'feed.paymentmethod_cash_26deposit.clicked',
					'feed.paymentmethod_transferwise.clicked',
					'feed.paymentmethod_stripe_card.clicked',
					'feed.paymentmethod_stripe_wallet.clicked',
					'feed.paymentmethod_sepa.clicked',
					'feed.paymentmethod_eudirectdeposit.clicked',
					'feed.quickactions.viewed',
					'feed.quickactions.scheduled.clicked',
					'feed.quickactions.statistics.clicked',
					'login',
					'FCT.quoteContinue.tapped'
					)
;

with txn_events as (
select 
	t.user_created,
	e.event_type,
	e.event_ts,
	t.txn_date,
	(n_dt + n_ct) as n_sepa
from dbt.zrh_txn_day t 
join events e 
	on t.user_created = e.user_created 
	and t.txn_date = e.event_dt
	and event_type in ('login',
						'actions.sepa',
						'feed.sepa'
						--'FCT.quoteContinue.tapped'
						)
	and event_dt > '2020-07-26'::date
where n_sepa > 0
	and txn_date > '2020-07-26'::date
), filter_events as (
select 
	*,
	lead(event_type) over (partition by user_created,txn_date order by event_ts) as lead_event_type,
	case when event_type = 'login' and (lead_event_type = 'actions.sepa' or lead_event_type = 'feed.sepa') then 'keep'
		when (event_type = 'actions.sepa' or event_type = 'feed.sepa') then 'keep'
		else 'remove' end as keep_flow
from txn_events 
order by 1,3
), filter_event as (
select user_created,
	txn_date,
	n_sepa,
	event_type,
	min(event_ts) as min_ts,
	lag(min_ts) over (partition by user_created, txn_date order by min_ts) as lag_min_ts,
	datediff('seconds',lag_min_ts, min_ts) as time_duration,
	count(*) over (partition by user_created,txn_date) as n_events
from filter_events where keep_flow = 'keep'
group by 1,2,3,4
)
select 
	date_trunc('weeks',txn_date) as week,
	event_type,
	median(time_duration) as med_seconds
from filter_event
where n_events = 2 and event_type != 'login'
group by 1,2
order by 1,2
limit 500;
                    
                    ", "redshift-eu")

save(txns,
     mau,
     entry,
     stats,
     #so,
     entry_2,
     duration,
     file = file.path("quickactions_ABtest_20200814.RData"))
