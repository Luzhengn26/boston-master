with dunning as (
select 
	end_time,
	user_created
from la_arrears 
inner join dwh_cohort_months
	on end_time between start_date and least(coalesce(exit_date, '2100-01-01'), current_date)
	and end_time::date = '2022-03-31'
group by 1, 2
),
wo as (
select 
	end_time,
	user_created
from dbt.write_off wo 
inner join dwh_cohort_months
	on end_time between write_off_dt and current_date
	and reason in ('Arranged Overdraft', 'Credit', 'TBIL')
	and end_time::date = '2022-03-31'
group by 1, 2
),
drs as (
select 
	user_id, 
	end_time,
	count(*) as n_dr_last_3mo
from np_collection
inner join dwh_cohort_months
	on created between dateadd('month', -3, end_time) and end_time
	and end_time::date = '2022-03-31'
	and return_reason = 'INSUFFICIENT_FUNDS'
group by 1, 2
),
scores as(
select * from (
	select
        user_id,
        rating_class,
        row_number() over (partition by user_id, model_version, score_status, calculated_at::date order by calculated_at desc) as row_nu
    from
        etl_reporting.lisbon_score_audit_log lsa
	where calculated_at::date = '2022-03-31'
    and model_version = 'v2.0'
    and score_status = 'BETA'
) where row_nu = 1
),
tbil_users as (
select 
    il.user_id, 
    u.user_created,
    u.tnc_country_group,
bpu.user_id is not null as currently_using_tbil
from nh_transaction_instalment_loan il 
inner join dbt.zrh_users u using (user_id)
left join dbt.bank_products_users bpu 
    on il.user_id = bpu.user_id
    and tbil_volume > 0 
    and end_time = '2022-03-31' 
group by 1, 2, 3, 4
),
od_users as (
select  
    user_id,
    user_created,
    tnc_country_group,
    outstanding_balance_eur is not null as using_od
from dbt.bp_overdraft_users 
inner join dbt.zrh_users using (user_created)
where od_enabled_flag
    and timeframe = 'month'
    and end_time = '2022-03-31'
),
totals as (
select 
    user_id, 
    user_created,
    tnc_country_group,
    tu.user_id is not null as had_tbil_all_time,
    coalesce(currently_using_tbil, false) as currently_using_tbil,
    ou.user_id is not null as od_enabled,
    coalesce(using_od, false) as using_od
from tbil_users tu
full outer join od_users ou using (user_id, user_created, tnc_country_group)
)
select *,
    d.user_created is not null as has_dunning, 
    wo.user_created is not null as has_write_off,
    drs.user_id is not null as has_drs,
    rating_class
from totals
left join dunning d using (user_created)
left join wo using (user_created)
left join drs using (user_id)
left join scores using (user_id)