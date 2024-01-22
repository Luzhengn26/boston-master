-- first we create monthly user lists for the conditions that need to be met over time
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
scores as (
select
user_id,
max(case when purpose = 'OVERDRAFT' then 1 end) as overdraft_eligible,
max(case when purpose = 'TRANSACTION_BASED_INSTALMENT_LOAN' then 1 end) as tbil_eligible
from dev_dbt.temp_tbil_elig_users_02_22
where end_time::date = '2022-04-04'
group by 1
),
/** 
----- OVERDRAFT ELIGIBLITY CRITERIA (FEB 2022)
TRACKED:
. their calculated internal credit score is 12 or below
. the accepted T&C country is Germany or Austria
. age is not less than 18
. don’t have a seized account

. customer doesn't have 2 or more direct debit reversal, due to insufficient funds, within the last 3 months
. no ongoing dunning process for this customer


NOT TRACKED:
. customer is not receiving unemployment or similar social benefit
. customer is not receiving substitute payments in case of bankrupt employers 
 */
od_eligibility as (
select 
	m.end_time,
	su.user_id,
    su.user_created,
	su.closed_at,
	su.tnc_country_group
from dbt.stg_users su
inner join dwh_cohort_months m
	on end_time between user_created and least (current_date, coalesce (su.closed_at, '2100-01-01'))
	and end_time::date = '2022-03-31'
left join drs using (user_id, end_time)
left join dunning using (user_created, end_time)
left join wo using (user_created, end_time)
inner join scores s 
	on s.user_id = su.user_id 
    and overdraft_eligible = 1 
inner join cmd_users c 
	on c.user_created = su.user_created
	and tnc_country_group in ('DEU', 'AUT') -- the accepted T&C country is Germany or Austria
	and datediff(year,birth_date,current_date) >=18 -- age isn't less than 18
inner join cr_account cr
	on su.account_id = cr.id
	and cr.status not in ('SEIZED')   -- exculde seized accounts
where coalesce(n_dr_last_3mo, 0) < 2 -- customer doesn't have 2 or more direct debit reversal, due to insufficient funds, within the last 3 months (NEEDS FIXING)
	and dunning.user_created is null -- exclude dunning users
	and wo.user_created is null -- exclude Credit write-off users
	and not is_fraudster --exclude fraudsters
group by 1, 2, 3, 4, 5
),
/** 
----- INSTALLMENT LOANS CRITERIA (FEB 2022)
TRACKED:
. has an eligible Lisbon score (in Einsteinium)
. has German T&Cs (adding specific details for other markets)
. doesn’t have an on-going dunning process
. haven’t had an installment loan written-off
 */
tbil_eligibility as (
select
	m.end_time,
	su.user_id,
    su.user_created,
	su.closed_at,
	su.tnc_country_group
from dbt.stg_users su
inner join dwh_cohort_months m
	on end_time between user_created and least (current_date, coalesce (su.closed_at, '2100-01-01'))
	and end_time >= '2022-01-01' -- Public launch was on July 2021
left join wo using (user_created, end_time)
left join dunning d using (user_created, end_time)
inner join scores s 
	on s.user_id = su.user_id 
    and tbil_eligible =1 
where wo.user_created is null --Doesn't have a Credit Write-off
	and d.user_created is null -- doesn't currently have dunning
	and ((tnc_country_group = 'DEU') -- German T&`C
	or (tnc_country_group = 'ESP' and legal_entity = 'EU' and m.end_time >= '2021-12-01') --Spain Launched in December 2021 for German IBANs
	or (tnc_country_group in ('FRA', 'ITA') and legal_entity = 'EU' and m.end_time >= '2021-12-01')) --France and Italy launched March 2022 for German IBANs but looking into eiligibility before that
	and not is_fraudster --exclude fraudsters
group by 1, 2, 3, 4, 5  
),
unions as (
select *,
    'Overdraft Eligible Users'::text as label
from od_eligibility 
union all 
select *,
    'Installment Loans Eligible Users'::text as label
from tbil_eligibility 
)
select 
	distinct
	label, 
	u.user_id,
	end_time::date, 
	tnc_country_group,    
	a.user_created is not null as is_mau
from unions u
left join dbt.zrh_user_activity_txn as a
	on a.user_created = u.user_created
	and end_time between a.activity_start and least(u.closed_at,a.activity_end)
	and activity_type = '1_tx_35'