with lisbon_scores as(
select * from (
	select
        user_id,
        row_number() over (partition by user_id, model_version, score_status, calculated_at::date order by calculated_at desc) as row_nu
    from
        etl_reporting.lisbon_score_audit_log lsa
    inner join dbt.zrh_users using (user_id)
	where calculated_at::date = '2022-04-04'
	    and model_version = 'v2.0'
	    and score_status = 'BETA'
	    and ((tnc_country_group != 'FRA' and rating_class <= 12) or (tnc_country_group = 'FRA' and rating_class <= 9))
) where row_nu = 1
),
tbil_es_scores as (
select
	distinct user_id
from dev_dbt.temp_tbil_elig_users_02_22
where purpose = 'TRANSACTION_BASED_INSTALMENT_LOAN'
	and end_time::date = '2022-04-04'
),
od_es_scores as (
select
	distinct user_id
from dev_dbt.temp_tbil_elig_users_02_22
where purpose = 'OVERDRAFT'
	and end_time::date = '2022-04-04'
)
select
	'TBIL' as product,
	case when ls.user_id is null then 'Missing in Lisbon'
		else 'Not Missing in Lisbon' end as ls_status,
	case when es.user_id is null then 'Missing in Einsteinium'
		else 'Not Missing in Einsteinium' end as es_status,
	count(*) as n_users,
	round(n_users::numeric/ sum(n_users) over(),3)*100 as perc_users
from lisbon_scores ls
full outer join tbil_es_scores es using (user_id)
group by 1, 2, 3
union all
select
	'Overdraft' as product,
	case when ls.user_id is null then 'Missing in Lisbon'
		else 'Not Missing in Lisbon' end as ls_status,
	case when es.user_id is null then 'Missing in Einsteinium'
		else 'Not Missing in Einsteinium' end as es_status,
	count(*) as n_users,
	round(n_users::numeric/ sum(n_users) over(),3)*100 as perc_users
from lisbon_scores ls
full outer join od_es_scores es using (user_id)
group by 1, 2, 3
order by 1, 4 desc