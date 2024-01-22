with dunning as (
select 
    distinct user_created
from la_arrears 
where '2022-01-31' between start_date and least(coalesce(exit_date, '2100-01-01'), current_date)
),
wo as (
select 
    distinct user_created
from dbt.write_off wo 
where write_off_dt <= '2022-01-31'
    and reason in ('Arranged Overdraft', 'Credit', 'TBIL')
),
scores as (
select e.*, 
    coalesce(lead(audit_rev_timestamp - interval '0.000001 second', 1) over (partition by e.user_id order by audit_rev_timestamp), '2100-01-01') as end_timestamp
from private.einsteinium_internal_credit_score_audit_log e
where e.purpose = 'TRANSACTION_BASED_INSTALMENT_LOAN'
    and score <= 12 -- just to be sure
),
eligible_users as (
select 
    u.tnc_country_group,
    u.user_id,
    case when wo.user_created is not null --Has a OD/Credit/TBIL Write-off
        or d.user_created is not null -- Currently has dunning
        then true else false end as has_arrears_or_wo
from dbt.stg_users u
left join wo using (user_created)
left join dunning d using (user_created)
inner join scores s
    on u.user_id = s.user_id
    and '2022-01-31' between audit_rev_timestamp and end_timestamp 
where u.tnc_country_group in('AUT', 'DEU', 'ESP', 'FRA', 'ITA')
    and closed_at is null -- Has open account
)
select 
    u.tnc_country_group,
    count(case when kyc_first_completed is not null and closed_at is null then 1 end) as kycc_users,
    count(case when is_mau then 1 end) as n_maus,
    count(distinct eu.user_id) as n_lisbon_eligible_users,
    count(distinct case when not has_arrears_or_wo then eu.user_id end) as n_eligible_users,
    count(distinct case when not has_arrears_or_wo and is_mau then eu.user_id end) as n_eligible_maus,
    round(n_eligible_users::numeric/ kycc_users, 3) *100 as perc_eligible_users_out_of_kycc_users,
    round(n_eligible_maus::numeric/ n_maus, 3) *100 as perc_eligible_maus_out_of_maus,
    count(case when legal_entity = 'EU'  and kyc_first_completed is not null and closed_at is null then 1 end) as kycc_users_eu_legal,
    count(case when legal_entity = 'EU' and is_mau then 1 end) as n_maus_eu_legal,
    count(distinct case when legal_entity = 'EU' then eu.user_id end) as n_lisbon_eligible_users_eu_legal,
    count(distinct case when not has_arrears_or_wo and legal_entity = 'EU'then eu.user_id end) as n_eligible_users_eu_legal,
    count(distinct case when not has_arrears_or_wo and is_mau and legal_entity = 'EU' then eu.user_id end) as n_eligible_maus_eu_legal,
    round(n_eligible_users_eu_legal::numeric/ kycc_users_eu_legal, 3) *100 as perc_eligible_users_out_of_kycc_users_eu_legal,
    round(n_eligible_maus_eu_legal::numeric/ n_maus_eu_legal, 3) *100 as perc_eligible_maus_out_of_maus_eu_legal
from dbt.zrh_users u
left join eligible_users eu using (user_id)
where u.tnc_country_group in('AUT', 'DEU', 'ESP', 'FRA', 'ITA')
group by 1