with feb_end_ts as (
select e.*,
        coalesce(lead(audit_rev_timestamp - interval '0.000001 second', 1) over (partition by e.user_id, purpose order by audit_rev_timestamp), '2100-01-01') as end_timestamp
from private.einsteinium_internal_credit_score_audit_log e
),
mar_end_ts as (
select e.*,
        coalesce(lead(audit_rev_timestamp - interval '0.000001 second', 1) over (partition by e.user_id, purpose order by audit_rev_timestamp), '2100-01-01') as end_timestamp
from private.einsteinium_n26_credit_score_audit_log  e
)
select
    end_time,
    user_id,
    purpose
from feb_end_ts
inner join dbt.zrh_users using (user_id)
inner join dwh_cohort_months dcm
	on end_time between audit_rev_timestamp and end_timestamp
	and end_time::date = '2022-02-28'
    and ((tnc_country_group != 'FRA' and score <= 12) or (tnc_country_group = 'FRA' and score <= 9))
union all
select
    end_time,
    user_id,
    purpose
from mar_end_ts
inner join dbt.zrh_users using (user_id)
inner join dwh_cohort_dates dcm
	on end_time between audit_rev_timestamp and end_timestamp
	and end_time::date = '2022-04-04'
    and ((tnc_country_group != 'FRA' and score <= 12) or (tnc_country_group = 'FRA' and score <= 9))