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
    u.user_created,
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
),
eligible_txns as ( 
select 
    eu.tnc_country_group,
	user_created,
	created::date as txn_date,
	round(amount_cents_eur::numeric/100) as amount_cents_eur,
	case when mcc in (
		5541,1343,1381,1454,1500,1761,1771,2490,3014,3028,3039,3048,3049,3267,3302,3351,3352,3353,3354,3356,
		3358,3359,3360,3361,3362,3363,3364,3365,3366,3367,3368,3369,3370,3371,3372,3373,3374,3375,3376,3377,
		3378,3379,3380,3381,3382,3383,3384,3385,3386,3387,3388,3390,3391,3392,3393,3394,3395,3396,3397,3398,
		3399,3400,3401,3402,3403,3404,3406,3407,3408,3409,3410,3411,3412,3413,3414,3415,3416,3417,3418,3419,
		3420,3421,3422,3423,3424,3425,3426,3427,3428,3429,3430,3431,3432,3433,3434,3435,3436,3437,3438,3439,
		3440,3520,3526,3544,3615,3635,3641,3670,3672,3716,3754,3777,3824,4121,4411,4511,4722,4812,4814,4816,
		4829,5039,5094,5122,5231,5310,5399,5411,5541,5812,5814,5815,5816,5912,5933,5960,5962,5963,5964,5965,
		5966,5967,5972,5983,5993,6011,6012,6051,6211,6513,6538,6540,7012,7021,7273,7277,7311,7361,7372,7399,
		7519,7841,7922,7988,7993,7994,7995,7997,8062,8071,8099,8211,8389,8999,9222,9223,9311,9399,9405,5813,
		5499,7523,8111,5948,5047,6300,5099,1520,7997,8299,3790) --Blacklisted MCCs provided by product
		then true else false end as blacklisted_mcc,
    case when blacklisted_mcc then mcc end as mcc,
    case when blacklisted_mcc then mcc_category end as mcc_category,
    case when blacklisted_mcc then iso_description end as mcc_description,
	count(*) as n_txns
from dbt.zrh_card_transactions t
inner join eligible_users eu using(user_created)
left join dwh_mcc_description using (mcc)
where type = 'PT' 
	and created::date between '2021-07-01' and '2022-01-31'
group by 1, 2, 3, 4, 5, 6, 7, 8
),
currently_elig_users as (
    select 
    tnc_country_group,
    user_created
    from eligible_txns 
    where ((tnc_country_group = 'DEU' and amount_cents_eur between 50 and 500) or (tnc_country_group != 'DEU' and amount_cents_eur between 50 and 200))
    and not blacklisted_mcc
    group by 1,2 
),
all_eligible_txns as ( 
select 
    eu.tnc_country_group,
	t.user_created,
    ceu.user_created is not null as has_elig_txns,
	txn_date,
	feature,
	sum(value) as total_value
from dbt.zrh_txn_day_rows t
inner join eligible_users eu using (user_created)
left join currently_elig_users ceu using (user_created)
where txn_date::date between '2021-07-01' and '2022-01-31'
    and feature in ('n_dd', 'n_dt')
group by 1, 2, 3, 4, 5
)
select 
    'month' as timeframe,
    tnc_country_group,
	date_trunc('month', txn_date) as txn_date,
	feature,
	count(distinct user_created) as n_users,
    count(distinct case when not has_elig_txns then user_created end) as n_users_excl_elig_txns,
	sum(total_value) as total_value
from all_eligible_txns
group by 1, 2, 3, 4