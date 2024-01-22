--first we look for users that have an intallment loan and were using overdraft the day before the loan
--we focus on the first loan with a negative balance the day before to reduce complexity
with two_product_users as (
    select * from (
        select
            u.user_created,
            u.user_id,
            id as nh_id,
            disbursement_date,
            row_number() over (partition by u.user_id order by disbursement_date) as rn
        from nh_transaction_instalment_loan nh
        inner join dbt.zrh_users u using (user_id)
        inner join dbt.bp_overdraft_users od
            on u.user_created = od.user_created
            and end_time::date = disbursement_date::date - interval '1 day'
            and timeframe = 'day'
            and od_enabled_flag
            and outstanding_balance_eur is not null
            and disbursement_date between '2021-07-01' and '2021-08-31'  --timeframe of the research
        group by 1, 2, 3, 4
    )
    where rn = 1
)
-- then we see the evolution of the users' balance in the following 30 days
select
    tpu.user_id,
    nh_id,
    to_char(disbursement_date, 'YYYY-MM-DD') as min_disbursement_date,
    to_char(date, 'YYYY-MM-DD') as date,
    date_diff('day', disbursement_date::date, date::date) as diff,
    od_enabled_flag,
    coalesce(max_amount_cents::numeric/100, 0) as max_amount_eur,
    coalesce(outstanding_balance_eur, 0) as outstanding_balance_eur,
    case when max_amount_eur::numeric = 0 then 0 else outstanding_balance_eur::numeric/max_amount_eur::numeric end as perc_usage,
    -- percentage usage buckets are how close a the outstanding balance of a user is to their overdraft limit
    case when perc_usage is null then '   non-negative balance'
        when perc_usage > 1 then 'in arrears'
        when perc_usage < 0.1 then '  <=10%'
        when perc_usage between 0.1 and 0.2 then ' 10% to 20%'
        when perc_usage between 0.2 and 0.4 then ' 20% to 40%'
        when perc_usage between 0.4 and 0.6 then ' 40% to 60%'
        when perc_usage between 0.6 and 0.8 then ' 60% to 80%'
        when perc_usage between 0.8 and 0.9 then ' 80% to 90%'
        when perc_usage > 0.9 then ' > 90%'
        end as usage_buckets,
    count(case when balance_eur >= 0 then 1 end) as n_days_non_negative_balance,
    count(case when balance_eur < 0 then 1 end) as n_days_negative_balance
from two_product_users tpu
inner join dbt.mmb_daily_balance_aud dba
    on tpu.user_created = dba.user_created
    and account_role = 'PRIMARY'
    and date::date between disbursement_date::date - interval '1 day' and disbursement_date::date + interval '30 day'
left join dbt.bp_overdraft_users od
    on tpu.user_created = od.user_created
    and od.end_time = dba.date
    and timeframe = 'day'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10