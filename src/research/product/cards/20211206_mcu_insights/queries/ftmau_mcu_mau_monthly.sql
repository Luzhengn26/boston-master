with maus as (
    select
        month as month,
        sum(maus) as maus
    from dbt.mau_market_agg
    where month < '2021-12'
        and month > '2019-01'
    group by 1
), mcus as (
    select
        month as month,
        sum((activity_start < start_time)::int) as mcus
    from dwh_cohort_months
    inner join dbt.stg_mcu_periods
        on dwh_cohort_months.start_time <= stg_mcu_periods.activity_end
        and dwh_cohort_months.end_time >= stg_mcu_periods.activity_start
    where month < '2021-12'
        and month > '2019-01'
    group by 1
), ftmau as (
    select
        month as month,
        sum((kyc_first_completed < start_time)::int) as ftmau
    from dwh_cohort_months
    inner join dbt.zrh_users
        on dwh_cohort_months.start_time <= coalesce(zrh_users.closed_at, current_date)
        and dwh_cohort_months.end_time >= zrh_users.ft_mau
        and zrh_users.ft_mau is not null
    where month < '2021-12'
        and month > '2019-01'
    group by 1

)
select
    month,
    ftmau,
    maus,
    mcus,
    mcus::numeric / maus as perc_on_mau,
    mcus::numeric / ftmau as perc_on_ftmau
from maus
left join mcus
    using (month)
left join ftmau
    using (month)
order by 1 desc
