with raw_data as (
    select
        datediff('month', activity_start, activity_end) as months,
        count(*) as mcu_periods
    from dbt.mcu_activity
    group by 1
), base as (
    select
        *,
        sum(mcu_periods) over (order by months rows unbounded preceding)::float / sum(mcu_periods) over () as cumulative
    from raw_data
)
select
    *
from base
where months <= 24
