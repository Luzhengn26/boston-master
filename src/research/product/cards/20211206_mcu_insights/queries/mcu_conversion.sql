with ft_mcu as (
    select
        user_created,
        activity_start as ft_mcu
    from dbt.mcu_activity
    where first_time_flag is true
)
select
    to_char(ft_mau, 'yyyy-mm') as month,
    count(*) as ftmau_customers,
    avg((ft_mcu.user_created is not null)::int::float) as converted_to_mcu
from dbt.zrh_users
left join ft_mcu
    on zrh_users.user_created = ft_mcu.user_created
    and ft_mcu.ft_mcu <= zrh_users.ft_mau + interval '10 weeks'
where ft_mau >= '2019-01-01'
and ft_mau < '2021-12-01'
group by 1
