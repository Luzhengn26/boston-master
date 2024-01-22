setwd('/Users/wendyvu/Documents/analysis/20220929_visibility_entrypoints_ABtest')
library(n26)
library(data.table)


summary_users <- queryDB("
-- create table dev.abtest_users_visibility_tog_20220927 as
-- with users as (
-- select
--     distinct user_created,
--     platform,
--     se_value,
--     min(collector_tstamp) as ft_ts,
--     dateadd(day,20,ft_ts) as ft_ts_plus_20,
--     datediff('days',ft_ts, current_date) as diff,
--     count(*) over (partition by user_created) as n_rows
-- from dbt.snowplow where se_action = 'login.visibility_toggle_experiment.entered'
-- group by 1,2,3
-- )
-- select *
-- from users
-- where n_rows = 1 --removing users that have been assigned to more than 1 group
--     --and ft_ts_plus_20 <= current_date
--     and diff >= 20
-- order by 1
-- ;

with users as (
select u.*,
    collector_tstamp,
    collector_date,
    se_action,
    se_label,
    u.platform as login_platform,
    e.platform as vis_platform
from dev.abtest_users_visibility_tog_20220927 u
left join (select *
            from dbt.snowplow
            where se_action in ('my_account.app_settings.viewed'
                                ,'personal_information.viewed'
                                ,'my_account.visibility.toggled'
                                ,'personal_information.visibility.toggled'
                                )
) e on e.user_created = u.user_created
    and collector_tstamp between ft_ts and ft_ts_plus_20 -- timeframe for analysis
    and e.platform = u.platform
order by user_created,collector_tstamp
)
select
    case when platform = 1 then 'Android' else 'iOS' end as platform,
    case when se_value = 0 then 'control' else 'test' end as groups,
    count(distinct user_created) as n_tot_users,
    count(distinct case when se_action in ('my_account.app_settings.viewed','personal_information.viewed') then user_created end) as n_users_viewed_entry,
    count(distinct case when se_action ilike '%toggled%' then user_created end) as n_users_vis_changed,
    round(n_users_viewed_entry::float/n_tot_users,5) as perc_log_entry_viewed,
    round(n_users_vis_changed::float/n_tot_users,5) as perc_log_vis_changed,
    round(n_users_vis_changed::float/n_users_viewed_entry,3) as perc_viewed_vis_changed
from users
group by 1,2
order by 1,2
limit 500;
" , "redshift-eu")

visibility_setting <- queryDB("
with users as (
select u.*,
    collector_tstamp,
    collector_date,
    se_action,
    se_label,
    u.platform as login_platform,
    e.platform as vis_platform
from dev.abtest_users_visibility_tog_20220927 u
left join (select *
            from dbt.snowplow
            where se_action in ('my_account.app_settings.viewed'
                                ,'personal_information.viewed'
                                ,'my_account.visibility.toggled'
                                ,'personal_information.visibility.toggled'
                                )
) e on e.user_created = u.user_created
    and collector_tstamp between ft_ts and ft_ts_plus_20
    and e.platform = u.platform
order by user_created,collector_tstamp
), tog as (
select *,
    JSON_EXTRACT_PATH_TEXT(se_label, 'state',true) as is_visible,
    row_number() over (partition by user_created order by collector_tstamp desc ) as rn
from users
where se_action ilike '%toggle%'
)
select
    case when platform = 1 then 'Android' else 'iOS' end as platform,
    case when se_value = 0 then 'control' else 'test' end as groups,
    count(distinct user_created) as n_tot_users,
    count(distinct case when is_visible = 'true' then user_created end) as n_users_visible,
    round(n_users_visible::float/n_tot_users,3) as perc_vis
from tog
where rn = 1 -- last event fired within timeframe
group by 1,2
order by 1,2
limit 500;                              
                              
                              ","redshift-eu")

save(summary_users,
     visibility_setting,
     file = file.path("visibility_entrypoint_abtest.RData"))
