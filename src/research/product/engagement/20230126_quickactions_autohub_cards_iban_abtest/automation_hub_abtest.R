setwd('/Users/wendyvu/Documents/analysis/20221025_quickactions_autohub_cards&iban_abtest/')
library(n26)
library(data.table)

sample_size <- queryDB("
-- -- users who entered the experiment
-- -- and have been in the experiment for at least 10 days
-- -- and created a space or activated automation feature with 10 days of entering the experiment\
-- drop table if exists dev.automation_spaces_hub_abtest_10d;
-- create table dev.automation_spaces_hub_abtest_10d as
-- with users_enter as (
-- select u.user_id,
--     z.user_created,
--     u.groups,
--     s.se_action,
--     min(s.collector_tstamp) as enter_ts
-- from (select * from dev.automation_hub_spaces_userid_20221006 where last_login_app_version ilike '%android%') u
-- join dbt.zrh_users z using(user_id)
-- join dbt.snowplow s
--     on z.user_created = s.user_created
--     and s.collector_tstamp >= '2022-10-10'::date
--     and se_action in ('login')
-- group by 1,2,3,4
-- order by 1,2,3
-- ), fh as (
-- select
--     u.*,
--     s.se_action as fh_viewed,
--     min(collector_tstamp) as fh_ts,
--     dateadd('day',10, enter_ts) as end_ts
-- from users_enter u
-- left join dbt.snowplow s
--     on u.user_created = s.user_created
--     and s.se_action = 'financial_home.tab.viewed'
--     and s.collector_tstamp >= enter_ts::date -- filter for events after user enters experiment
-- group by 1,2,3,4,5,6
-- )
-- select a.*,
--     p.is_premium,
--     p.is_newstandard,
--     case when b.status = 'ACTIVE' then 'spaces_user' else 'non_spaces_users' end as user_type
-- from fh a
-- join dbt.zrh_user_product p
--     on p.user_created = a.user_created
--     and a.enter_ts between p.subscription_valid_from and p.subscription_valid_until
-- left join w_space_aud b
--     on a.user_created = b.user_created
--     and b.is_primary is false
--     and b.status = 'ACTIVE'
--     and a.enter_ts between b.rev_timestamp and b.end_timestamp
-- where end_ts::date <= current_date::date
-- group by 1,2,3,4,5,6,7,8,9,10,b.status
-- order by 1
-- 

select
    groups,
    user_type,
    count(distinct user_created) as n_users,
    count(distinct case when fh_viewed is not null then user_created end) as n_users_fh
from dev.automation_spaces_hub_abtest_10d
group by 1,2
order by 1,2                       
                       
                       ","redshift-eu")

group_size <- queryDB("
select
    is_premium,
    is_newstandard,
    groups,
    user_type,
    count(distinct user_created) as n_users,
    count(distinct case when fh_viewed is not null then user_created end) as n_users_fh
from dev.automation_spaces_hub_abtest_10d
group by 1,2,3,4
order by 1,2

" , "redshift-eu")


spaces <- queryDB("
with users as (
select a.groups,
    user_type,
    a.is_newstandard,
    fh_viewed,
    count(distinct case when activity_type = 'SPACE_CREATED' then user_created end) as n_users,
    count(distinct user_created) as tot_users,
    round(n_users::float/tot_users,4) as perc
from dev.automation_spaces_hub_abtest_10d a
left join w_activity_log b
    on a.user_id = b.initiator_user_id
    and activity_type = 'SPACE_CREATED'
    and b.created between enter_ts and end_ts
group by 1,2,3,4
)
select *
from users
order by 1,2,3,4

" , "redshift-eu")



automation <- queryDB("
with users as (
select a.*,
    b.created,
    b.action_type
from dev.automation_spaces_hub_abtest_10d a
left join cn_automated_rule b
    on a.user_created = b.user_created
    and b.created between enter_ts and end_ts
    and b.action_type in ('ROUND_UP','SPACES_MONEY_TRANSFER','INCOME_SORTER')
order by user_created, created
)
select groups,
    fh_viewed,
    user_type,
    count(distinct case when action_type is not null then user_created end) as n_users,
    count(distinct user_created) as tot_users,
    round(n_users::float/tot_users,4) as perc
from users
where is_premium is true
group by 1,2,3
order by 1,2,3
;
" , "redshift-eu")

upgrades <- queryDB("
--upgrades
with users as (
select a.*,
    b.subscription_valid_from,
    b.subscription_valid_until,
    b.enter_reason
from dev.automation_spaces_hub_abtest_10d a
left join dbt.zrh_user_product b
    on a.user_created = b.user_created
    and enter_reason = 'UPGRADED'
    and b.subscription_valid_from > enter_ts
order by user_created, subscription_valid_from
)
select groups,
    fh_viewed,
    user_type,
    count(distinct case when enter_reason is not null then user_created end) as n_users,
    count(distinct user_created) as tot_users,
    round(n_users::float/tot_users,4) as perc
from users
where is_premium is false -- standard at the time of login and entering the experiment
group by 1,2,3
order by 1,2,3                    
                    
                    ","redshift-eu")

save(sample_size,
     group_size,
     spaces,
     automation,
     upgrades,
     file = file.path("automation_hub_abtest.RData"))
