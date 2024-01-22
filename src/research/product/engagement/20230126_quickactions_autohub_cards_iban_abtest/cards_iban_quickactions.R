setwd('/Users/wendyvu/Documents/analysis/20221025_quickactions_autohub_cards&iban_abtest/')
library(n26)
library(data.table)

group_size <- queryDB("
-- drop table if exists dev.spaces_cards_iban_abtest_users_nov222022;
-- create table dev.spaces_cards_iban_abtest_users_nov222022 as
-- with users as (
-- select
--     user_id,
--     last_login_app_version,
--     ntile(2) over (order by random()) as groups
-- from (select distinct user_created from w_space_aud
--         where is_primary is false
--             and status = 'ACTIVE'
--             and current_date between rev_timestamp and end_timestamp --2022-11-11 current date
--         ) as s
-- join dbt.zrh_users using(user_created)
-- where is_mau
--     )
-- select user_id,
--     last_login_app_version,
--     case when groups = 1 then 'test' else 'control' end as groups
-- from users
-- ;
-- 
-- drop table if exists dev.users;
-- create table dev.users as
-- with users as (
-- select u.*,
--     z.user_created,
--     '2022-11-23'::date as exp_launch_dt,
--     min(act_date) as login_ts,
--     datediff('days',login_ts,current_date) as diff
-- from dev.spaces_cards_iban_abtest_users_nov222022 u
-- join dbt.zrh_users z using(user_id)
-- left join dbt.zrh_act_day a
--     on z.user_created = a.user_created
--     and a.act_date::date >= '2022-11-23'::date
--     and n_logins > 0
-- group by 1,2,3,4,5
-- )
-- select
--     u.*,
--     case when se_action is not null then 'spaces_view' else null end as se_action,
--     min(collector_tstamp)::date as fh_view_ts
-- from users u
-- left join dbt.snowplow s
--     on u.user_created = s.user_created
--     and collector_date between login_ts and u.login_ts + interval '35 days'
--     and se_action in ('space.quick_action.send_money_clicked'
--                         ,'financial_home.quick_actions.create_space'
--                         ,'spaces.details.quick_action_clicked'
--                         ,'spaces.quick_action.manage_bottom_sheet_menu.clicked'
--                         ,'spaces.details.quick_action.clicked'
--                         ,'space.quick_action.add_money_clicked'
--                         ,'financial_home.quick_actions.automation')
-- group by 1,2,3,4,5,6,7,8
-- ;


select
    groups,
    count(distinct user_created) as n_users
from dev.users
where diff >= 5
group by 1
order by 1                      
                       
                       ","redshift-eu")


cards <- queryDB("
-- cards conversion rate
with test as (
select u.*,
    '2022-11-23'::date as exp_launch_dt,
    card_id,
    start_ts,
    end_ts,
    previous_acccount_role,
    new_account_role
from dev.users u
left join dbt.card_linking_history c
    on c.user_created = u.user_created
    --and (previous_acccount_role = 'SECONDARY' OR new_account_role = 'SECONDARY')
    and start_ts >= login_ts
)
select groups,
    count(distinct user_created) as total_users,
    count(distinct case when card_id is not null then user_created end) as n_users,
    round(n_users::float/total_users,4) as perc_conversion
from test
where true
    and diff >= 35
    and se_action is not null
group by 1
" , "redshift-eu")



iban <- queryDB("
--iban conversion rates
with test as (
select u.*,
    '2022-11-23'::date as exp_launch_dt,
    w.activity_type,
    w.space_id,
    w.created
from dev.users u
left join w_activity_log w
    on w.initiator_user_id = u.user_id
    and w.created >= login_ts
    and activity_type = 'SPACE_EXTERNAL_ID_ADDED'
)
select groups,
    count(distinct user_created) as total_users,
    count(distinct case when space_id is not null then user_created end) as n_users,
    round(n_users::float/total_users,4) as perc_conversion
from test
where true
    and diff >= 35
    and se_action is not null
group by 1
" , "redshift-eu")


cards_entry <- queryDB("
-- which entry point are test users discoverying
with test as (
select u.*,
    '2022-11-23'::date as exp_launch_dt,
    collector_tstamp,
    case when w.se_action ilike '%tap-linked-to-CTA%' then '' else 'qa' end as se_act,
    case when w.se_action ilike '%tap-linked-to-CTA%' then split_part(w.se_property, '-', 2) else se_property end as entry_point
from dev.users u
left join dbt.snowplow w
    on w.user_created = u.user_created
    and w.collector_tstamp between login_ts and login_ts + interval '35 days'
    and (w.se_action ilike '%tap-linked-to-CTA%' or
        (w.se_action = 'spaces.details.quick_action_clicked' and se_property in ('cards_and_iban'))
        )
)
select
    groups,
    coalesce(entry_point,'not_viewed') as entry_point,
    count(distinct user_created) as n_users,
    sum(n_users) over(partition by groups) as tot_users,
    round(n_users::float/tot_users,4) as perc
from test
where diff >= 35
    --and se_action is not null
group by 1,2
order by 1,2;

" , "redshift-eu")



upgrades <- queryDB("
--upgrades
with test as (
select u.*,
    '2022-11-23'::date as exp_launch_dt,
    b.product_id,
    b.subscription_valid_from
from dev.users u
left join dbt.zrh_user_product b
    on u.user_created = b.user_created
    and enter_reason = 'UPGRADED'
    and b.subscription_valid_from between login_ts::date and login_ts + interval '35 days'
)
select groups,
    count(distinct case when product_id is not null then a.user_created end ) as n_upgrades,
    count(distinct a.user_created) as tot_users,
    round(n_upgrades::float/tot_users,4) as perc
from test as a
where diff >= 35
group by 1
limit 500;                   
                    
                    ","redshift-eu")


ft_cards <- queryDB("
-- are the users who link their card more likely to be first time users in the test group relative to the control group?
with card_link as (
select user_created, start_ts, end_ts, card_id, previous_account_id, new_account_id,
    row_number() over (partition by user_created order by start_ts) as rn
from dbt.card_linking_history
), test as (
select u.*,
    '2022-11-23'::date as exp_launch_dt,
    card_id,
    start_ts,
    end_ts,
    previous_account_id,
    new_account_id,
    rn,
    is_premium
from dev.users u
join dbt.zrh_users z using(user_created)
left join card_link c
    on c.user_created = u.user_created
    --and (previous_acccount_role = 'SECONDARY' OR new_account_role = 'SECONDARY')
    and start_ts between login_ts and login_ts + interval '35 days'
)
select groups,
    count(distinct user_created) as total_users,
    count(distinct case when card_id is not null and rn = 1 then user_created end) as n_users,
    round(n_users::float/total_users,4) as perc_conversion
from test
where true
    and diff >= 35
    and se_action is not null
    and is_premium is true
group by 1
                    ","redshift-eu")

n_cards_link <- queryDB("
-- how many times do user tend to link and unlink their cards?
with cards as (
select
    user_created, start_ts, end_ts, card_id, previous_account_id, new_account_id,
    count(*) over (partition by user_created,card_id) as rn
from dbt.card_linking_history
order by 1,card_id,start_ts
), agg as (
select user_created,
    card_id,
    rn
from cards
group by 1,2,3
)
select *
from agg
;                       
                       
                       ","redshift-eu")

save(group_size,
     cards,
     iban,
     cards_entry,
     upgrades,
     ft_cards,
     n_cards_link,
     file = file.path("cards_iban_quickactions.RData"))
