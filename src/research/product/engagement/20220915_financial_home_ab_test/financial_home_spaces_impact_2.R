setwd('/Users/wendyvu/Documents/analysis/financial_home_spaces_impact/')
library(n26)
library(data.table)



test_users <- queryDB("
-- number of spaces created 
with users as (
select user_id,
       country_tnc_legal,
       kyc_first_completed,
       n_spaces_txns,
       spaces_balance_cents,
       status_spaces,
       exp_group
from dev_dbt.fh_abtest_control_infocard
union all
select user_id,
       country_tnc_legal,
       kyc_first_completed,
       n_spaces_txns,
       spaces_balance_cents,
       status_spaces,
       exp_group
from dev_dbt.fh_abtest_test_infocard
), space_created as (
    select --u.*,
           u.user_id,
           exp_group,
           --user_type,
           status_spaces,
           case when n_spaces_txns is not null then 1 else 0 end as spaces_tx,
           tot_user_grp_type,
           a.activity_type,
           a.space_id,
           a.created
    from (select *,count(*) over (partition by exp_group,status_spaces) as tot_user_grp_type from dev.fh_abtest_infocard) u
             join dbt.zrh_users z
                  using(user_id)
             join w_activity_log a
                  on a.initiator_user_id = z.user_id
                      and a.created >= '2022-05-31'::date
                      and a.created between '2022-05-31'::date and '2022-05-31'::date + interval '35 days'
                      and activity_type = 'SPACE_CREATED'
)
select
    case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
    exp_group,
    spaces_tx,
    tot_user_grp_type,
    --date_trunc('week',created) as week,
    count(distinct user_id) as n_users,
    count(distinct space_id) as n_space_created,
    round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created
group by 1,2,3,4
order by 1,2,3
;

                           ","redshift-eu")





space_create_click <- queryDB("
-- AB test launch May 31 2022
with finhome as (
    select
        u.*,
        --collector_date,
        case when se_action = 'financial_home.long.press' then 'fh_longpress'
             when se_action = 'financial_home.main_account.clicked' then 'fh_main_acct_click'
             when se_action = 'financial_home.spaces.clicked' then 'fh_space_details_click'
             when se_action = 'financial_home.spaces.shared.certification.clicked' then 'fh_shared_certify'
             when se_action = 'financial_home.error_retry_clicked' then 'fh_error_retry'
             when se_action = 'financial_home.spaces.createspace.clicked' then 'fh_create_space'
             when se_action = 'financial_home.tab.viewed' then 'fh_view'
             when se_action = 'financial_home.spaces.transfer.clicked' then 'fh_transfer_click_dnd'
             when se_action = 'financial_home.drag.drop' then 'fh_dnd' --drag_drop
             when se_action = 'financial_home.spaces.shared.invitation.clicked' then 'fh_shared_invite'
             when se_action = 'financial_home.spaces.emptystate.createspace.clicked' then 'fh_emptystate_create_space'
             --when se_action = 'space_creation_view' then 'sp_create_space'
             when se_action is null then 'no_fh_view'
             else se_action end as feature
    from (select *, count(*) over (partition by exp_group,status_spaces) as group_size from dev.fh_abtest_users) u
             join dbt.zrh_users z using(user_id)
             left join (select *
                        from dbt.snowplow
                        where collector_date >= '2022-02-22'::date
                          and se_action ilike 'financial_home%'
    ) s
                       on z.user_created = s.user_created
                           and collector_date between start_date and start_date + interval '35 days'
)
select 
       feature,
       case when status_spaces = 'ACTIVE' then 'Spaces Users' else 'Non-Spaces Users' end as status_spaces,
       exp_group,
       group_size,
       count(distinct user_id)
from finhome
--where exp_group = 'control'
group by 1, 2, 3, 4
order by 1,2,3                              
                              ","redshift-eu")


revisit_create_spaces <- queryDB("
--revisiting the created space from the initial fh launch 
with fh_users as (
    select * from dev.fh_test_users
    union all
    select * from dev.fh_control_users
),space_created as (
    select --u.*,
           z.user_created,
           start_date,
           group_,
           user_type,
           status_spaces,
           tot_user_grp_type,
           a.activity_type,
           a.space_id,
           a.created
    from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from fh_users) u
             join dbt.zrh_users z
                  using(user_id)
             join w_activity_log a
                  on a.initiator_user_id = z.user_id
                      and a.created >= u.start_date
                      and activity_type = 'SPACE_CREATED'
)
select
    start_date,
    user_type,
    group_,
    case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
    tot_user_grp_type,
    --date_trunc('week',created) as week,
    count(distinct user_created) as n_users,
    count(distinct space_id) as n_space_created,
    round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created
group by 1,2,3,4,5
order by 1,2,3,4
;                                 
                                 
                                 ","redshift-eu")

dragndrop <- queryDB("
-- drag n drop
with fh_users as (
    select * from dev.fh_test_users
    union all
    select * from dev.fh_control_users
), finhome as (
    select
        z.user_created,
        start_date,
        group_,
        user_type,
        status_spaces,
        --collector_date,
        case when se_action ilike 'financial%' then 'fh_dragdrop' else se_action end as feature,
        tot_user_grp_type
    from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from fh_users) as u
        join dbt.zrh_users z using(user_id)
             left join (select *
                        from dbt.snowplow
                        where collector_date >= '2022-02-22'::date
                          and se_action in ('spaces.dadmovemoney_viewed' --,'financial_home.spaces.transfer.clicked','financial_home.drag.drop'
                            )
    ) s
                       on z.user_created = s.user_created
                           and collector_date >= start_date
    group by 1,2,3,4,5,6,7
)
select
    case when start_date = '2022-02-22' then '3k'
         else '35k' end as sampling,
    user_type,
    group_,
    case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
    --feature,
    tot_user_grp_type,
    count(distinct user_created) as n_users,
    round(n_users::float/tot_user_grp_type,4) as perc_users
from finhome
where feature is not null
group by 1,2,3,4,5
order by 1,2,3,4
limit 500;
               
               
               ","redshift-eu")

users_spaces_txns <- queryDB("
-- count users with 1+ spaces txns
--Can customers understand how to do money transfer? - # of space tx/user, # of tx performed by DnD
-- no significant difference between test and control group when it comes to the avg amount of txns and users initiaiting spaces txns
with fh_users as (
    select * from dev.fh_test_users
    union all
    select * from dev.fh_control_users
), space_created as (
    select zu.user_created,
           z.user_created as user_created_tx,
           user_type,
           group_,
           tot_user_grp_type,
           status_spaces,
           --z.txn_date,
           sum(coalesce(n_spaces,0)) as n_spaces
    from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from fh_users) u
        join dbt.zrh_users zu using(user_id)
             join dbt.zrh_txn_day z
                  on zu.user_created = z.user_created
                      and z.txn_date >= start_date
                      and n_spaces > 0
    group by 1,2,3,4,5,6
)
select
    user_type,
    group_,
    tot_user_grp_type,
    case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
    --date_trunc('week',created) as week,
    --median(n_spaces) as med_spaces_txn
    count(distinct user_created) as n_users,
    round(n_users::float/tot_user_grp_type,3) as perc_users
from space_created
group by 1,2,3,4
order by 1,2,3
limit 500;                             
                             
                             ","redshift-eu")

dragndrop_os <- queryDB("
-- drag n drop split by platform
with fh_users as (
    select * from dev.fh_test_users
    union all
    select * from dev.fh_control_users
), finhome as (
    select
        z.user_created,
        z.last_login_os,
        start_date,
        group_,
        user_type,
        status_spaces,
        --collector_date,
        case when se_action ilike 'financial%' then 'fh_dragdrop' else se_action end as feature,
        tot_user_grp_type,
        count(*) over (partition by last_login_os,group_,user_type,status_spaces) as tot_user_grp_type2
    from (select *,count(*) over (partition by group_,user_type,status_spaces) as tot_user_grp_type from fh_users) as u
        join dbt.zrh_users z using(user_id)
             left join (select *
                        from dbt.snowplow
                        where collector_date >= '2022-02-22'::date
                          and se_action in ('spaces.dadmovemoney_viewed' --,'financial_home.spaces.transfer.clicked','financial_home.drag.drop'
                              )

    ) s
                       on z.user_created = s.user_created
                           and collector_date >= start_date
    group by 1,2,3,4,5,6,7,8
)
select
    last_login_os as platform,
    case when start_date = '2022-02-22' then '3k'
         else '35k' end as sampling,
    user_type,
    group_,
    case when status_spaces = 'ACTIVE' then 'Space Users' else 'Non-Space Users' end as status_spaces,
    --feature,
    tot_user_grp_type,
    case when platform = 1 then 'Android'
        when platform = 2 then 'iOS'
            else 'Other' end as platform,
    count(distinct user_created) as n_users,
    round(n_users::float/tot_user_grp_type,4) as perc_users
from finhome
where feature is not null
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5
limit 500;
                        
                        
                        ","redshift-eu")


revisit_space_create_click <- queryDB("
-- revisit launch in march 2022
with users as (
    select * from dev.fh_test_users
    union all
    select * from dev.fh_control_users
), finhome as (
    select
        u.*,
        --collector_date,
        case when se_action = 'financial_home.long.press' then 'fh_longpress'
             when se_action = 'financial_home.main_account.clicked' then 'fh_main_acct_click'
             when se_action = 'financial_home.spaces.clicked' then 'fh_space_details_click'
             when se_action = 'financial_home.spaces.shared.certification.clicked' then 'fh_shared_certify'
             when se_action = 'financial_home.error_retry_clicked' then 'fh_error_retry'
             when se_action = 'financial_home.spaces.createspace.clicked' then 'create_space'
             when se_action = 'financial_home.tab.viewed' then 'fh_view'
             when se_action = 'financial_home.spaces.transfer.clicked' then 'fh_transfer_click_dnd'
             when se_action = 'financial_home.drag.drop' then 'fh_dnd' --drag_drop
             when se_action = 'financial_home.spaces.shared.invitation.clicked' then 'fh_shared_invite'
             when se_action = 'financial_home.spaces.emptystate.createspace.clicked' then 'create_space'
             when se_action = 'space_creation_view' then 'create_space'
             when se_action is null then 'no_fh_view'
             else se_action end as feature
    from (select *, count(*) over (partition by group_,status_spaces) as group_size from users) u
             join dbt.zrh_users z using(user_id)
             left join (select *
                        from dbt.snowplow
                        where collector_date >= '2022-02-22'::date
                          and (se_action ilike 'financial_home%' or se_action = 'space_creation_view')
    ) s
                       on z.user_created = s.user_created
                           and collector_date >= start_date
)
select
    case when feature ilike '%create_space' then 'create_space' else feature end as feat_cat,
    feature,
    case when status_spaces = 'ACTIVE' then 'Spaces Users' else 'Non-Spaces Users' end as status_spaces,
    user_type,
    group_,
    group_size,
    count(distinct user_id) as n_users
from finhome
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
limit 500;                             
                              ","redshift-eu")


save(test_users,
     space_create_click,
     revisit_create_spaces,
     dragndrop,
     users_spaces_txns,
     dragndrop_os,
     revisit_space_create_click,
     file = file.path("financial_home_spaces_impact_2.RData"))

 