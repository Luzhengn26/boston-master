setwd('/Users/wendyvu/Documents/analysis/20230215_spaces_iban_deepdive')
library(n26)
library(data.table)


iban_txn_activity <- queryDB("
-- drop table if exists dev.spaces_iban_sepa_txns;
-- create table dev.spaces_iban_sepa_txns as
-- with iban as (
-- select
--     s.user_created,
--     --a.space_id,
--     s.account_id,
--     a.initiator_user_id as user_id,
--     a.created as iban_ts,
--     s.end_ts
-- from (select * from w_activity_log where activity_type = 'SPACE_EXTERNAL_ID_ADDED') a
-- join (select user_created,account_id, id, min(rev_timestamp) as start_ts, max(end_timestamp) as end_ts
--         from w_space_aud
--         where is_primary is false
--         and status = 'ACTIVE'
--         group by 1,2,3
--         ) s
--     on a.space_id = s.id
--     and a.created::date between s.start_ts::date and s.end_ts::date
-- ), txn as (
-- select i.user_created,
--     i.account_id,
--     i.iban_ts,
--     d.start_time,
--     d.end_time,
--     t.account_role,
--     t.txn_type,
--     t.type,
--     sum(n_txns) as n_txns
-- from (select * from dbt.all_cohorts where type = 'month' and end_time <= current_date) d
-- join iban i
--     on d.end_time between i.iban_ts and i.end_ts::date
-- left join (
--     select
--             user_created,
--             account_id,
--             account_role,
--             user_certified::date as txn_ts,
--             --date_trunc('month',user_certified)::date as month,
--             case when account_role = 'SECONDARY' and payment_scheme != 'SPACES' and type in ('CT','DT','DD') then 'spaces_sepa'
--                 when payment_scheme = 'SPACES' then 'spaces_internal'
--                 --when account_role = 'PRIMARY' and payment_scheme != 'SPACES' and type in ('CT','DT','DD') then 'main_sepa'
--                 else 'other' end as txn_type,
--             type,
--             count(*) as n_txns
--     from dbt.zr_transaction_user
--     where true
--         and user_created in (select distinct user_created from iban)
--         and type in ('CT','DT','DD')
--     --and user_certified >= date_trunc('months',current_date)::date - interval '16 months'
--     group by 1,2,3,4,5,6
--     ) t
--         on t.account_id = i.account_id
--         --on t.user_created = i.user_created
--         and t.txn_ts::date between i.iban_ts::date and d.end_time::date
-- group by 1,2,3,4,5,6,7,8
-- )
-- select *
-- from txn
-- --group by 1,2,3
-- order by 1,2,4
-- ;
-- how many accounts have 1+ sepa trxns? ~67% accounts have 1+ sepa txns and ~88% accounts have 1+ trxns
with piv as (
select user_created,
    account_id,
    iban_ts,
    start_time::date as month,
    sum(case when txn_type = 'spaces_sepa' and type = 'CT' then n_txns else 0 end) as spaces_sepa_ct,
    sum(case when txn_type = 'spaces_sepa' and type = 'DT' then n_txns else 0 end) as spaces_sepa_dt,
    sum(case when txn_type = 'spaces_sepa' and type = 'DD' then n_txns else 0 end) as spaces_sepa_dd,
    sum(case when txn_type = 'spaces_internal' then n_txns else 0 end) as spaces_internal,
    (spaces_sepa_ct + spaces_sepa_dt + spaces_sepa_dd) as spaces_sepa_all,
    (spaces_sepa_ct + spaces_sepa_dt + spaces_sepa_dd + spaces_internal) as spaces_txn_all
from dev.spaces_iban_sepa_txns
group by 1,2,3,4
)
select month,
    count(distinct case when spaces_txn_all > 0 then account_id end) as n_iban_act_txn,
    count(distinct case when spaces_sepa_all > 0 then account_id end) as n_iban_sepa_all,
    count(distinct case when spaces_internal > 0 then account_id end) as n_internal_txn,
    count(distinct account_id) as tot_iban,
    round(n_iban_act_txn::float/tot_iban,4) as perc_iban,
    round(n_iban_sepa_all::float/tot_iban,4) as perc_iban_sepa,
    round(n_internal_txn::float/tot_iban,4) as perc_internal
from piv
group by 1
order by 1
;

" , "redshift-eu")


iban_txn_type <- queryDB("
-- % of accounts with 1+ sepa trxns split by type
with piv as (
select user_created,
    account_id,
    iban_ts,
    start_time::date as month,
    sum(case when txn_type = 'spaces_sepa' and type = 'CT' then n_txns else 0 end) as spaces_sepa_ct,
    sum(case when txn_type = 'spaces_sepa' and type = 'DT' then n_txns else 0 end) as spaces_sepa_dt,
    sum(case when txn_type = 'spaces_sepa' and type = 'DD' then n_txns else 0 end) as spaces_sepa_dd,
    sum(case when txn_type = 'spaces_internal' then n_txns else 0 end) as spaces_internal,
    (spaces_sepa_ct + spaces_sepa_dt + spaces_sepa_dd) as tot_spaces_sepa,
    (spaces_sepa_ct + spaces_sepa_dt + spaces_sepa_dd + spaces_internal) as tot_spaces_txn
from dev.spaces_iban_sepa_txns
group by 1,2,3,4
)
select
    month,
    round(count(distinct case when spaces_sepa_ct > 0 then account_id end)::float/count(distinct account_id),4) as spaces_sepa_ct,
    round(count(distinct case when spaces_sepa_dt > 0 then account_id end)::float/count(distinct account_id),4) as spaces_sepa_dt,
    round(count(distinct case when spaces_sepa_dd > 0 then account_id end)::float/count(distinct account_id),4) as spaces_sepa_dd,
    --round(count(distinct case when tot_spaces_sepa > 0 then account_id end)::float/count(distinct account_id),4) as spaces_sepa_all,
    --round(count(distinct case when spaces_internal > 0 then account_id end)::float/count(distinct account_id),4) as spaces_internal,
    count(distinct account_id) as tot_acct
from piv
group by 1
order by 1
;
                        
                        ","redshift-eu")

text_sepa <- queryDB("
with test as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    t.partner_name,
    --t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    --and type = 'DD'
group by 1,2,3,4
)
select *
from test
;                     
                     ","redshift-eu")

text_internal <- queryDB("
with test as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    --t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme = 'SPACES'
    --and type = 'DD'
join (select user_created,locale from cmd_user_preferences where locale in ('de','en'))
  using(user_created)
group by 1,2,3
)
select *
from test
;                         
                         ","redshift-eu")

ref_text_sepa <- queryDB("
with test as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    --and type = 'DD'
group by 1,2,3,4
)
select *
from test
;                     
                     ","redshift-eu")

top_partner_dt <- queryDB("
with top_partner as (
select
--     i.user_created,
--     i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    t.partner_name,
    --t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'DT'
group by 1,2,3
), partners as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    t.partner_name,
    --t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'DT'
    and partner_name in (select distinct partner_name from top_partner order by n_accts desc limit 500)
group by 1,2,3,4
)
select * from partners
                          
                          
                          ","redshift-eu")

top_reftext_dt <- queryDB("
with top_partner as (
select
--     i.user_created,
--     i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt,
    row_number() over (partition by locale order by n_accts desc) as rank
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'DT'
    and reference_text not like '%N26%'
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
group by 1,2,3,4
), partners as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'DT'
    and reference_text in (select reference_text from top_partner where rank <= 1000)
group by 1,2,3,4,5
)
select *
from partners
;                          
                          
                          ","redshift-eu")

iban_txn_direction <- queryDB("
-- create temp table transfer_ids as
-- with spaces_users as (
-- select
--     COALESCE(s_m.user_created,s.user_created) as user_created,
--     s_m.space_id,
--     s.account_id,
--     a.activity_type,  -- spaces iban = 'SPACE_EXTERNAL_ID_ADDED'
--     COALESCE(s_m.role, 'OWNER') as role,
-- 	coalesce(count(case when s_m.role = 'MEMBER' then 1 end)
-- 		   			 over (partition by s.id, s.end_ts),0) as shared_members,
-- 	start_ts, end_ts
-- from (select user_created, id,account_id, min(rev_timestamp) as start_ts, max(end_timestamp) as end_ts
--         from w_space_aud
--         where is_primary is false
--         and status = 'ACTIVE'
--         group by 1,2,3
--         ) s
-- join w_member_aud as s_m
--     on s_m.space_id = s.id
-- 	and current_date between s_m.rev_timestamp and s_m.end_timestamp
--     and s_m.status = 'ACTIVE'
-- left join (select * from w_activity_log where activity_type = 'SPACE_EXTERNAL_ID_ADDED') a --spaces iban
--     on a.space_id = s.id
--     and a.created::date between s.start_ts::date and s.end_ts::date
-- ),transfer_ids as (
-- select
--     s.user_created,
--     s.shared_members,
--     case when activity_type = 'SPACE_EXTERNAL_ID_ADDED' then 'iban_spaces'
--         when shared_members > 0 and activity_type is null then 'shared_spaces'
--         else 'regular_spaces' end as spaces_type,
--     s.start_ts,
--     s.end_ts,
--     t.created,
--     t.transfer_id,
--     t.account_id,
--     t.account_role,
--     t.type
-- from (select * from spaces_users where activity_type = 'SPACE_EXTERNAL_ID_ADDED') s
-- join ag_transaction_details t
--     on s.account_id = t.account_id
--     and s.user_created = t.user_created
--     and t.created between s.start_ts and s.end_ts
-- )
-- select * from transfer_ids;
-- drop table if exists dev.iban_transfer_id;
-- create table dev.iban_transfer_id as
-- select s.user_created,
--     s.start_ts,
--     s.end_ts,
--     t.created,
--     t.type,
--     t.transfer_id,
--     t.account_id,
--     t.account_role
-- from transfer_ids s
-- join ag_transaction_details t
--     on s.transfer_id = t.transfer_id
--     and t.created between start_ts and end_ts
-- order by user_created, transfer_id
-- ;

with trans_id as (
select
    user_created,
    transfer_id,
    type,
    account_role
from dev.iban_transfer_id
group by 1,2,3,4
), piv as (
select
    user_created,
    transfer_id,
    sum(case when account_role = 'PRIMARY' and type = 'DT' then 1 else 0 end ) as primary_dt,
    sum(case when account_role = 'PRIMARY' and type = 'CT' then 1 else 0 end ) as primary_ct,
    sum(case when account_role = 'SECONDARY' and type = 'DT' then 1 else 0 end ) as secondary_dt,
    sum(case when account_role = 'SECONDARY' and type = 'CT' then 1 else 0 end ) as secondary_ct
from trans_id
group by 1,2
)
select
    primary_dt,
    primary_ct,
    secondary_dt,
    secondary_ct,
    count(distinct transfer_id) as n_transfers,
    sum(n_transfers) over () as tot_transfers,
    round(n_transfers::float/tot_transfers,4) as perc
from piv
group by 1,2,3,4
having(count(distinct transfer_id) > 50)
order by n_transfers desc
limit 500;                              
                              
                              ","redshift-eu")

top_partner_ct <- queryDB("
with top_partner as (
select
--     i.user_created,
--     i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    t.partner_name,
    --t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt,
    row_number() over (partition by locale order by n_accts desc) as rank
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'CT'
    and reference_text not like '%N26%'
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
group by 1,2,3,4
), partners as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    t.partner_name,
    --t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'CT'
    and partner_name in (select partner_name from top_partner where rank <= 1000)
group by 1,2,3,4,5
)
select *
from partners
;                          
                          
                          ","redshift-eu")

top_reftext_ct <- queryDB("
with top_partner as (
select
--     i.user_created,
--     i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt,
    row_number() over (partition by locale order by n_accts desc) as rank
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'CT'
    and reference_text not like '%N26%'
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
group by 1,2,3,4
), partners as (
select
--     i.user_created,
    i.account_id,
--     iban_ts,
--     start_time,
--     end_time,
--     t.user_certified,
    c.locale,
    t.type,
    case when t.payment_scheme = 'SPACES' then 'internal' else 'sepa' end as payment_scheme,
    --t.partner_name,
    t.reference_text,
    count(distinct t.account_id) as n_accts,
    count(*) as cnt
from (select user_created, account_id, iban_ts, start_time, end_time
        from dev.spaces_iban_sepa_txns
        group by 1,2,3,4,5) i
join (select user_created, locale from cmd_user_preferences where locale in ('de','en')) c
    on c.user_created = i.user_created
join etl_reporting.zr_transaction t
    on t.account_id = i.account_id
    --and t.type in ('CT','DT','DD')
    and t.user_certified::date between i.iban_ts::date and i.end_time::date
    and payment_scheme != 'SPACES'
    and type = 'CT'
    and reference_text in (select reference_text from top_partner where rank <= 1000)
group by 1,2,3,4,5
)
select *
from partners
;                          
                          
                          ","redshift-eu")

save(
     iban_txn_activity,
     iban_txn_type,
     text_sepa,
     text_internal,
     ref_text_sepa,
     top_partner_dt,
     top_reftext_dt,
     iban_txn_direction,
     top_reftext_ct,
     top_partner_ct,
     file = file.path("spaces_iban_deepdive.RData"))
