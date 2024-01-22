{{
	config(
		materialized = "table"
				)
}}

with all_users as (select u.user_id,
                          u.user_created,
                          cmd_address.country,
                          tnc_country_group
                   from dbt.zrh_users u
                       inner join cmd_address
                                       on u.user_id = cmd_address.user_id
                                           and cmd_address.type = 'LEGAL'
                                           and cmd_address.country in ('DEU','ESP') -- DE based users
                   where u.closed_at is null -- account still open
                     and tnc_country_group in ('DEU','ESP') -- DE TnC
                     and kyc_first_completed <= current_date - interval '6 months' --users with at least 6 months of activity
),

     rejected_transaction as (  select np_collection.user_id
                                from np_collection
                                inner join all_users au on np_collection.user_id = au.user_id
                                where np_collection.created  >=  current_date - interval '6 months'
                                and np_collection.return_reason = 'INSUFFICIENT_FUNDS'
                                
                                union all
                                
                                select cta.user_id
                                from dbt.card_transactions_ar cta
                                inner join all_users au on cta.user_id = au.user_id
                                where cta.rejection_reason = 'INSUFFICIENT_FUNDS'
                                and cta.created  >=  current_date - interval '6 months'
                                ),     -- rejected transaction last 6 months

     avg_balance AS (select month,
                            mmb_monthly_balance_aud.user_id,
                            od_enabled_flag,
                            outstanding_balance_eur,
                            coalesce(sum(balance_eur),0)                                         as balance,
                            avg(balance) over (partition by mmb_monthly_balance_aud.user_id) as avg_balance_last6m
                     from dbt.mmb_monthly_balance_aud
                     inner join all_users au on au.user_id = mmb_monthly_balance_aud.user_id
                              left join dbt.bp_overdraft_users ou
                                on ou.user_created = mmb_monthly_balance_aud.user_created and
                                   to_char(mmb_monthly_balance_aud.date, 'YYYY-MM') =
                                   to_char(ou.end_time, 'YYYY-MM') and timeframe = 'month'
                     where mmb_monthly_balance_aud.date >= current_date - interval '6 months'
                     and (od_enabled_flag is null or od_enabled_flag = false) 
                     group by 1, 2, 3, 4, balance_eur)


select distinct au.*
from all_users au
         inner join rejected_transaction rt on rt.user_id = au.user_id
         inner join avg_balance ab on ab.user_id = au.user_id
         left join dbt.zrh_txn_month tm on tm.user_created = au.user_created and tm.txn_month >= current_date - interval '6 months' 
where ab.avg_balance_last6m >= 1000