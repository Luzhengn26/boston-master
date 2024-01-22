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
                                           and cmd_address.country in ('DEU', 'ESP') -- DE/ES based users
                   where u.closed_at is null       -- account still open
                     and tnc_country_group in ('DEU', 'ESP') -- DE/ES TnC
                     and kyc_first_completed <= current_date - interval '6 months' --users with at least 6 months of activity
),

     rejected_transaction as (select np_collection.user_id
                              from np_collection
                                       inner join all_users au on np_collection.user_id = au.user_id
                              where np_collection.created >= current_date - interval '6 months'
                                and np_collection.return_reason = 'INSUFFICIENT_FUNDS'

                              union all

                              select cta.user_id
                              from dbt.card_transactions_ar cta
                                       inner join all_users au on cta.user_id = au.user_id
                              where cta.rejection_reason = 'INSUFFICIENT_FUNDS'
                                and cta.created >= current_date - interval '6 months'), -- rejected transaction last 6 months


     regular_income AS (SELECT zt.user_id,
                               zt.user_id || zt.amount_cents/100 || coalesce(merchant_name, partner_bic) || CEIL(EXTRACT(DAY FROM txn_ts) / 10.0) as label,
                               COUNT(DISTINCT to_char(txn_ts, 'YYYY-MM')) as num_months
                        FROM rejected_transaction
                                 inner join dbt.zrh_transactions zt
                                            on zt.user_id = rejected_transaction.user_id 
                                            and zt.type not in ('AA', 'DR', 'PF', 'PRESENTMENT_REFUND', 'WU')
                                            and internal_txn_flg = false
                                 left join zr_transaction on zt.txn_id = zr_transaction.id
                                 left join dbt.zrh_rule_txn zrt on zrt.txn_id = zt.txn_id
                        where txn_ts >= date_trunc('month', current_date) - interval '6 month'
                          and amount_cents > 10000
                          and (action_type not in
                               ('INCOME_SORTER', 'INCOME_SORTER_FIXED_AMOUNT', 'ROUND_UP', 'SPACES_MONEY_TRANSFER') or
                               action_type is null)
                        GROUP BY 1, 2
                        HAVING num_months >= 3)


select all_users.*
from regular_income
left join all_users using (user_id)