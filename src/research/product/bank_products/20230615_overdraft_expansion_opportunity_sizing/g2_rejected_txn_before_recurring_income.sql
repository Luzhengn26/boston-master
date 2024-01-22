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
                                           and cmd_address.country in ('DEU','ESP') -- DE/ES based users
                   where u.closed_at is null       -- account still open
                     and tnc_country_group in ('DEU','ESP') -- DE/ES TnC
                     and kyc_first_completed <= current_date - interval '6 months'
), -- table to obtain DEU/ESP users who have at least 6 months of activity and did not close their acounts

     rejected_transaction as (select np_collection.user_id,
                                     np_collection.created
                              from np_collection
                                       inner join all_users au on np_collection.user_id = au.user_id
                              where np_collection.created >= current_date - interval '6 months'
                                and np_collection.return_reason = 'INSUFFICIENT_FUNDS'

                              union all

                              select cta.user_id,
                                     cta.created
                              from dbt.card_transactions_ar cta
                                       inner join all_users au on cta.user_id = au.user_id
                              where cta.rejection_reason = 'INSUFFICIENT_FUNDS'
                                and cta.created >= current_date - interval '6 months'),
                                -- rejected transaction (card txn or recurring payment) last 6 months

     regular_income AS (SELECT zt.user_id,
                               zt.txn_ts,
                               zt.user_id || zt.amount_cents/100 || coalesce(merchant_name, partner_bic) || CEIL(EXTRACT(DAY FROM txn_ts) / 10.0) as label,
                               COUNT(to_char(txn_ts, 'YYYY-MM'))
                               OVER (partition by label) as num_months
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
                        GROUP BY 1, 2, 3)

select distinct all_users.*
from rejected_transaction rt
         inner join regular_income ri on rt.user_id = ri.user_id
         and rt.created < ri.txn_ts
         and (ri.txn_ts::date - rt.created::date) <= 6
inner join all_users on rt.user_id = all_users.user_id
where ri.num_months >= 3