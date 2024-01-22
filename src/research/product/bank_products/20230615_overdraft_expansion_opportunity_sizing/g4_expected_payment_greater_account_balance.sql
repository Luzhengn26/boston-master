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
                     and kyc_first_completed <= current_date - interval '6 months' --users with at least 6 months of activity
),

     regular_payment AS (SELECT zt.user_id,
                                zt.txn_ts,
                                round(zt.amount_cents / 100)                                as amount_eur,
                                zt.user_id || zt.amount_cents / 100 || coalesce(merchant_name, partner_bic) ||
                                CEIL(EXTRACT(DAY FROM txn_ts) / 10.0)                       as label,
                                COUNT(to_char(txn_ts, 'YYYY-MM')) OVER (partition by label) as num_months
                         FROM all_users
                                  inner join dbt.zrh_transactions zt
                                             on zt.user_id = all_users.user_id
                                                 and zt.type not in ('AA', 'DR', 'PF', 'PRESENTMENT_REFUND', 'WU')
                                                 and internal_txn_flg = false
                                  left join zr_transaction on zt.txn_id = zr_transaction.id
                                  left join dbt.zrh_rule_txn zrt on zrt.txn_id = zt.txn_id
                         where txn_ts >= date_trunc('month', current_date) - interval '6 month'
                           and amount_cents < 0
                           and abs_amount_cents >= 500
                           and (action_type not in
                                ('INCOME_SORTER', 'INCOME_SORTER_FIXED_AMOUNT', 'ROUND_UP', 'SPACES_MONEY_TRANSFER')
                             or
                                action_type is null)
                         GROUP BY 1, 2, 3, 4),

     balance_less_than_recurring_payment AS
         (select rp.*,
                 sum(balance_eur) as account_balance
          from regular_payment rp
                   inner join dbt.mmb_daily_balance_aud mdb on mdb.user_id = rp.user_id and mdb.date >= date_trunc('month', current_date) - interval '6 month'
              and (rp.txn_ts::date - mdb.date::date) = 1 -- Not enough balance the day before the recurring txn
          where rp.num_months >= 3
          group by 1, 2, 3, 4, 5
          having account_balance < amount_eur)

select all_users.*
from balance_less_than_recurring_payment
left join all_users using (user_id)
