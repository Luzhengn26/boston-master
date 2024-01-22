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
topped_up as (select distinct expenses.user_id,
                expenses.txn_ts                                                  as expense_ts,
                expenses.txn_id                                                  as expense_txn_id,
                top_ups.txn_ts                                                   as top_up_ts,
                top_ups.txn_id                                                   as top_up_txn_id,
                round(expenses.abs_amount_cents/100,0)                           as expense_amount_eur,
                round(top_ups.abs_amount_cents/100,0)                            as top_up_amount_eur

from dbt.zrh_transactions expenses
         inner join all_users au on au.user_id = expenses.user_id
         left join dbt.zrh_rule_txn zrt on zrt.txn_id = expenses.txn_id
         inner join dbt.zrh_transactions top_ups on top_ups.user_id = expenses.user_id 
                                                 and top_ups.amount_cents > 0 
                                                 and top_ups.txn_ts::date = expenses.txn_ts::date
where expenses.amount_cents < 0
  and expenses.txn_ts >= current_date - interval '6 months'
  and expenses.n26_init_txn_flg = false
  and expenses.type not in ('AA', 'DR', 'PF', 'PRESENTMENT_REFUND', 'WU')
  and round((expenses.abs_amount_cents/100),0) = round((top_ups.abs_amount_cents/100),0)
  and expenses.internal_txn_flg = false
  and expenses.abs_amount_cents >= 1000
  and (action_type not in ('INCOME_SORTER', 'INCOME_SORTER_FIXED_AMOUNT', 'ROUND_UP') or action_type is null))

select distinct au.*
from all_users au
         inner join topped_up using (user_id)