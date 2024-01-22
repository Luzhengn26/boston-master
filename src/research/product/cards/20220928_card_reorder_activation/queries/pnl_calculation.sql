with add_card as (
	select
		c.id,
		c.user_created,
		c.order_date,
		c.replaced_card_id,
		case
			when c.order_id ilike '%auto%'
				then 'Batch reissue'
				else 'Sync reissue'
		end as reissue_type,
		sum((tx.id is not null)::int) > 0 as card_used
	from dbt.zrh_cards c
	left join dbt.zrh_cards ac
		on c.user_created = ac.user_created
		and c.id != ac.id
		and ac.id != c.replaced_card_id
		and c.order_date between ac.order_date and coalesce(ac.card_terminated, '2200-01-01')
		and ac.card_design_basic != 'wirecard'
	left join dbt.card_transactions_pt tx
		on c.id = tx.card_id
	where c.order_date between '2021-01-01'::date and current_date - 25
		and not c.is_digital
		and c.reissue_reason = 'EXPIRED'
	group by 1, 2, 3, 4, 5
), old_card as (
	select
		add_card.id,
		date_diff('month', max(created)::date, add_card.order_date::date) <= 6 as not_abandoned
	from dbt.card_transactions_aa tx
	inner join add_card
		on tx.card_id = add_card.replaced_card_id
	group by 1, add_card.order_date
), mcu_level_raw as (
	select
		c.id,
		c.card_used,
		c.user_created,
		c.reissue_type,
		date_diff('day', u.ft_mau::date, c.order_date::date) as active_days,
		max(mcu.activity_end) as last_activity_end,
		sum((mcu.user_created is not null)::int) as mcu_periods,
		coalesce(sum(date_diff('day', mcu.activity_start::date, least(c.order_date, mcu.activity_end)::date)), 0) as mcu_days,
		mcu_days::float / nullif(active_days, 0) as mcu_rate,
		case
			when last_activity_end > c.order_date then 'Active'
			when date_diff('month', last_activity_end::date, c.order_date::date) > 6 then 'Lapsed'
			when date_diff('month', last_activity_end::date, c.order_date::date) <= 6 then 'Dormant'
		end as mcu_status
	from add_card c
	inner join dbt.zrh_users u
			on c.user_created = u.user_created
	left join dbt.mcu_activity mcu
		on c.user_created = mcu.user_created
		and mcu.activity_start < c.order_date
	group by 1, 2, 3, 4, 5, c.order_date
), mcu_level as (
	select
		mcu.id,
		mcu.card_used,
		mcu.user_created,
		mcu.reissue_type,
		case
			when mcu.mcu_periods = 0 then 'Potential'
			when mcu.mcu_status != 'Active' then mcu.mcu_status
			when mcu.mcu_rate > 0.9 then 'Continuous'
			else 'Sporadic/Undecided'
		end as mcu_category
	from mcu_level_raw mcu
), tx_sum as (
    select
        to_char(created, 'yyyy-mm') as month,
        user_created,
        card_id,
        count(*) as tx,
        sum(amount_eur) as volume
    from dbt.card_transactions_aa
    where user_created in (select distinct user_created from mcu_level)
    group by 1, 2, 3
), card_pre_split as (
    select
        month,
        user_created,
        card_id,
        tx::float / sum(tx) over (partition by month, user_created) as tx_rate,
        volume::float / sum(volume) over (partition by month, user_created) as volume_rate
    from tx_sum
), card_split as (
	select 
		*
	from card_pre_split
	where card_id in (select distinct id from mcu_level)
), mpts_processing as (
    select
        c.card_id,
        c.user_created,
        sum(mptsp.value * c.tx_rate) / 100 as mpts_processing_cost
    from dbt.ucm_mpts_processing mptsp
    inner join card_split c
        using (month, user_created)
    group by 1, 2
), mdes_token_costs as (
    select
        card_id,
        count(*) * -0.4 as mdes_token_cost
    from mizzium_card_tokens
    where card_id in (select distinct id from mcu_level)
    group by 1
), mdes_processing_costs as (
    select
        card_id,
        count(*) * -0.00004 as mdes_processing_cost
    from dbt.card_transactions_pt
	where wallet in ('APPLE_PAY', 'GOOGLE_PAY')
	    and card_id in (select distinct id from mcu_level)
	group by 1
), transaction_pnl as (
    select
        t.card_id,
        sum(coalesce(t.issuer_fee, 0) + coalesce(f.fee_value / 100, 0)) as tx_pnl
    from dbt.card_transactions_pt t
    left join dbt.ucm_stg_payments_fees_txns f
        on t.id = f.pt_txn_id
        and f.payment_fee in ('FAIR_USE_ATM', 'FX_MARKUP', 'FLEX_ACCOUNT_ATM', 'MAESTRO_FEE')
    where t.card_id in (select distinct id from mcu_level)
    group by 1
), card_pnl as (
    select
        c.id,
        coalesce(mptsi.value / mptsi.cards_per_user, 0) / 100 as mpts_issuing_cost,
        coalesce(mpts_processing_cost, 0)
            + coalesce(mdes_processing_cost, 0)
            + coalesce(tx_pnl, 0)
            as processing_pnl,
        (processing_pnl / datediff('day', c.order_date::date, current_date::date)) * 365 + mpts_issuing_cost + coalesce(mdes_token_cost, 0) as first_year_pnl
    from dbt.zrh_cards c
    left join dbt.ucm_mpts_issuing mptsi
        on c.user_created = mptsi.user_created
        and to_char(c.order_date, 'yyyy-mm') = mptsi.month
    left join mpts_processing mptsp
        on c.id = mptsp.card_id
    left join mdes_token_costs mdest
        on c.id = mdest.card_id
    left join mdes_processing_costs mdesp
        on c.id = mdesp.card_id
    left join transaction_pnl tpnl
        on c.id = tpnl.card_id
    where c.id in (select distinct id from mcu_level)
), base as (
	select
		mcu_level.mcu_category,
		mcu_level.reissue_type,
		case when mcu_category not in ('Lapsed', 'Potential') then coalesce(not old_card.not_abandoned, true) end as old_card_abandoned,
		count(1) as cards,
		avg(mcu_level.card_used::int::float) as usage_rate,
		case when mcu_category not in ('Lapsed', 'Potential') then sum(mcu_level.card_used::int * first_year_pnl) end as used_first_year_pnl,
		sum((not mcu_level.card_used)::int * first_year_pnl) as not_used_first_year_pnl
	from mcu_level
	left join old_card
		on mcu_level.id = old_card.id
	left join card_pnl
		on mcu_level.id = card_pnl.id
	group by 1, 2, 3
)
select 
	mcu_category || ' MCU'
		|| ' & ' || reissue_type
		|| case when mcu_category in ('Lapsed', 'Potential') then '' else case when old_card_abandoned then ' & card abandoned' else ' & card used' end end
		as category,
	cards,
	round(usage_rate * 100, 1)::text || '%' as usage_rate,
	case when usage_rate <= 0.3 then 'No reissue' else 'Reissue' end as decision,
	coalesce('€' || round(used_first_year_pnl / cards, 2)::text, '-') as used_first_year_pnl,
	'€' || round(not_used_first_year_pnl / cards, 2)::text as not_used_first_year_pnl,
	'€' || round(not_used_first_year_pnl + coalesce(used_first_year_pnl, 0))::text as total_pnl
from base
order by 2 desc
