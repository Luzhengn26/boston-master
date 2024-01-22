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
	group by 1, 2, 3, 4, c.order_date
), mcu_level as (
	select
		mcu.id,
		mcu.card_used,
		mcu.reissue_type,
		case
			when mcu.mcu_periods = 0 then 'Potential'
			when mcu.mcu_status != 'Active' then mcu.mcu_status
			when mcu.mcu_rate > 0.9 then 'Continuous'
			else 'Sporadic/Undecided'
		end as mcu_category
	from mcu_level_raw mcu
), base as (
	select
		mcu_level.mcu_category,
		case when mcu_category in ('Lapsed', 'Potential') then null else coalesce(not old_card.not_abandoned, true) end as old_card_abandoned,
		count(*) as cards,
		sum(mcu_level.card_used::int::float) as used_cards,
		sum(case when reissue_type = 'Sync reissue' then 1 end) as sync_cards,
		sum(case when reissue_type = 'Sync reissue' then mcu_level.card_used::int end) as sync_used,
		sum(case when reissue_type = 'Batch reissue' then 1 end) as batch_cards,
		sum(case when reissue_type = 'Batch reissue' then mcu_level.card_used::int end) as batch_used
	from mcu_level
	left join old_card
		on mcu_level.id = old_card.id
	group by 1, 2
), final_sum as (
    select
        row_number() over (order by used_cards::float / cards desc) as rn,
        mcu_category || case when mcu_category in ('Lapsed', 'Potential') then '' else case when old_card_abandoned then ' & abandoned' else ' & used' end end as category,
        used_cards::float / cards as usage_rate_before,
        cards as cards_reissued_before,
        case
            when sync_used::float / sync_cards > 0.3
                then 'Sync reissue'
            else 'No sync'
        end as sync_decision,
        sync_used::float / sync_cards as sync_used_rate,
        case
            when batch_used::float / batch_cards > 0.3
                then 'Batch reissue'
            else 'No reissue'
        end as batch_decision,
         batch_used::float / batch_cards as batch_used_rate,
        ((sync_decision = 'Sync reissue')::int * sync_used + (batch_decision = 'Batch reissue')::int * batch_used)::float
            / 	nullif(((sync_decision = 'Sync reissue')::int * sync_cards + (batch_decision = 'Batch reissue')::int * batch_cards), 0) as usage_rate_after,
        ((sync_decision = 'Sync reissue')::int * sync_cards + (batch_decision = 'Batch reissue')::int * batch_cards) as cards_reissued_after,
        (cards_reissued_after - cards)::float / cards as reissued_volume_change
    from base
)
select 
	*
from
(
	select
		rn,
		category,
		round(usage_rate_before * 100, 0)::text || '%' as usage_rate_before,
		cards_reissued_before,
		sync_decision,
		round(sync_used_rate * 100, 0)::text || '%' as sync_used_rate,
		batch_decision,
		round(batch_used_rate * 100, 0)::text || '%' as batch_used_rate,
		coalesce(round(usage_rate_after * 100, 0)::text || '%', '') as usage_rate_after,
		cards_reissued_after,
		round(reissued_volume_change * 100, 0)::text || '%' as reissued_volume_change
	from final_sum
	union all
	select
		10 as rn,
		'Total' as category,
		round(sum(usage_rate_before * cards_reissued_before)::float / sum(cards_reissued_before) * 100)::text || '%' as usage_rate_before,
		sum(cards_reissued_before),
		'' as sync_decision,
		'' as sync_used_rate,
		'' as batch_decision,
		'' as batch_used_rate,
		round(sum(usage_rate_after * cards_reissued_after)::float / sum(cards_reissued_after) * 100)::text || '%' as usage_rate_after,
		sum(cards_reissued_after),
		round((sum(cards_reissued_after) - sum(cards_reissued_before))::float / sum(cards_reissued_before) * 100)::text || '%'
	from final_sum
)
order by 1 
