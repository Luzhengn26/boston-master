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
            sum((tx.id is not null)::int) > 0 as card_used,
            sum((ac.id is not null)::int) > 0 as has_other_card
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
            c.has_other_card,
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
            mcu.has_other_card,
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
            coalesce(not old_card.not_abandoned, true) as old_card_abandoned,
			--mcu_level.has_other_card,
			mcu_level.reissue_type,
            count(*) as cards,
            sum(mcu_level.card_used::int::float) as used_cards
        from mcu_level
        left join old_card
            on mcu_level.id = old_card.id
        group by 1, 2, 3--, 4
    ), pre_window as (
    select
        mcu_category,
		case reissue_type when 'Sync reissue' then 'Sync' else 'Batch' end 
			|| case when mcu_category in ('Lapsed', 'Potential') then '' else case when old_card_abandoned then ' & abandoned' else ' & used' end end 
			--|| case when has_other_card then ' & 1+c' else ' & 1c' end 
			as category,
		sum(cards) as cards,
		sum(used_cards) as used_cards,
		sum(used_cards)::float / sum(cards) as usage_rate,
        round(sum(cards)::float/1000)::text || 'k cards reissued' as cards_text,
        round(usage_rate * 100, 1)::text || '% used' as usage_text
    from base
	group by 1, 2
)
select 
	*,
	used_cards::float / sum(used_cards) over () as used_card_share,
	(cards - used_cards)::float / sum(cards - used_cards) over () as unused_card_share,
	round(used_card_share * 100, 1)::text || '% of all used cards' as used_share_text,
	round(unused_card_share * 100, 1)::text || '% of all not-used cards' as unused_share_text
from pre_window
