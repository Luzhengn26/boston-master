with base as (
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
)
select 
	reissue_type,
	count(*) as cards,
	sum(card_used::int) as used_cards,
	used_cards::float / cards as usage_rate,
	round(cards::float/1000)::text || 'k cards reissued' as cards_text,
	round(usage_rate * 100, 1)::text || '% used' as usage_text
from base
group by 1
