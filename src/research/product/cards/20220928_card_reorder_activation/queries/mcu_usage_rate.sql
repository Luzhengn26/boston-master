with add_card as (
    select
        c.id,
        c.user_created,
        c.order_date,
        sum((tx.id is not null)::int) > 0 as card_used
    from dbt.zrh_cards c
    left join dbt.card_transactions_pt tx
        on c.id = tx.card_id
    where c.order_date between '2021-01-01'::date and current_date - 25
        and not c.is_digital
        and c.reissue_reason = 'EXPIRED'
    group by 1, 2, 3
), mcu_level_raw as (
    select
        c.id,
        c.card_used,
        date_diff('day', u.ft_mau::date, c.order_date::date) as active_days,
        max(mcu.activity_end) as last_activity_end,
        sum((mcu.user_created is not null)::int) as mcu_periods,
        coalesce(sum(date_diff('day', mcu.activity_start::date, least(c.order_date, mcu.activity_end)::date)), 0) as mcu_days,
        mcu_days::float / nullif(active_days, 0) as mcu_rate,
        case
            when last_activity_end > c.order_date then 'Active'
            when date_diff('month', last_activity_end::date, c.order_date::date) >= 6 then 'Lapsed'
            when date_diff('month', last_activity_end::date, c.order_date::date) < 6 then 'Dormant'
        end as mcu_status
    from add_card c
    inner join dbt.zrh_users u
            on c.user_created = u.user_created
    left join dbt.mcu_activity mcu
        on c.user_created = mcu.user_created
        and mcu.activity_start < c.order_date
    group by 1, 2, 3, c.order_date
), mcu_level as (
    select
        mcu.id,
        mcu.card_used,
        case
            when mcu.mcu_periods = 0 then 'Potential'
            when mcu.mcu_status != 'Active' then mcu.mcu_status
            when mcu.mcu_rate > 0.9 then 'Continuous'
            else 'Sporadic/Undecided'
        end as mcu_category
    from mcu_level_raw mcu
), base as (
	select 
		mcu_category,
		count(*) as customers,
		sum(card_used::int) as usage_rate
	from mcu_level
	group by 1
)
select 
	*,
	round(customers::float/1000)::text || 'k cards reissued' as cards_text,
	round(usage_rate::float / customers * 100, 1)::text || '% used' as usage_text
from base
