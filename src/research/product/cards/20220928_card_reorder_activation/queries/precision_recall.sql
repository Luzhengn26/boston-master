with add_card as (
    select
        c.id,
        c.user_created,
        c.order_date,
        c.card_activated is not null as card_activated,
        case
            when c.order_id ilike '%auto%'
                then 'Batch reissue'
                else 'Sync reissue'
        end as reissue_type,
        sum((ac.id is not null)::int) > 0 as has_other_card,
        sum((tx.id is not null)::int) > 0 as card_used
    from dbt.zrh_cards c
    left join dbt.zrh_cards ac
        on c.user_created = ac.user_created
        and c.id != ac.id
        and coalesce(c.card_activated, c.order_date) between ac.order_date and coalesce(ac.card_terminated - 5, current_date)
        and coalesce(ac.card_terminated, '2200-01-01') > c.order_date + 180
    left join dbt.card_transactions_pt tx 
        on c.id = tx.card_id
    where c.order_date between '2021-01-01'::date and current_date - 25
        and not c.is_digital
        and c.reissue_reason = 'EXPIRED'
    group by 1, 2, 3, 4, 5
), mcu_level_raw as (
    select
        c.id,
        c.has_other_card,
        c.card_used,
        c.order_date,
        c.reissue_type,
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
    group by 1, 2, 3, 4, 5, 6
), mcu_level as (
    select
        id,
        card_used,
        has_other_card,
        reissue_type,
        case
            when mcu_periods = 0 then 'Potential'
            when mcu_status != 'Active' then mcu_status
            when mcu_rate > 0.9 then 'Continuous'
            else 'Sporadic/Undecided'
        end as mcu_category
    from mcu_level_raw
), base as (
    select
        case 
            when has_other_card 
                then 'Has 2nd card'
                else 'No 2nd card'
        end as additional_card,
        mcu_category,
        reissue_type,
        count(*) as cards,
        sum(card_used::int) as activated,
        sum((not card_used)::int) as not_activated
    from mcu_level
    group by 1, 2, 3
)
select
    additional_card,
    mcu_category,
    reissue_type,
    additional_card || ' and ' || lower(reissue_type) as subcategory,
    cards as sample_size,
    cards::float / sum(cards) over () as prevalence,
    activated::float / (activated + not_activated) as precision_activate,
    activated::float / sum(activated) over () as recall_activate,
    not_activated::float / (activated + not_activated) as precision_not_activate,
    not_activated::float / sum(not_activated) over () as recall_not_activate,
    'Percentage of all activated: ' || round(recall_activate * 100, 1)::text || '%' as activated_percentage,
    'Percentage of all not-activated: ' || round(recall_not_activate * 100, 1)::text || '%' as unactivated_percentage
from base;