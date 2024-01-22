with new_card as (
    select
        c.id,
        c.order_date,
        c.replaced_card_id,
        sum((tx.id is not null)::int) > 0 as card_used
    from dbt.zrh_cards c
    left join dbt.card_transactions_pt tx
        on c.id = tx.card_id
    where c.order_date between '2021-01-01'::date and current_date - 25
        and not c.is_digital
        and c.reissue_reason = 'EXPIRED'
    group by 1, 2, 3
), old_card as (
    select
        new_card.id,
        max(created) as last_tx
    from dbt.card_transactions_aa tx
    inner join new_card
        on tx.card_id = new_card.replaced_card_id
    group by 1
), counts as (
    select 
        date_diff('month', old_card.last_tx::date, new_card.order_date::date) as months_since_last_tx,
        count(*) as cards,
        sum(card_used::int) as used_cards,
        sum((not card_used)::int) as not_used_cards
    from new_card
    inner join old_card 
        using (id)
    group by 1
), all_data as (
select
    *,
    sum(not_used_cards)     over (order by months_since_last_tx rows between current row and unbounded following) as true_positive,
    sum(used_cards)     over (order by months_since_last_tx rows between current row and unbounded following) as false_positive,
    coalesce(true_positive,0)::float/ (true_positive + false_positive) as ppv,
    
    sum(used_cards) over (order by months_since_last_tx rows between unbounded preceding and 1 preceding) as true_negative,
    sum(not_used_cards) over (order by months_since_last_tx rows between unbounded preceding and 1 preceding) as false_negative,
    coalesce(true_negative,0)::float/ (true_negative + false_negative) as npv,
    
    coalesce(true_positive,0)::float/ (true_positive + false_negative) as perc_positive_detected,
    coalesce(true_negative,0)::float/ (true_negative + false_positive) as perc_negative_detected,
    
    sum(cards) over () as sample_total,
    (coalesce(true_positive,0) + coalesce(true_negative,0))::float/sum(cards) over()::float as accuracy,
    sum(cards) over (order by months_since_last_tx rows between current row and unbounded following)::float / sum(cards) over()::float as penetration,
    true_positive::float / sum(cards) over()::float as penetration_tp
from
    counts
)
select 
    months_since_last_tx,
	cards,
	accuracy,
	perc_negative_detected,
	perc_positive_detected,
	round(accuracy * 100)::text || '% correct predictions' as accuracy_text,
	round(perc_positive_detected * 100)::text || '% of non-activations predicted correctly' as corr_non_activations_text,
	round(perc_negative_detected * 100)::text || '% of activations predicted correctly' as corr_activations_text
from all_data 
where months_since_last_tx between -2 and 18
