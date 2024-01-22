with prev_limit as (
    select 
        user_created,
        user_id,
        to_char(created, 'YYYY-MM-DD') as created,
        to_char(created, 'YYYY-MM') as month,
        amount_cents::numeric/100 as limit_eur, 
        type, 
        status,
        changed_by,
        lag(amount_cents::numeric/100) over (partition by user_created order by created) as prev_limit_eur,
        lag(status) over (partition by user_created order by created) as prev_status
    from pu_overdraft_history
)
select 
    *, 
    prev_limit_eur - limit_eur as reduced_volume
from prev_limit
where changed_by = 'monitoring'
	and created > '2021-06-01'
    and created <= '2021-12-31' 
    and prev_limit_eur > limit_eur