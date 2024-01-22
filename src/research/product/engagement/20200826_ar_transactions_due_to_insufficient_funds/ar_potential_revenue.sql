with ar_labels as (
select 
id,
txn_amount_eur,
date_trunc('month', txn_created) as month,
tnc_country_group, 
product_id,
user_created || txn_created::date || round(txn_amount_eur*100) || coalesce(merchant_name, '') || coalesce(terminal_id, '') || coalesce(mcc::text, '') as label
from dev_dbt.temp_ar_insuf_funds taif
),
aa_pt_labels as (
select label,
count(case when type = 'AA' then 1 end) as aa_count,
count(case when type = 'PT' then 1 end) as pt_count
from dev_dbt.temp_ar_insuf_funds_with_pts 
group by 1
),
totals as (
select 
al.*,
case when aa_count >0 then 1 end as has_aa, 
case when pt_count >0 then 1 end as has_pt
from ar_labels al
left join aa_pt_labels apl
using (label) 
),
sums as(
select
month::date,
tnc_country_group, 
product_id, 
sum(has_aa) as ar_with_repeated_aa_count,
sum(has_pt) as ar_with_also_pt_count,
count(*) as ar_count
from totals
group by 1, 2, 3
),
fees as (
select 
date_trunc('month', created)::date as month,
u.tnc_country_group, 
coalesce(up.product_id, 'Other') as product_id,
count(*) as pt_count,
sum(issuer_fee_cents_eur) as sum_fee_value,
sum_fee_value::numeric/pt_count::numeric as avg_fee_per_pt
from dbt.zrh_card_transactions zct 
inner join dbt.zrh_users u using (user_created)
left join dbt.zrh_user_product up
on u.user_created = up.user_created
and created between subscription_valid_from  and subscription_valid_until
where type = 'PT'
and created between '2020-06-01' and '2020-08-31' 
group by 1,2,3
)
select month,
tnc_country_group, 
product_id,
ar_count,
ar_with_also_pt_count,
round(avg_fee_per_pt::numeric/100, 2) as avg_fee_per_pt_eur,
ar_count * avg_fee_per_pt_eur as all_ar_estimated_fee,
ar_with_also_pt_count * avg_fee_per_pt_eur ar_with_pt_estimated_fee,
all_ar_estimated_fee - ar_with_pt_estimated_fee ar_excl_pt_estimated_fee
from sums
left join fees using (month, tnc_country_group, product_id)
