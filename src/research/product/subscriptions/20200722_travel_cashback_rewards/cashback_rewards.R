#setwd('/Users/wendyvu/Documents/transaction_tag_usage/')
library(n26)
library(data.table)


travelers <- queryDB("
with members as (
select 
	user_created,
	kyc_c,
	product_id
from dbt.zrh_user_product
where status = 'ACTIVE' and kyc_c <= '2020-01-01'::date
), cards as (
select m.*,
    txn_condition,
    original_currency,
    mcc_category,
    card_tx_type,
    region_group,
    created
from members m 
left join dbt.zrh_card_transactions c 
    on c.user_created = m.user_created 
where c.type = 'PT' and c.created >= '2020-01-01'::date
), card_agg as (
select 
    user_created,
    product_id,
    case when card_tx_type in ('cardpresent','atm') then 'cardpresent' else card_tx_type end as card_tx_type,
    case when region_group in ('inter','intra') 
    	and original_currency not in ('EUR','GBR','BGN','CZK','DKK','HUF','ISK','EEK','HRK','NOK','PLN','RON','SEK') 
    	then 'foreign' else region_group end as region_group,
    date_trunc('months',created)::date as month,
    count(*) as cnt 
from cards
group by 1,2,3,4,5
), card_months as (
select 
	user_created,
	product_id,
	card_tx_type,
	region_group,
	count(distinct month) as months 
from card_agg 
group by 1,2,3,4
order by 1,3
)
select 
	product_id,
	count(distinct user_created) as users,
	count(distinct case when card_tx_type = 'cardpresent' and region_group = 'foreign' and months > 1 then user_created end ) as month_1,
	count(distinct case when card_tx_type = 'cardpresent' and region_group = 'foreign' and months > 2 then user_created end ) as month_2,
	count(distinct case when card_tx_type = 'cardpresent' and region_group = 'foreign' and months > 3 then user_created end ) as month_3
from card_months 
where product_id in ('STANDARD','METAL_CARD_MONTHLY','BLACK_CARD_MONTHLY')
group by 1
limit 500;

" , "redshift-eu")

travel_purchases <- queryDB("
with members as (
select 
	user_created,
	kyc_c,
	product_id
from dbt.zrh_user_product
where status = 'ACTIVE' and kyc_c <= '2020-01-01'::date
), cards as (
select m.*,
    txn_condition,
    original_currency,
    case when mcc_category in ('travel_tour_agencies','airline','hotel_lodging') then 'airline_travel_hotel' else 'non-travel' end as mcc,
    card_tx_type,
    case when original_currency not in ('EUR','GBR','BGN','CZK','DKK','HUF','ISK','EEK','HRK','NOK','PLN','RON','SEK') -- exclude EEA countries
    	and region_group in ('inter', 'intra') 
    	then 'foreign' else region_group end as region_group,
    created,
    round(amount_cents_eur::float/100,2) as amount_eur
from members m 
left join dbt.zrh_card_transactions c 
    on c.user_created = m.user_created 
where c.type = 'PT' and c.created >= '2020-01-01'::date
)
select 
    count(distinct user_created) as users,
    count(distinct case when mcc = 'airline_travel_hotel' and region_group = 'foreign' then user_created end) as users_air_travel_hotel,
    sum(distinct case when mcc = 'airline_travel_hotel' and region_group = 'foreign' then amount_eur end) as vol_air_travel_hotel
from cards

" , "redshift-eu")

txns <- queryDB("



" , "redshift-eu")

ecommerce <- queryDB("
with members as (
select 
	user_created,
	kyc_c,
	product_id
from dbt.zrh_user_product
where status = 'ACTIVE' and kyc_c <= '2020-01-01'::date
), cards as (
select m.*,
    txn_condition,
    original_currency,
    case when mcc_category in ('travel_tour_agencies','airline','hotel_lodging') then 'airline_travel_hotel' else 'non-travel' end as mcc,
    card_tx_type,
    case when original_currency not in ('EUR','GBR','BGN','CZK','DKK','HUF','ISK','EEK','HRK','NOK','PLN','RON','SEK') -- exclude EEA countries
    	and region_group in ('inter', 'intra') 
    	then 'foreign' else region_group end as region_group,
    created
from members m 
left join dbt.zrh_card_transactions c 
    on c.user_created = m.user_created 
where c.type = 'PT' and c.created >= '2020-01-01'::date
)
select 
    count(distinct user_created) as users,
    count(distinct case when card_tx_type = 'ecomm' and region_group = 'foreign' then user_created end) as users_air_travel_hotel
from cards;

" , "redshift-eu")

ecommerce_merchants <- queryDB("
                               
with members as (
select 
	user_created,
	kyc_c,
	product_id
from dbt.zrh_user_product
where status = 'ACTIVE' and kyc_c <= '2020-01-01'::date
), cards as (
select m.*,
    txn_condition,
    original_currency,
    case when mcc_category in ('travel_tour_agencies','airline','hotel_lodging') then 'airline_travel_hotel' else 'non-travel' end as mcc,
    card_tx_type,
    case when original_currency not in ('EUR','GBR','BGN','CZK','DKK','HUF','ISK','EEK','HRK','NOK','PLN','RON','SEK') -- exclude EEA countries
    	and region_group in ('inter', 'intra') 
    	then 'foreign' else region_group end as region_group,
    created,
    merchant_name
from members m 
left join dbt.zrh_card_transactions c 
    on c.user_created = m.user_created 
where c.type = 'PT' and c.created >= '2020-01-01'::date
), merch_name as (
select merchant_name,
	count(*) as txn_cnt
from cards 
where card_tx_type = 'ecomm' 
	and region_group = 'foreign' 
group by 1
), brands as (
select 
    cm.id as merchant_id,
    cm.title,
	cm.mcc,
	cm.brand_id,
	cm.created,
	cm.match_rule_id,
	cm.third_party_payment_provider,
	cb.name
from etl_reporting.cro_merchant cm 
join etl_reporting.cro_brand cb 
	on cm.brand_id = cb.id 
), clean_names as (
select 
	m.merchant_name,
	b.title,
	b.name,
	m.txn_cnt,
	case when name is not null then b.name else m.merchant_name end as merch_name 
from merch_name m 
left join brands b
	on b.title = m.merchant_name
group by 1,2,3,4
)
select merch_name, 
	sum(txn_cnt) as txn_cnt 
from clean_names
group by 1
order by 2 desc
limit 100;                
                               
", "redshift-eu")

save(travelers,
     travel_purchases,
     ecommerce,
     ecommerce_merchants,
     file = file.path("early_topup_behavior.RData"))