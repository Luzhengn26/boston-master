library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/Hierarchical_Kmean_Clustering","data")

clustering_query <- queryDB("

-- kycc cohort for the month of Oct 2018
-- filter out lapsed users
drop table if exists kycc;
create temp table kycc as 
select 
	row_number() over(order by a.user_created) as id,
	a.user_created,
	a.user_id,
	a.kyc_first_completed as kycc,
	to_char(date_trunc('months',kycc),'YYYY-MM') as cohort,
	case when is_premium is false then 'standard' else 'premium' end as membership,
	product_id,
	case when country_tnc_legal in ('DEU', 'FRA','ITA', 'ESP', 'GBR', 'AUT') THEN country_tnc else 'RoE' end as country_tnc_legal,
	case when country_shipping in ('DEU', 'FRA','ITA', 'ESP', 'GBR', 'AUT') THEN country_shipping else 'RoE' end as country_shipping,
	b.nationality,
	a.closed_at,
	date_diff('days',kycc,a.closed_at) as kycc_closed_days,
	a.gender,
	date_diff('years',b.birth_date, current_date) as age,
	age_group,
	case when is_expat is true then 'expat' else 'native' end as nat_status,
	last_click_source as referral,
	c.continent_insee as continent,
	c.long_form as cntry_nationality
from dbt.zrh_users a
join cmd_users b 
	on a.user_created = b.user_created 
join dwh_country_codes c
	on c.three_letter = b.nationality 
where kyc_first_completed between '2018-09-01' and '2018-10-31'
order by 2 



--WAU/SAU
drop table if exists sau;
create temp table sau as
SELECT
	k.user_created,
	k.kycc,
	a.activity_start,
	a.activity_end,
	a.activity_type,
	case when a.activity_end > kycc + interval '420 days' then kycc + interval '420 days' else a.activity_end end as act_end,
	ceil(date_diff('days',a.activity_start,act_end)::float/35) as months
from kycc k 
left join dwh_user_activity a 
	on k.user_created = a.user_created
	and activity_type in (3,4)
where activity_start between k.kycc and k.kycc + interval '420 days'
order by 1,4 


create temp table wau_sau as 
select 
	user_created,
	--sum(case when activity_type = 4 then weeks else null end) weeks_wau, -- not a good measure of weeks active FROM THIS TABLE
	sum(case when activity_type = 3 then months else null end) as months_sau
from sau 
group by 1
order by 1


-- 	WAU

drop table if exists activity;
create temp table activity as
select 
	k.id,
	k.user_created,
	kycc,
	t.activity_type,
	t.activity_start,
	t.activity_end,
	case when t.activity_end > kycc + interval '420 days' then kycc + interval '420 days' else t.activity_end end as act_end,
	date_diff('days',activity_start,act_end) as days
from kycc k 
join dbt.zrh_user_activity_txn t 
	on k.user_created = t.user_created
where activity_start between kycc and kycc + interval '420 days'
order by 1,4,5


drop table if exists wau;
create temp table wau as 
select 
	id,
	user_created,
	kycc,
	ceil(sum(case when activity_type = '1_tx_7' then days end)::float/7) as weeks_wau_txn,
	ceil(sum(case when activity_type = '1_tx_35' then days end)::float/35) as mau_txn
from activity 
--where activity_start between kycc and kycc + interval '420 days'
group by 1,2,3
order by 1


--- ACTIVITY GROUPINGS based on mau defined by >= 1 deposit per 35 day period
drop table if exists mau_dep;
create temp table mau_dep as 
select 
    k.user_created,
    k.kycc,
    z.txn_date,
    z.value,
    datediff('days',kycc,z.txn_date) as days_kycc_ct,
    ceil(days_kycc_ct::float/35) as period --- period corresponds to 35 days, which is considered a month
from kycc k
join dbt.zrh_txn_day_rows z
    on k.user_created = z.user_created 
        and feature in ('amount_cents_ct','amount_cents_cash26_in','amount_cents_ft_in','amount_cents_stripetopup_in')
where z.txn_date between kycc and kycc + interval '420 days'
order by 1,5



--- Total monthly active groups
drop table if exists mau_act;
create temp table mau_act as 
select 
    user_created,
    count(period) as mau_act --- total number of months active groupings
from (select user_created, period from mau_dep group by 1,2)  
group by 1
order by 1


-- TRANSACTION TYPES AND SUM AMOUNT ASSOCIATED 

--CASHOUT
-- CARD TRANSACTIONS PTs and FOREIGN/DOMESTIC TRANSACTIONS
drop table if exists dev.pt;
create table dev.pt as 
select 
	k.user_created,
	tx.created,
	type,
	tx.card_tx_type,
	coalesce(rate,1)*amount_cents::float/100 as amount_euro,
	case when mcc between 3000 and 3299 then 'airline'
		when mcc in (4511) then 'airline'
		when mcc between 3351 and 3441 then 'car_rental'
		when mcc between 4201 and 3831 then 'hotel_lodging'
		when mcc in (5411) then 'grocery_market'
		when mcc in (6011) then 'atm'
		when mcc in (5812,5811) then 'restaurants'
		when mcc in (5814) then 'fast_food'
		when mcc in (4121) then 'taxicabs'
		when mcc in (5541,5542) then 'gas_service_station'
		when mcc in (5735) then 'record_stores'
		when mcc in (4111,4112,4131) then 'local_transport_railway' --transportation-suburban and local commuter passenger, including ferries
		when mcc in (5499,5921,5441) then 'food_drink_stores' -- liquer/beer,convenience stores, markets, specialty stores, vending machines
		when mcc in (5999,5941,5993,5399,5947,5309,5945,5994,5940) then 'retail_store' -- bikes,game/toy/hobby,miscellaneous specialty retail, sporting good store, cigar
		when mcc in (5912) then 'drug_pharma'
		when mcc in (5813) then 'bars_clubs'
		when mcc in (5942) then 'bookstores'
		when mcc in (7011) then 'hotel_lodging'
		when mcc in (7995,7800,7801,7802) then 'gambling_gaming'
		when mcc in (5734,5732,5945,5045,5946) then 'computer_electronic_stores' --computer, electronic, photography
		when mcc in (7399,7392,7299,6513,4215,8641) then 'business_org_services'
		when mcc in (5331,5200,5712,5251,5072,5074,5085,5193,5211,5261,5231,5533,5719,5992,5995,763,5722) then 'household_store' -- repair shops, pets, florists,variety store, wide range or household goods
		when mcc in (5651,5691,5611,5621,5631,5641,5655,5661,5681,5697,5698,5699,5311) then 'clothing_depart_store'
		when mcc in (4784,7523) then 'car_toll_parking' -- bridge and road fees, toll
		when mcc in (5462) then 'bakeries'
		when mcc in (5968,5964,5969,5967,5965) then 'subscriptions' --Direct Marketing–Continuity/Subscription Merchants
		when mcc in (5815,5816,5817,5818) then 'digital_goods' --Digital Goods: Books, Movies, Music
		when mcc in (4899, 4900, 4812,4813,4814,4816) then 'utilities' -- cable/satelite/television/electric/heating/water
		when mcc in (4789) then 'transport_serv' --Transportation Services Not Elsewhere Classified
		when mcc in (7512) then 'car_rental'
		when mcc in (8999) then 'prof_serv' -- professional services and membership organizations
		when mcc in (7372) then 'computer_data_serv' -- Computer Programming, Data Processing and Integrated System Design Services
		when mcc in (4722) then 'travel_tour_agencies' 
		when mcc in (5310) then 'discount_stores'
		when mcc in (5977,7230) then 'beauty_stores' --cosmetic, barber, beauty shops
		when mcc in (7311) then 'advertising_serv'
		when mcc in (7832,7922,7829,7833,7841,7922,7929,7932,7933,7941,7991,7992,7993,7994,7996,7997,7998,7999) 
			then 'entertainment' --games/arcades,tourist, sports field,music, dance,movies,theaters,pool,bowling,golf,amusement
		when mcc in (9211,9222,9223,9311,9399,9402,9405) then 'fines_taxes_gov' --government services
		when mcc in (4829,6051,6050,6012,6540) then 'money_cash_financial' --quasi cash merchant,wire transfer money order, money transfer
		when mcc in (7273) then 'dating_serv' 
		when mcc between 8011 and 8099 then 'health_serv' --doctors,hospitals, dental, chiropractor,nurse
		when mcc between 8211 and 8299 then 'education' -- schools, colleges/uni
			else 'no_cat' end as mcc_cat,
	tx.region_group,
	case when tx.card_tx_type = 'cardpresent' and region_group = 'intra' then region_group end as intra,
	case when tx.card_tx_type = 'cardpresent' and region_group = 'inter' then region_group end as inter,
	case when tx.card_tx_type = 'cardpresent' and region_group != 'dom' then 'foreign' else null end as foreign,
	case when tx.card_tx_type = 'cardpresent' and region_group = 'dom' then 'domestic' else null end as domestic,
	k.country_shipping,
	tx.merchant_country
from kycc k 
join dbt.zrh_card_transactions tx 
	on k.user_created = tx.user_created 
left join ecb_exchange_rates_daily as e
	on e.date = tx.created::date
	and e.currency = tx.currencycode
where type = 'PT' and tx.created between k.kycc and k.kycc + interval '420 days'
order by 1,2

drop table if exists pt_dom_foreign;
create temp table pt_dom_foreign as 
select 
	user_created,
	count(case when card_tx_type = 'cardpresent' and region_group = 'dom' then amount_euro end) as n_pt_dom,
	count(case when card_tx_type = 'cardpresent' and region_group = 'intra' then amount_euro end) as n_pt_intra,
	count(case when card_tx_type = 'cardpresent' and region_group = 'inter' then amount_euro end) as n_pt_inter,
	count(case when card_tx_type = 'ecomm' then amount_euro end) as n_pt_ecomm,
	count(case when card_tx_type = 'atm' and region_group = 'dom' then amount_euro end) as n_pt_dom_atm,
	count(case when card_tx_type = 'atm' and region_group = 'intra' then amount_euro end) as n_pt_intra_atm,
	count(case when card_tx_type = 'atm' and region_group = 'inter' then amount_euro end) as n_pt_inter_atm,
	sum(case when card_tx_type = 'cardpresent' and region_group = 'dom' then amount_euro end) as pt_dom_sum,
	sum(case when card_tx_type = 'cardpresent' and region_group = 'intra' then amount_euro end) as pt_intra_sum,
	sum(case when card_tx_type = 'cardpresent' and region_group = 'inter' then amount_euro end) as pt_inter_sum,
	sum(case when card_tx_type = 'ecomm' then amount_euro end) as pt_ecomm_sum,
	sum(case when card_tx_type = 'atm' and region_group = 'dom' then amount_euro end) as pt_dom_atm_sum,
	sum(case when card_tx_type = 'atm' and region_group = 'intra' then amount_euro end) as pt_intra_atm_sum,
	sum(case when card_tx_type = 'atm' and region_group = 'inter' then amount_euro end) as pt_inter_atm_sum
from dev.pt 
group by 1

-- DT, DD, FT, Cash26, CT
drop table if exists txns;
create temp table txns as 
select 
	k.user_created,
	sum(n_dt) as n_dt,
	sum(n_dd) as n_dd,
	sum(n_ft) as n_ft,
	sum(n_cash26) as n_cash26,
	sum(n_ct) as n_ct,
	sum(n_ext_total_out) as n_ext_out,
	sum(n_ext_total_in) as n_ext_in, 
	sum(amount_cents_dt)::float/100 as dt_sum,
	sum(amount_cents_dd)::float/100 as dd_sum,
	sum(amount_cents_ft)::float/100 as ft_sum,
	sum(amount_cents_cash26)::float/100 as cash26_sum,
	sum(amount_cents_ct)::float/100 as ct_sum,
	sum(amount_cents_ext_total_out)::float/100 as ext_out_sum,
	sum(amount_cents_ext_total_in)::float/100 as ext_in_sum 
from kycc k 
join dbt.zrh_txn_day t 
	on k.user_created = t.user_created
where t.txn_date between k.kycc and k.kycc + interval '420 days'
group by 1


-- friend referral 
drop table if exists friend;
create temp table friend as 
select 
	k.user_created,
	count(txn_ts) as n_wu, 
	round(sum(amount_cents)::float/100,2) as wu_sum
from kycc k 
join dbt.zrh_transactions t 
	on k.user_created = t.user_created 
where type = 'WU' and amount_cents > 0
	and txn_ts between k.kycc and k.kycc + interval '420 days'
group by 1



--TXN CATEGORIES
drop table if exists mcc_cat;
create temp table mcc_cat as 
select 
	user_created,
	count(case when mcc_cat = 'grocery_market' then created end) as grocery_market,
	count(case when mcc_cat = 'restaurants' then created end) as restaurant,
	count(case when mcc_cat = 'atm' then created end) as atm,
	count(case when mcc_cat = 'fast_food' then created end) as fast_food,
	count(case when mcc_cat = 'local_transport_railway' then created end) as local_transport,
	count(case when mcc_cat = 'clothing_depart_store' then created end) as clothing,
	count(case when mcc_cat = 'retail_store' then created end) as retail,
	count(case when mcc_cat = 'household_store' then created end) as household,
	count(case when mcc_cat = 'gas_service_station' then created end) as gas_service,
	count(case when mcc_cat = 'food_drink_stores' then created end) as food_drinks,
	count(case when mcc_cat = 'taxicabs' then created end) as taxicabs,
	count(case when mcc_cat = 'drug_pharma' then created end) as drug_pharma,
	count(case when mcc_cat = 'bars_clubs' then created end) as bars_clubs,
	count(case when mcc_cat = 'car_toll_parking' then created end) as car_toll_parking,
	count(case when mcc_cat = 'entertainment' then created end) as entertainment,
	count(case when mcc_cat = 'utilities' then created end) as utilities,
	count(case when mcc_cat = 'subscriptions' then created end) as subscriptions,
	count(case when mcc_cat = 'bookstores' then created end) as bookstores,
	count(case when mcc_cat = 'business_org_services' then created end) as business_org_serv,
	count(case when mcc_cat = 'hotel_lodging' then created end) as hotel_lodge,
	count(case when mcc_cat = 'computer_electronic_stores' then created end) as computer_electronic,
	count(case when mcc_cat = 'bakeries' then created end) as bakeries,
	count(case when mcc_cat = 'gambling_gaming' then created end) as gambling_gaming,
	count(case when mcc_cat = 'record_stores' then created end) as record_stores,
	count(case when mcc_cat = 'digital_goods' then created end) as digital_goods,
	count(case when mcc_cat = 'airline' then created end) as airline,
	count(case when mcc_cat = 'beauty_stores' then created end) as beauty_stores,
	count(case when mcc_cat = 'transport_serv' then created end) as transport_serv,
	count(case when mcc_cat = 'fines_taxes_gov' then created end) as fines_tax_gov,
	count(case when mcc_cat = 'money_cash_financial' then created end) as money_financial,
	count(case when mcc_cat = 'prof_serv' then created end) as professional_serv,
	count(case when mcc_cat = 'discount_stores' then created end) as discount_stores,
	count(case when mcc_cat = 'travel_tour_agencies' then created end) as travel_agencies,
	count(case when mcc_cat = 'computer_data_serv' then created end) as computer_data_serv,
	count(case when mcc_cat = 'car_rental' then created end) as car_rental,
	count(case when mcc_cat = 'health_serv' then created end) as health_serv,
	count(case when mcc_cat = 'advertising_serv' then created end) as advertising_serv,
	count(case when mcc_cat = 'education' then created end) as education,
	count(case when mcc_cat = 'dating_serv' then created end) as dating_serv,
	count(case when mcc_cat = 'no_cat' then created end) as no_cat
from dev.pt
group by 1
order by 2 desc



-- TXN CAT VALUE
drop table if exists mcc_cat_value;
create temp table mcc_cat_value as 
select 
	user_created,
	sum(case when mcc_cat = 'grocery_market' then amount_euro end) as grocery_market_sum,
	sum(case when mcc_cat = 'restaurants' then amount_euro end) as restaurant_sum,
	sum(case when mcc_cat = 'atm' then amount_euro end) as atm_sum,
	sum(case when mcc_cat = 'fast_food' then amount_euro end) as fast_food_sum,
	sum(case when mcc_cat = 'local_transport_railway' then amount_euro end) as local_transport_sum,
	sum(case when mcc_cat = 'clothing_depart_store' then amount_euro end) as clothing_sum,
	sum(case when mcc_cat = 'retail_store' then amount_euro end) as retail_sum,
	sum(case when mcc_cat = 'household_store' then amount_euro end) as household_sum,
	sum(case when mcc_cat = 'gas_service_station' then amount_euro end) as gas_service_sum,
	sum(case when mcc_cat = 'food_drink_stores' then amount_euro end) as food_drinks_sum,
	sum(case when mcc_cat = 'taxicabs' then amount_euro end) as taxicabs_sum,
	sum(case when mcc_cat = 'drug_pharma' then amount_euro end) as drug_pharma_sum,
	sum(case when mcc_cat = 'bars_clubs' then amount_euro end) as bars_clubs_sum,
	sum(case when mcc_cat = 'car_toll_parking' then amount_euro end) as car_toll_parking_sum,
	sum(case when mcc_cat = 'entertainment' then amount_euro end) as entertainment_sum,
	sum(case when mcc_cat = 'utilities' then amount_euro end) as utilities_sum,
	sum(case when mcc_cat = 'subscriptions' then amount_euro end) as subscriptions_sum,
	sum(case when mcc_cat = 'bookstores' then amount_euro end) as bookstores_sum,
	sum(case when mcc_cat = 'business_org_services' then amount_euro end) as business_org_serv_sum,
	sum(case when mcc_cat = 'hotel_lodging' then amount_euro end) as hotel_lodge_sum,
	sum(case when mcc_cat = 'computer_electronic_stores' then amount_euro end) as computer_electronic_sum,
	sum(case when mcc_cat = 'bakeries' then amount_euro end) as bakeries_sum,
	sum(case when mcc_cat = 'gambling_gaming' then amount_euro end) as gambling_gaming_sum,
	sum(case when mcc_cat = 'record_stores' then amount_euro end) as record_stores_sum,
	sum(case when mcc_cat = 'digital_goods' then amount_euro end) as digital_goods_sum,
	sum(case when mcc_cat = 'airline' then amount_euro end) as airline_sum,
	sum(case when mcc_cat = 'beauty_stores' then amount_euro end) as beauty_stores_sum,
	sum(case when mcc_cat = 'transport_serv' then amount_euro end) as transport_serv_sum,
	sum(case when mcc_cat = 'fines_taxes_gov' then amount_euro end) as fines_tax_gov_sum,
	sum(case when mcc_cat = 'money_cash_financial' then amount_euro end) as money_financial_sum,
	sum(case when mcc_cat = 'prof_serv' then amount_euro end) as professional_serv_sum,
	sum(case when mcc_cat = 'discount_stores' then amount_euro end) as discount_stores_sum,
	sum(case when mcc_cat = 'travel_tour_agencies' then amount_euro end) as travel_agencies_sum,
	sum(case when mcc_cat = 'computer_data_serv' then amount_euro end) as computer_data_serv_sum,
	sum(case when mcc_cat = 'car_rental' then amount_euro end) as car_rental_sum,
	sum(case when mcc_cat = 'health_serv' then amount_euro end) as health_serv_sum,
	sum(case when mcc_cat = 'advertising_serv' then amount_euro end) as advertising_serv_sum,
	sum(case when mcc_cat = 'education' then amount_euro end) as education_sum,
	sum(case when mcc_cat = 'dating_serv' then amount_euro end) as dating_serv_sum,
	sum(case when mcc_cat = 'no_cat' then amount_euro end) as no_cat_sum
from dev.pt
group by 1
order by 2 desc


-- PRIMARY AND SPACES BALANCES
drop table if exists balance;
create temp table balance as 
select 
	k.user_created,
	avg(case when account_type = 'PRIMARY' then balance_cents_euro end)::float/100 as avg_primary_bal,
	avg(case when account_type = 'SPACES' then balance_cents_euro end)::float/100 as avg_spaces_bal,
	count(distinct case when account_type = 'SPACES' then b.id end) as n_spaces
from kycc k
join dbt.zrh_monthly_balance_aud b
	on k.user_created = b.user_created 
where month between k.kycc and k.kycc + interval '420 days'
group by 1


-- SPACES TRANSACTIONS
create temp table spaces_txn as
select
	k.user_created,
	sum(t.n_spaces_ct) as n_space_ct,
	avg(t.amount_cents_spaces_ct) as avg_space_ct,
	sum(t.n_spaces_dt) as n_space_dt,
	avg(t.amount_cents_spaces_dt) as avg_space_dt
from kycc k 
join dbt.zrh_txn_day t
	on k.user_created = t.user_created
where t.txn_date between k.kycc and k.kycc + interval '420 days'
group by 1



-- PNL 
drop table if exists pnl;
create temp table pnl as 
select 
	a.user_created,
	sum(value) as net_pnl
from kycc a
left join dbt.ucm_pnl b 
	on a.user_created = b.user_created
where b.month between a.kycc and a.kycc + interval '420 days'
group by 1
order by 1,2 

limit 500;


-- TABLES TO COMBINE
drop table if exists dev.txn_pca_cohort_SeptOct2018;
create table dev.txn_pca_cohort_SeptOct2018 as 
select
	a.id, a.user_created, a.kycc, coalesce(a.membership,'standard') AS membership, coalesce(a.product_id,'STANDARD') as product_id, a.country_tnc_legal as market,a.nationality,a.nat_status, a.referral, 
	a.closed_at::date, a.kycc_closed_days, a.gender, a.age_group, a.age,
	j.weeks_wau_txn, j.mau_txn, b.months_sau, c.mau_act, 
	d.n_pt_dom, d.n_pt_intra, d.n_pt_inter, d.n_pt_ecomm, d.n_pt_dom_atm, d.n_pt_intra_atm, d.n_pt_inter_atm,
	e.n_dt, e.n_dd, e.n_ft, e.n_cash26, e.n_ct, k.n_wu, g.n_space_ct, g.n_space_dt, 
	d.pt_dom_sum, d.pt_intra_sum, d.pt_inter_sum, d.pt_ecomm_sum, d.pt_dom_atm_sum, d.pt_intra_atm_sum, d.pt_inter_atm_sum,
	e.dt_sum, e.dd_sum, e.ft_sum, e.cash26_sum, e.ct_sum, k.wu_sum, 
	f.avg_primary_bal, f.avg_spaces_bal, f.n_spaces,
	g.avg_space_ct, g.avg_space_dt, e.n_ext_out, e.n_ext_in,e.ext_out_sum, e.ext_in_sum,
	grocery_market, restaurant, atm,fast_food,local_transport,clothing,retail,
	household,gas_service,food_drinks,taxicabs,drug_pharma,bars_clubs,car_toll_parking,
	entertainment,utilities,subscriptions,bookstores,business_org_serv,hotel_lodge,
	computer_electronic,bakeries,gambling_gaming,record_stores,digital_goods,airline,
	beauty_stores,transport_serv,fines_tax_gov,money_financial,professional_serv,discount_stores,
	travel_agencies,computer_data_serv,car_rental,health_serv,advertising_serv,education,dating_serv,
	no_cat,
	grocery_market_sum, restaurant_sum, atm_sum,fast_food_sum,local_transport_sum,clothing_sum,retail_sum,
	household_sum,gas_service_sum,food_drinks_sum,taxicabs_sum,drug_pharma_sum,bars_clubs_sum,car_toll_parking_sum,
	entertainment_sum,utilities_sum,subscriptions_sum,bookstores_sum,business_org_serv_sum,hotel_lodge_sum,
	computer_electronic_sum,bakeries_sum,gambling_gaming_sum,record_stores_sum,digital_goods_sum,airline_sum,
	beauty_stores_sum,transport_serv_sum,fines_tax_gov_sum,money_financial_sum,professional_serv_sum,discount_stores_sum,
	travel_agencies_sum,computer_data_serv_sum,car_rental_sum,health_serv_sum,advertising_serv_sum,education_sum,dating_serv_sum,
	no_cat_sum,
	a.cohort, a.continent, a.cntry_nationality, 
	l.net_pnl 
from kycc a 
left join wau_sau b on a.user_created = b.user_created
left join mau_act c on a.user_created = c.user_created
left join pt_dom_foreign d on a.user_created = d.user_created
left join txns e on a.user_created = e.user_created 
left join balance f on a.user_created = f.user_created 
left join spaces_txn g on a.user_created = g.user_created
left join mcc_cat h on a.user_created = h.user_created 
left join mcc_cat_value i on a.user_created = i.user_created
left join wau j on a.user_created = j.user_created
left join friend k on a.user_created = k.user_created
left join pnl l on a.user_created = l.user_created 
;

select *
from dev.txn_pca_cohort_SeptOct2018
;

", "redshift-eu")

save(clustering_query,
     file = file.path(kDataPath,"clustering_data.RData"))
