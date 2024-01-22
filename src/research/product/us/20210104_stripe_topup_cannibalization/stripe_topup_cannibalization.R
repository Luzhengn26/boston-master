library(n26)
library(data.table)

kDataPath <- file.path("/Users/danielmermelstein/src/boston/research/product/deep_dive/stripe_topup_cannibalization_20210104","data")

start.date <- '2020-04-01'
end.date <- '2020-08-09'

transactions <- queryDB(paste0("
with fraud_labels as (
  select distinct 
    lel.entity_id as user_id
  from public.lin_entity_label lel 
  join public.lin_label ll on ll.id = lel.label_id
  where ll.name in ('AML - General',
    'AML - Terrorist Financing',
    'Fraud',
    'Fraud - ACH',
    'Fraud - Card - Chargeback',
    'Fraud - Card - Stripe Top Up',
    'Fraud - KYC - ID Theft',
    'Fraud - OCT - CashApp',
    'Fraud - Presentment Refund')
)
, users_with_benefits as (
select distinct 
  t.user_id
from dbt.zrh_transactions t 
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and t.completed_tstamp between '2020-04-01' and current_date
  and t.type = 'ACH'
  and t.direction = 'Incoming'
  and t.company in
('IRS  TREAS 310 ',
'SSA  TREAS 310 ',
'SSI  TREAS 310 ',
'TWC-BENEFITS ',
'GA DEPT OF LABOR ',
'NYS DOL UI DD ',
'FL DEO ',
'MA DUA ',
'STATE OF INDIANA ',
'MN DEPT OF DEED ',
'STATE OF ARIZONA ',
'TN UI PAYMENTS ',
'DEPT OF LABOR ',
'VEC - VIRGINIA ',
'UIA PRE-PAID CAR ',
'COMM OF PA  UCD ',
'ODJFS ',
'CTDOL UNEMP COMP ',
'IDES ',
'WA ST EMPLOY SEC ',
'CDLE UI BENEFITS ',
'SCESC-UIBENEFITS ',
'MODES ',
'ODJFS-PUA ',
'ADWS ',
'KS DEPT OF LABOR ',
'TN DUA PAYMENTS ',
'VACP TREAS 310 ',
'ST OF IA-UI PAY ',
'DFAS-CLEVELAND ',
'RIDLT-UI ',
'WISCONSIN-DWD-UI ',
'DFAS-IN  IND, IN ',
'AGRI TREAS 310 ',
'TEXAS OAG ',
'MAINE DEPT LABOR ',
'WORKFORCE WV ',
'FRANCHISE TAX BD ',
'SBAD TREAS 310 ',
'NY STATE ',
'IHSS2 ST OF CA ',
'EMPLOYMT BENEFIT ',
'JOB SERVICE ND ',
'VAED TREAS 310 ',
'UI BEN EFT ',
'MDES ',
'TCS  TREAS 449 ',
'WYOMING DWS ',
'ECSI AS AGENT ',
'GA CHILD SUPPORT ',
'CEN  TREAS 310 ',
'NHUS ',
'STATEOFMICHIGAN ',
'TAX PRODUCTS PE3 ',
'TAX PRODUCTS PE1 ',
'ID DEPT OF LABOR ',
'DEPT OF REVENUE ',
'STATE OF MONTANA ',
'STATE OF OHIO ',
'FL DEPT OF REVE ',
'TAX PRODUCTS PE4 ',
'DOT4 TREAS 310 ',
'STATE OF ILL ',
'TAX PRODUCTS PE5 ',
'GEORGIA DEPARTME ',
'AK DEPT OF LABOR ',
'OH CHILD SUPPOR ',
'CO FAM SUPP REG ',
'LA REVENUE DEPT. ',
'SC STATE TREASUR ',
'TAX PRODUCTS PE2 ',
'MO DEPT REVENUE ',
'ALASKA DOL ',
'AZ DEPT OF REV ',
'MCTF TREAS 310 ',
'COMM OF PA ',
'COMM OF PA  SSP ',
'CONDUENT ',
'KABBAGE ',
'MA PUA ',
'NCDES-UIBENEFITS UI BENEFIT',
'NEVADA ESD',
'NEW MEXICO DWS ',
'RIDL-TDI ',
'TWC-BENEFITS  UI BENEFITU ',
'WA ST EMPLOY SEC UI BENEFIT') 
)
, transactions as (
select 
  t.type,
  date_trunc('week', t.completed_tstamp)::date as txn_week,
  sum(t.bank_balance_impact_cents)/100::float as txn_amount,
  count(distinct t.user_id) as users
from dbt.zrh_transactions t
left join fraud_labels fl on fl.user_id = t.user_id
left join users_with_benefits ub on ub.user_id = t.user_id
where t.completed_tstamp::date between '",start.date,"'::date and '",end.date,"'::date
  and not t.is_internal_txn
  and not t.is_micro_deposit
  and t.direction = 'Incoming'
  -- exclude frausters
  and fl.user_id is null
  -- exclude users with stimulus
  and ub.user_id is null
group by 1,2
)
select 
  *
from transactions
"), "postgres-us")


# set as datatable so we can easily aggregate
transactions <- as.data.table(transactions)
transactions <- transactions[!is.na(type),]

save(transactions,
     start.date,
     end.date,
     file = file.path(kDataPath,"stripe_data.RData"))
