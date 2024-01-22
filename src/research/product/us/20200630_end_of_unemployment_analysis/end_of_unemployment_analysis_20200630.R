####
# Authors: Dani Mermelstein and Carlo Scalisi
# Description: How will the end of unemployment insurance affect our MAU rate?
# Date: 2020-06-30
####

library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/product/deep_dive/end_of_unemployment_analysis_20200630","data")


# there's three high-level ways to approach the problem:
  # 1) look at all legit users with UI deposits, use their monthly cashflows to determine how fast they would run through savings and when they would no longer be counted as MAU
  # 2) look at users with DD who started getting UI deposits (or whose DD stopped in the last month). Take that as the unemployment rate for users with DD, take the average monthly outflows and average monthly balance for "unemployed" users, and project how long it would take for those of users to draw down their savings.
  # 3) look at users with DD who started getting UI deposits (or whose DD stopped in the last month). Assume that's the unemployment rate for all our users, take overall average monthly outflows and average savings, and project how long it would take to draw down their savings.
# The first method looks at users we know get UI benefits and cuts off their inflows
# The second method tries to estimate the unemployment rate (implicitly assumes users might get their UI benefits at another bank)
# The third method tries to estimate the unemployment rate and applies it to the whole N26 population

# excluding a bunch of sources to filter down to non-federal entities
source_list <- c("--'IRS  TREAS 310 ',
--'SSA  TREAS 310 ',
--'SSI  TREAS 310 ',
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
--'VACP TREAS 310 ',
'ST OF IA-UI PAY ',
'DFAS-CLEVELAND ',
'RIDLT-UI ',
'WISCONSIN-DWD-UI ',
'DFAS-IN  IND, IN ',
--'AGRI TREAS 310 ',
'TEXAS OAG ',
'MAINE DEPT LABOR ',
'WORKFORCE WV ',
'FRANCHISE TAX BD ',
--'SBAD TREAS 310 ',
'NY STATE ',
'IHSS2 ST OF CA ',
'EMPLOYMT BENEFIT ',
'JOB SERVICE ND ',
--'VAED TREAS 310 ',
'UI BEN EFT ',
'MDES ',
--'TCS  TREAS 449 ',
'WYOMING DWS ',
'ECSI AS AGENT ',
'GA CHILD SUPPORT ',
--'CEN  TREAS 310 ',
'NHUS ',
'STATEOFMICHIGAN ',
--'TAX PRODUCTS PE3 ',
--'TAX PRODUCTS PE1 ',
'ID DEPT OF LABOR ',
'DEPT OF REVENUE ',
'STATE OF MONTANA ',
'STATE OF OHIO ',
'FL DEPT OF REVE ',
--'TAX PRODUCTS PE4 ',
--'DOT4 TREAS 310 ',
'STATE OF ILL ',
--'TAX PRODUCTS PE5 ',
'GEORGIA DEPARTME ',
'AK DEPT OF LABOR ',
'OH CHILD SUPPOR ',
'CO FAM SUPP REG ',
'LA REVENUE DEPT. ',
'SC STATE TREASUR ',
--'TAX PRODUCTS PE2 ',
'MO DEPT REVENUE ',
'ALASKA DOL ',
'AZ DEPT OF REV ',
--'MCTF TREAS 310 ',
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
'WA ST EMPLOY SEC UI BENEFIT '")


current_date <- queryDB("
select current_date
",'postgres-us')

deterministic_ui <- queryDB(paste0("
with unemploy_bene as (
select distinct
  t.user_id,
--  cua.account_id,
  t.txn_id,
--  t.company,
--  t.description,
--  ca.status,
  t.bank_balance_impact_cents/100::float as amount,
  t.completed_tstamp
from dbt.zrh_transactions t 
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and t.completed_tstamp between '2020-03-01' and ",current_date,"
  and t.type = 'ACH'
  and t.direction = 'Incoming'
  and t.company in
(",source_list,")
--order by t.user_id, cua.account_id, t.completed_tstamp 
)
select 
  t.user_id,
  t.direction,
  t.type,
  t.completed_tstamp,
  date_trunc('month', t.completed_tstamp)::date as month,
  t.bank_balance_impact_cents/100::float as amount,
  case when ub.txn_id is not null then 1 else 0 end as ui_deposit
from dbt.zrh_transactions t 
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
left join unemploy_bene ub on ub.txn_id = t.txn_id 
where t.completed_tstamp between '2020-03-01' and ",current_date,"
  and not t.is_internal_txn 
  and not t.is_micro_deposit 
"), 'postgres-us')



probabilistic_dd <- queryDB(paste0("
with dd_users as (
  select
    t.user_id,
    max(case when t.type = 'DIR_DEP' then 1 else 0 end) as dd_user,
    max(case when t.completed_tstamp >= '2020-06-01' then 1 else 0 end) as has_june_dd,
    max(case when t.completed_tstamp >= current_date - interval '16 days' then 1 else 0 end) as recent_dd,
    max(case when t.company in
      (",source_list,") then 1 else 0 end) as has_ui_deposit
  from dbt.zrh_transactions t 
  -- exclude fraudsters
  join public.cr_user_account cua on cua.user_id = t.user_id
  join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
  where t.type in ('DIR_DEP','ACH')
    and t.completed_tstamp between '2020-03-01' and ",current_date,"
  group by 1
)
select 
*
from dd_users
where dd_user = 1
"), 'postgres-us')


user_balances <- queryDB("
with balances as (
select
    cua.user_id,
    sum(au.amount_cents)/100::float as balance
from au_transactions au
join cr_user_account cua on cua.account_id = au.account_id 
-- exclude fraudsters
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
group by 1
)
select
    *
from balances
order by 1 desc 
", 'postgres-us')

user_spending <- queryDB(paste0("
select 
  t.user_id,
  t.direction,
  date_trunc('month', t.completed_tstamp)::date as month,
  sum(t.bank_balance_impact_cents/100::float) as amount
from dbt.zrh_transactions t
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.completed_tstamp between '2020-03-01' and ",current_date,"
  and not t.is_internal_txn 
  and not t.is_micro_deposit 
group by 1,2,3
"), 'postgres-us')


deterministic_ui <- as.data.table(deterministic_ui)
probabilistic_dd <- as.data.table(probabilistic_dd)
user_balances <- as.data.table(user_balances)
user_spending <- as.data.table(user_spending)

save(deterministic_ui,
     probabilistic_dd,
     user_balances,
     user_spending,
     current_date,
     file = file.path(kDataPath,"ui_data.RData"))
