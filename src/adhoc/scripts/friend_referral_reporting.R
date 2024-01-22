#####
# Author: Dani Mermelstein
# Date: 20200825
# Description: Friend referral code reporting for marketing
#
#####

library(n26)

mau_codes <- queryDB("
select distinct
  u.id as user_id,
  zuat.activity_end,
  r.code
from dbt.zrh_user_activity_txn zuat 
join public.cmd_users u using (user_created)
join etl_reporting.u_referral_code r on r.user_id = u.id
where zuat.activity_type ='1_tx_35'
	and current_date between zuat.activity_start and zuat.activity_end
",'postgres-us')

write.csv(mau_codes, '~/current_mau_referral_codes.csv')

lapsed_user_codes <- queryDB("
with activity as (
select
  zuat.user_created,
  max(zuat.activity_end) as activity_end
from dbt.zrh_user_activity_txn zuat
where zuat.activity_type ='1_tx_35'
group by 1
)
select distinct 
  u.id as user_id,
  a.activity_end,
  r.code
from activity a
join public.cmd_users u on a.user_created = u.user_created
join etl_reporting.u_referral_code r on r.user_id = u.id
where a.activity_end between current_date - interval '120 days' and current_date - interval '35 days'
",'postgres-us')

write.csv(lapsed_user_codes, '~/lapsed_user_referral_codes.csv')

unfunded_kycc_codes <- queryDB("
select distinct
  zu.user_id,
  zu.kyc_first_completed,
  r.code
from dbt.zrh_users zu
join etl_reporting.u_referral_code r on r.user_id = zu.user_id
where zu.first_time_mau_tstamp is null
  and zu.kyc_first_completed >= current_date - interval '60 day'
",'postgres-us')

write.csv(unfunded_kycc_codes, '~/unfunded_kycc_referral_codes.csv')
