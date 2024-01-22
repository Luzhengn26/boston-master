library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/src/research/product/us/20210720_nationality_demonyms_us_test","data")

test_data <- queryDB(" 
with base as (
  select
    user_id,
    created,
    se_property as variant,
    attribution,
    os_family
  from dbt.f_upper_funnel 
  where country = 'US'
    and se_property ilike '%nationality_demonyms%'
    and step = 'signup-start'
)
, mau as (
  select 
    user_id,
    min(activity_start) as ft_mau
  from dbt.f_user_activity_txn
  group by 1
)
select
  b.*,
  case when datediff(days, b.created::datetime, u.kyc_first_initiated::datetime) <= 7 then u.kyc_first_initiated else null end as kyci,
  case when datediff(days, b.created::datetime, u.kyc_first_completed::datetime) <= 7 then u.kyc_first_completed else null end as kycc,
  case when datediff(days, kycc::datetime, m.ft_mau::datetime) <= 21 then m.ft_mau else null end as ftmau,
  u.first_fraud_flag
from base b
left join dbt.dim_users u on u.user_id = b.user_id
left join mau m on m.user_id = b.user_id
where b.created <= current_date - interval '21 days'
",'redshift-us')

test_data <- as.data.table(test_data)

# If re-running this file, create a folder named "Data" in the file path
save(test_data,
     file = file.path(kDataPath,"nationality_data.RData"))
