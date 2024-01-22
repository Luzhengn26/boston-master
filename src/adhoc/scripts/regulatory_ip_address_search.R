#####
# Author: Dani Mermelstein
# Date: 20200611
# Description: Legal will occasionally have a regulatory request for all IP addresses for a list of users. This script can be modified to output
#               this data jointly or split by user.
#
#####

library(n26)
library(data.table)

# import external list
list <- read.csv("/Users/danielmermelstein/ip_address_other_users.csv", stringsAsFactors = FALSE)

# or just write an adhoc list

list <- c("249d3084-09a6-4b9f-8774-47eb81702baf",
      "86e58253-a7d2-4c9d-9ef1-88e055703e2b",
      "5951f060-f444-441d-bd97-15444f360d00")

user_list <- paste(shQuote(list), collapse=", ")

ip_addresses <- queryDB(paste0("
select
  u.id as user_id,
  u.first_name,
  u.last_name,
  keb.derived_tstamp as time_accessed,
  keb.user_ipaddress,
  keb.device_manufacturer,
  keb.device_model 
from etl_reporting.cmd_users u
join public.cmd_shadow_user su on u.id = su.user_id 
join ksp_event_core kec  on kec.user_id = su.id
join etl_reporting.ksp_event_crab keb on keb.event_id = kec.event_id   
where kec.derived_tstamp::date >= '2019-01-01'
  and u.id in (",user_list,")
order by 1,2
"), 'postgres-us')

length(unique(ip_addresses$user_id))
length(unique(ip_addresses$user_ipaddress))

# append names together
ip_addresses$full_name <- paste(ip_addresses$first_name, ip_addresses$last_name)

ip_addresses <- ip_addresses[,c("full_name","user_id","time_accessed","user_ipaddress","device_manufacturer","device_model")]

# use this when we can just write one CSV output
write.csv(ip_addresses, "~/ip_addresses.csv")

# use this when we need to write individual CSVs for each user
# for (n in unique(ip_addresses$user_id)){
#   write.csv(ip_addresses[which(ip_addresses$user_id == n),], paste0("~/",unique(ip_addresses[which(ip_addresses$user_id == n),]$full_name),".csv", sep=""))
#   }
