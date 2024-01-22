# baby names
library(stringi)
library(n26)
library(data.table)
library(ggplot2)
library(dplyr)

# got names from here: https://www.babynamewizard.com/the-top-1000-baby-names-of-2016-united-states-of-america
# this bit of analysis has A BUNCH of caveats and did not make it into the final analysis
names <- read.csv('~/src/boston/research/product/deep_dive/who_are_our_users_20200430_US/data/popular_names.csv', stringsAsFactors = FALSE)
names$Name <- tolower(names$Name)

all_mau <- queryDB(" 
select distinct
  u.id as user_id,
  u.user_created,
  lower(u.first_name) as first_name,
  lower(u.last_name) as last_name,
  o.occupation,
  a.zip_code,
  a.city,
  a.state,
  u.birth_date
from dbt.zrh_user_activity_txn zuat 
join etl_reporting.cmd_users u on zuat.user_created = u.user_created
join etl_reporting.cmd_address a on a.user_id = u.id 
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where zuat.activity_type = '1_tx_35'
  and a.type = 'FIRST_SHIPPING'
",'postgres-us')

signups <- queryDB(" 
select distinct
  u.id as user_id,
  u.user_created,
  lower(u.first_name) as first_name,
  o.occupation,
  a.zip_code,
  a.city,
  a.state,
  u.birth_date
from public.cmd_users u
join etl_reporting.cmd_address a on a.user_id = u.id 
join etl_reporting.cmd_user_account_usage o on o.user_id::uuid = a.user_id
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = a.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where a.type = 'FIRST_SHIPPING'
",'postgres-us')

all_mau$first_name <- stri_trans_general(all_mau$first_name,"Latin-ASCII")
signups$first_name <- stri_trans_general(signups$first_name,"Latin-ASCII")

mau_names <- merge(all_mau, names[order(-names$Births),], by.x="first_name", by.y = "Name", all.x=TRUE)
signup_names <- merge(signups, names[order(-names$Births),], by.x="first_name", by.y = "Name", all.x=TRUE)

mau_names <- as.data.table(mau_names)
signup_names <- as.data.table(signup_names)

# settle duplicates
duplicates <- distinct(mau_names[,c("user_id","Gender","Births")])
# duplicates[,.(cnt=.N, Births=min(Births)),by="user_id"][cnt>1,]
mau_names <- merge(mau_names, duplicates[,.(cnt=.N, Births=min(Births)),by="user_id"][cnt>1,], by=c("user_id","Births"), all.x=TRUE)
mau_names <- mau_names[is.na(cnt),]

duplicates <- distinct(signup_names[,c("user_id","Gender","Births")])
# duplicates[,.(cnt=.N, Births=min(Births)),by="user_id"][cnt>1,]
signup_names <- merge(signup_names, duplicates[,.(cnt=.N, Births=min(Births)),by="user_id"][cnt>1,], by=c("user_id","Births"), all.x=TRUE)
signup_names <- signup_names[is.na(cnt),]

###### MAU

# how many users did we match?
length(unique(mau_names[!is.na(Gender),]$user_id))
length(unique(mau_names[!is.na(Gender),]$user_id))/length(unique(mau_names$user_id))
# how many names from the list matched?
length(unique(mau_names[!is.na(Gender),]$first_name))
length(unique(mau_names[!is.na(Gender),]$first_name))/length(unique(names$Name))
# how many matched by gender split?
uni <- distinct(mau_names[!is.na(Gender),][,c("first_name", "Gender")])
uni[,.(users=.N), by=.(Gender)][,.(Gender=Gender, users=users, match_pct=users/1000, internal_pct=users/(711+791))]


# what's the gender split for MAU?
gender_totals <- mau_names[!is.na(Gender),][,.(users=.N), by=.(Gender)]
gender_totals$total_users <- mau_names[!is.na(Gender),][,.(users=.N)]
gender_totals$pct <- gender_totals$users/gender_totals$total_users
gender_totals



###### Signups

# how many users did we match?
length(unique(signup_names[!is.na(Gender),]$user_id))
length(unique(signup_names[!is.na(Gender),]$user_id))/length(unique(signup_names$user_id))
# how many names from the list matched?
length(unique(signup_names[!is.na(Gender),]$first_name))
length(unique(signup_names[!is.na(Gender),]$first_name))/length(unique(names$Name))
# how many matched by gender split?
uni <- distinct(signup_names[!is.na(Gender),][,c("first_name", "Gender")])
uni[,.(users=.N), by=.(Gender)][,.(Gender=Gender, users=users, match_pct=users/1000, internal_pct=users/(896+913))]


# what's the gender split for Signups?
gender_totals <- signup_names[!is.na(Gender),][,.(users=.N), by=.(Gender)]
gender_totals$total_users <- signup_names[!is.na(Gender),][,.(users=.N)]
gender_totals$pct <- gender_totals$users/gender_totals$total_users
gender_totals



# output MAU names as csv
write.csv(mau_names[,c("user_id", "first_name", "last_name", "Gender")], '~/mau_gender_match.csv')
