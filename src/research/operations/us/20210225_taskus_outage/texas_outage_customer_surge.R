library(n26)
library(data.table)

kDataPath <- file.path("~/src/boston/research/operations/us/taskus_outage_20210225","data")
start.date <- "2021-02-06"
incident.start.date <- "2021-02-13"
end.date <- "2021-02-19"

contacts <- queryDB(paste0("
  select
    user_id,
    initiated_date::date as contact_date,
    channel,
    abandoned,
    1 as contact
  from dbt.sf_all_contacts
  where initiated_date::date >= '",start.date,"'
    and initiated_date::date <= '",end.date,"'
    and channel in ('call','chat')
"), "postgres-us")

contacts <- as.data.table(contacts)

# flag users with a user_id
contacts$customer <- contacts$user_id
# we assume every contact without a user_id is a unique customer
new_ids <- sample(1:100000000, length(contacts[is.na(contacts$user_id),]$user_id), replace=FALSE)
contacts[is.na(contacts$user_id),]$user_id <- new_ids

contacts$period <- 'before'
contacts[contacts$contact_date >= incident.start.date,]$period <- 'during'

# number attempts per user per channel per day
contacts[, attempt_num := seq_len(.N), by = .(channel, user_id, contact_date)]
contact_attempts <- contacts[, .(contacts = max(attempt_num)), by = .(channel, user_id, contact_date, period)]


customer_contacts <- queryDB(paste0("
  select
    user_id,
    initiated_date::date as contact_date,
    channel,
    abandoned,
    1 as contact
  from dbt.sf_all_contacts
  where initiated_date::date >= '",start.date,"'
    and initiated_date::date <= '",end.date,"'
    and channel in ('call','chat')
    and user_id is not null
"), "postgres-us")

customer_contacts <- as.data.table(customer_contacts)

customer_contacts$period <- 'before'
customer_contacts[customer_contacts$contact_date >= incident.start.date,]$period <- 'during'

# number attempts per user per channel per day
customer_contacts[, attempt_num := seq_len(.N), by = .(channel, user_id, contact_date)]
customer_contact_attempts <- customer_contacts[, .(contacts = max(attempt_num)), by = .(channel, user_id, contact_date, period)]


save(contact_attempts,
     customer_contact_attempts,
     start.date,
     incident.start.date,
     end.date,
     contacts,
     customer_contacts,
     file = file.path(kDataPath,"contact_data.RData"))
