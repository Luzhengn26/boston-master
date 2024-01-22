#####
# Author: Dani Mermelstein
# Date: 20200825
# Description:  It was discovered that a substantial number of users didn't have referral codes. Slack convo here: https://n26.slack.com/archives/G010K2MDARE/p1594044446000900
#               This was the script used to produce unique friend referral codes for users missing that data
#
#####

library(n26)
library(data.table)
library(tidyverse)
library(stringr)

user_list <- queryDB("
SELECT DISTINCT
    u.id as user_id,
    lower(u.first_name) as first_name,
    lower(u.last_name) as last_name,
    r.code
FROM etl_reporting.cmd_users u
JOIN etl_reporting.cmd_kyc_process k
 ON k.user_id = u.id AND k.completed IS NOT NULL
LEFT JOIN etl_reporting.u_referral_code r ON r.user_id = u.id
", 'postgres-us')

user_list <- as.data.table(user_list)
user_list <- user_list[order(user_id, -code),] %>% distinct(user_id, .keep_all=TRUE)

# uncomment this to check how many users are missing a code
# sum(is.na(user_list$code))
# length(unique(user_list$user_id))

missing_user_list <- user_list[is.na(user_list$code),]
code_list <- as.list(unique(user_list$code))

for (row in 1:nrow(missing_user_list)) {
  
  # the friend referral code algorithm is up to 7 letters of the first name, 1 letter from the last name, and a random 4-digit number
  firstn <- substr(missing_user_list$first_name[row], start = 1, stop = 7)
  lastn <- substr(missing_user_list$last_name[row], start = 1, stop = 1)
  # we need to ensure uniqueness, so several numbers are generated from which to choose for each user
  nums <- str_pad(round(runif(6, min=1, max=10000)), 4, pad="0")
  
  gen_code <- paste0(firstn, lastn, nums[1])
  
  if(gen_code %in% code_list) {
    # try a different number
    gen_code <- paste0(firstn, lastn, nums[2])
    if(gen_code %in% code_list) {
      gen_code <- paste0(firstn, lastn, nums[3])
      if(gen_code %in% code_list) {
        gen_code <- paste0(firstn, lastn, nums[4])
        if(gen_code %in% code_list) {
          gen_code <- paste0(firstn, lastn, nums[5])
          if(gen_code %in% code_list) {
            gen_code <- paste0(firstn, lastn, nums[6])
          }
        }
      }
    }
  }
  
  # if we were unable to generate a unique code for this user, skip. Otherwise save the code. Move to the next user
  if(gen_code %in% code_list) {
    print("duplicate, aborting")
  }
  else {
    missing_user_list$code[row] <- gen_code
    code_list <- c(code_list, gen_code)
  }
}

# check how we're doing here
missing_user_list[is.na(missing_user_list$code),]
length(c(as.list(unique(missing_user_list$code)), as.list(unique(user_list$code))))
length(unique(c(as.list(unique(missing_user_list$code)), as.list(unique(user_list$code)))))


# update the list to check whether we have full coverage
# updated_user_list <- user_list
# ind <- match(updated_user_list$user_id, missing_user_list$user_id)
# updated_user_list[ind, 4] <- missing_user_list[4]

# check whether users with new codes already exist in the u_referral_code tables (they shouldn't):
new_users <- paste(shQuote(missing_user_list$user_id), collapse=", ")
user_check <- queryDB(paste0("
SELECT 
    count(distinct r.user_id)
FROM etl_reporting.u_referral_code r 
where r.user_id in (",new_users,")
"), 'postgres-us')

# check query output
user_check

# merge our results
updated_user_list <- merge(user_list, subset(missing_user_list, select =c(user_id, code)), by="user_id", all.x=TRUE)
# verify that we don't have any missing codes
updated_user_list[is.na(updated_user_list$code)]

# export to CSV
write.csv(subset(missing_user_list, select=c(user_id, code)), "~/missing_user_referral_codes.csv", row.names = FALSE)

