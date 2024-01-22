#####
# Author: Dani Mermelstein
# Date: 20201202
# Description: 
#
#####

library(n26)
library(dplyr)
library(data.table)

# import external list
list <- read.csv("~/Downloads/socure_codes.csv", stringsAsFactors = FALSE)

code_fires <- queryDB(paste0("
with pre as (
select
  id,
  status,
  user_id,
  status_reason,
  trim(unnest(regexp_split_to_array(trim(both '[]' from status_reason) , ','))) as u_code,
  k.initiated
from
  km_processes k
where
  1 = 1
  and primary_document != 'SSN_CARD'
  and k.provider = 'SOCURE'
  and status in ('FAILED','IMPROPER_DOCS')
  and lower(status_reason) != 'limit exceeded'
  and initiated::date >= '2020-08-17'
  )
, groupings as (
  select
    pre.initiated,
    pre.id,
    pre.user_id,
    e.code,
    e.description
  from
    pre
  join dwh_er_reason_code_description e on
    u_code = e.code
)
select * from groupings
"), 'postgres-us')

# add in the erbium mapping for each code
code_mapping <- merge(code_fires, list, by.x="code", by.y="Code", all.x=TRUE)
code_mapping <- as.data.table(code_mapping)
# add in whether codes fire by themselves or with other codes
code_mapping <- merge(code_mapping, code_mapping[, .(codes=.N), by=.(id)], by="id", all.x=TRUE)
# add in whether other Fail mappings come together
merge(code_mapping, , by="id", all.x=TRUE)

code_mapping[which(code %in% c('I911',
'R110',
'R111',
'R186',
'R708',
'R923',
'R933',
'R940',
'R953',
'R954')), .(codes=.N), by=.(id)]


unique(code_mapping$code)

# aggregate to see the counts
code_mapping[, .(
  sole_error_attempts=length(unique(if_else(codes==1, id, NULL)))
), by=.(description, code, Erbium.Mapping)]

# look at whether the selected error codes fire by themselves

# look at whether the selected error codes fire with other codes where they are the only FAIL code


