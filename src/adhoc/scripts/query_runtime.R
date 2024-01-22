library(n26)

query_runtime <- function(query){
  speeds <- c()
  for (r in 1:15) {
    time1 <- Sys.time()
    
    queryDB(paste0(
    query
    ), 'redshift-us')
    
    time2 <- Sys.time()
    
    speeds[r] <- difftime(time2, time1, units="secs")
    print(paste0("query run complete: #", r))
  }
  return(list(speeds))
}

old_moneybeam <- "
select
       mo_moneybeam.id as txn_id
, getdate()
, case when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (split_part(ace1.external_id, '|', 2)) else (split_part(ace2.external_id, '|', 2)) end as Account_Number1
, case when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (cra1.status) else (cra2.status) end as Account_Status1
, case when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (cmd1.first_name || ' ' || cmd1.last_name) else (cmd2.first_name || ' ' || cmd2.last_name) end as Customer_Name
, to_char (mo_moneybeam.created, 'MM/DD/YYYY, HH:MI:SS AM') as Transaction_Date 
, mo_moneybeam.status as Status
, case when mo_moneybeam.type = 'SEND' and mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then mo_moneybeam.amount :: numeric * -1
          when mo_moneybeam.type = 'SEND' and mo_moneybeam.target_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then mo_moneybeam.amount :: numeric
          when mo_moneybeam.type = 'REQUEST' and mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then mo_moneybeam.amount :: numeric
          when mo_moneybeam.type = 'REQUEST' and mo_moneybeam.target_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then mo_moneybeam.amount :: numeric * -1
          else null end as Amount
, case when mo_moneybeam.type = 'SEND' and mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then 'Outgoing'
          when mo_moneybeam.type = 'SEND' and mo_moneybeam.target_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then 'Incoming'
          when mo_moneybeam.type = 'REQUEST' and mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then 'Incoming'
          when mo_moneybeam.type = 'REQUEST' and mo_moneybeam.target_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then 'Outgoing'
          else 'N/A' end as Credit_Debit
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (cmd2.first_name || ' ' || cmd2.last_name) else (cmd1.first_name || ' ' || cmd1.last_name) end as Originator_Beneficiary
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (split_part(ace2.external_id, '|', 2)) else (split_part(ace1.external_id, '|', 2)) end as Account_Number
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (cra2.status) else (cra1.status) end as Account_Status
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then ('https://metabase-fincrime.tech26.us/question/206?user_id=' || mo_moneybeam.target_user_id) else ('https://metabase-fincrime.tech26.us/question/206?user_id=' || mo_moneybeam.initiator_user_id) end as MB_Check
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then ('https://dash26.tech26.us/users/' || mo_moneybeam.target_user_id) else ('https://dash26.tech26.us/users/' || mo_moneybeam.initiator_user_id) end as Dash_Link
, case when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then ('https://metabase-fincrime.tech26.us/question/188?user_id=' || mo_moneybeam.target_user_id) else ('https://metabase-fincrime.tech26.us/question/188?user_id=' || mo_moneybeam.initiator_user_id)
          end as Account_Transactions
from public.mo_moneybeam
inner join etl_reporting.cmd_users cmd1 on cmd1.id = mo_moneybeam.initiator_user_id
inner join etl_reporting.cmd_users cmd2 on cmd2.id = mo_moneybeam.target_user_id
inner join etl_reporting.cmd_address cma1 on cma1.user_id = mo_moneybeam.initiator_user_id
      and cma1.type = 'LEGAL' --legal address only
inner join etl_reporting.cmd_address cma2 on cma2.user_id = mo_moneybeam.target_user_id
         and cma2.type = 'LEGAL' --legal address only
left join etl_reporting.zr_transaction on zr_transaction.user_certified = mo_moneybeam.created and zr_transaction.account_id = mo_moneybeam.target_user_id
left join etl_reporting.cr_account_external_id ace1 on ace1.account_id = mo_moneybeam.initiator_account_id
left join etl_reporting.cr_account_external_id ace2 on ace2.account_id = mo_moneybeam.target_account_id
inner join public.mo_moneybeam mom on mom.deduplication_id = mo_moneybeam.deduplication_id
left join public.cr_user_account cua1 on cua1.account_id = mo_moneybeam.initiator_account_id
left join public.cr_user_account cua2 on cua2.account_id = mo_moneybeam.target_account_id
left join public.cr_account cra1 on cra1.id = cua1.account_id
        and cra1.account_role = 'PRIMARY'
left join public.cr_account cra2 on cra2.id = cua2.account_id
        and cra2.account_role = 'PRIMARY'
where '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id, mo_moneybeam.target_user_id)
and mo_moneybeam.status = 'COMPLETED' 
"

new_moneybeam <- "
    select
        mo_moneybeam.id as txn_id,
        getdate(),
        case
            when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (split_part(ace1.external_id, '|', 2))
            else (split_part(ace2.external_id, '|', 2))
        end as Account_Number1,
        case
            when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (cra1.status)
            else (cra2.status)
        end as AccountStatus1,
        case
            when '21f6c1dc-652c-45af-8d8b-07333e632291' in (mo_moneybeam.initiator_user_id) then (cmd1.first_name || ' ' || cmd1.last_name)
            else (cmd2.first_name || ' ' || cmd2.last_name)
        end as CustomerName,
        case
            when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (cmd2.first_name || ' ' || cmd2.last_name)
            else (cmd1.first_name || ' ' || cmd1.last_name)
        end as OriginatorBeneficiary,
        case
            when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (split_part(ace2.external_id, '|', 2))
            else (split_part(ace1.external_id, '|', 2))
        end as AccountNumber,
        case
            when mo_moneybeam.initiator_user_id = '21f6c1dc-652c-45af-8d8b-07333e632291' then (cra2.status)
            else (cra1.status)
        end as AccountStatus
    from
        public.mo_moneybeam
        inner join etl_reporting.cmd_users cmd1 on cmd1.id = mo_moneybeam.initiator_user_id
        inner join etl_reporting.cmd_users cmd2 on cmd2.id = mo_moneybeam.target_user_id
        left join etl_reporting.cr_account_external_id ace1 on ace1.account_id = mo_moneybeam.initiator_account_id
        left join etl_reporting.cr_account_external_id ace2 on ace2.account_id = mo_moneybeam.target_account_id
        inner join public.mo_moneybeam mom on mom.deduplication_id = mo_moneybeam.deduplication_id
        left join public.cr_user_account cua1 on cua1.account_id = mo_moneybeam.initiator_account_id
        left join public.cr_user_account cua2 on cua2.account_id = mo_moneybeam.target_account_id
        left join public.cr_account cra1 on cra1.id = cua1.account_id
        and cra1.account_role = 'PRIMARY'
        left join public.cr_account cra2 on cra2.id = cua2.account_id
        and cra2.account_role = 'PRIMARY'
    where
        '21f6c1dc-652c-45af-8d8b-07333e632291' in (
            mo_moneybeam.initiator_user_id,
            mo_moneybeam.target_user_id
        )
        and mo_moneybeam.status = 'COMPLETED'
"


better_speeds <- query_runtime(new_moneybeam)
orig_speeds <- query_runtime(old_moneybeam) # last element will be a dataframe


mean(orig_speeds[[1]][2:15])
mean(better_speeds[[1]])

