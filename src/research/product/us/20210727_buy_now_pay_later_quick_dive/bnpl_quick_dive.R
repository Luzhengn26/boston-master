library(n26)
library(ggplot2)
library(data.table)

kDataPath <- file.path("~/src/boston/src/research/product/us/20210727_buy_now_pay_later_quick_dive","data")

card_txns <- queryDB(paste0("
select 
 ft.user_id,
 ft.completed_tstamp,
 ft.mcc_group,
 case when mc.irs_description is not null then mc.irs_description else 'Other' end as mcc_description,
 ft.bank_balance_impact*-1 as amount
from dbt.f_transactions ft 
join dbt.dim_users u on u.user_id = ft.user_id and u.first_fraud_flag is null
left join dbt.mcc_codes mc on mc.mcc = ft.mcc_group 
where ft.type = 'Card'
 and ft.direction = 'Outgoing'
"),'redshift-us')

card_txns <- as.data.table(card_txns)

balances <- queryDB(paste0("
select 
 avg(ending_balance) as average_amount,
 median(ending_balance) as median_amount
from dbt.f_daily_account_balance_agg
where date = '2021-07-26'
 and account_role = 'PRIMARY'
 and ending_balance <> 0
"),'redshift-us')

# save average balance figures
write.csv(balances, file=file.path(kDataPath,"avg_balances.csv"))

# percentage of transactions that are between $100 - $500
length(card_txns[amount>100 & amount < 500,]$amount)/length(card_txns$amount)

length(card_txns[amount<59,]$amount)/length(card_txns$amount)

# 1. Open the image file
png(file.path(kDataPath, "card_txn_distribution.png"), width = 350, height = 350)
# 2. Create the plot
ggplot(card_txns, aes(x=amount))+
 geom_histogram()+
 geom_vline(xintercept=59, colour="blue")+
 xlim(0,250)+
 labs(title="Distribution of Transaction Amounts",
      subtitle="$59 = 90th percentile",
      x ="Amount",
      y = "# of Txns")
# 3. Close the file
dev.off()

# aggregate and save stats by category
write.csv(card_txns[,.(txn_cnt=.N,
            user_cnt=length(unique(user_id)),
            gross_amount=sum(amount),
            amount_per_user=sum(amount)/length(unique(user_id)),
            amount_per_txn=sum(amount)/.N
            ), 
         by=.(mcc_description)]
 , file=file.path(kDataPath,"category_spending.csv"))