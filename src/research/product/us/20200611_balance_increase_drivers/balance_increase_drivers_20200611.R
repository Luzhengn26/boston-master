library(n26)
library(data.table)
library(ggplot2)
library(RColorBrewer)
library(scales)
library(tidyr)


daily_balance <- queryDB("
select
    *
from
    (
    select
        created::date as date,
        sum(sum(amount_cents)) over (order by created::date)/100::numeric(32,2) as balance
    from au_transactions
    where created::date <= '2020-06-11'
    group by created::date
    ) as a
order by 1
desc limit 365
", 'postgres-us')

daily_balance <- as.data.table(daily_balance)

# add trend lines for different "eras" as needed
ggplot(daily_balance[date>='2020-01-01',], aes(x=date, y=balance))+
  geom_line(aes(y=balance))+
  # geom_smooth(method="lm", data=daily_balance[date>='2020-01-01' & date<'2020-02-20',], level=.2)+
  # geom_smooth(method="lm", data=daily_balance[date>='2020-02-21' & date<'2020-04-15',], level=.2)+
  # geom_smooth(method="lm", data=daily_balance[date>='2020-04-16',], level=.2)+
  labs(title="N26 US Balance - 2020",
       x ="day", 
       y = "amount")+
  scale_y_continuous(labels = dollar)


# for inflows:
  # look at the end of February 2020, mid-April 2020, and mid-June

funding <- queryDB("
select distinct
  t.user_id,
  t.is_first_time_mau,
  t.direction,
  t.is_internal_txn,
  t.type,
  t.txn_id,
  t.company,
  t.description,
  t.completed_tstamp,
  date_trunc('month', t.completed_tstamp)::date as month,
  date_trunc('week', t.completed_tstamp)::date as week,
  date_trunc('day', t.completed_tstamp)::date as day,
  t.bank_balance_impact_cents as amount_cents
from dbt.zrh_transactions t 
-- exclude fraudsters
join public.cr_user_account cua on cua.user_id = t.user_id
join public.cr_account ca on ca.id = cua.account_id and ca.status != 'CLOSED' and ca.status != 'SEIZED'
where t.is_micro_deposit is false 
  and t.type not in ('N26Fee')
  and t.completed_tstamp between '2020-01-01' and '2020-06-11'
order by t.user_id, t.completed_tstamp
",'postgres-us')

funding <- as.data.table(funding)


# overall trend
ggplot(funding[day>='2020-01-01' & direction=='Incoming' & is_internal_txn==FALSE,], aes(x=week, y=amount_cents/100, fill=type))+
  geom_bar(stat="identity")+
  scale_fill_brewer(palette = "Paired")+
  scale_y_continuous(labels = dollar)+
  labs(title="Weekly incoming transaction volume",
       x ="week", 
       y = "amount")


# end of February 2020 (incoming)
ggplot(funding[day>='2020-02-15' & day<'2020-03-01' & direction=='Incoming',], aes(x=day, y=amount_cents/100, fill=type))+
  geom_bar(stat="identity")+
  scale_fill_brewer(palette = "Paired")+
  scale_y_continuous(labels = comma)

# what are those ACH txns?
head(funding[day>='2020-02-24' & day<'2020-03-01' & direction=='Incoming' & type=='ACH', c("company","amount_cents","description")][, .(cnt=.N, amount=sum(amount_cents)/100), by=.(company)][order(-amount),], 10)
# looks like it's mostly IRS transactions. can we quantify as a percentage of the ACH increase? Maybe compare to the last week of the previous month
# also note that there is an increase in Spaces transactions -> this could indicate saving behavior


#  mid-April 2020
ggplot(funding[day>='2020-04-01' & day<'2020-05-01' & direction=='Incoming',], aes(x=day, y=amount_cents/100, fill=type))+
  geom_bar(stat="identity")+
  scale_fill_brewer(palette = "Paired")+
  scale_y_continuous(labels = comma)

# looks like ACH strikes again with federal stimulus payments
head(funding[(day=='2020-04-13' | day=='2020-04-27') & direction=='Incoming' & type=='ACH', c("company","amount_cents","description")][, .(cnt=.N, amount=sum(amount_cents)/100), by=.(company)][order(-amount),], 10)
# this would be stimulus checks. We can see there is a correlation between these txns and an increase in Spaces txns -> more saving behavior

# May to mid-June 2020
ggplot(funding[day>='2020-05-01' & direction=='Incoming' & is_internal_txn==FALSE,], aes(x=day, y=amount_cents/100, fill=type))+
  geom_bar(stat="identity")+
  scale_fill_brewer(palette = "Paired")+
  scale_y_continuous(labels = dollar)+
  labs(title="Weekly incoming transaction volume - $ amount",
       x ="week", 
       y = "amount")


# We're seeing ACH become an increasing source of inflows, in both amount and percentage. And people aren't spending all of it
ggplot(funding[day>='2020-01-01' & direction=='Incoming' & is_internal_txn==FALSE,], aes(x=week, y=amount_cents/100, fill=type))+ 
  geom_bar(position = "fill",stat = "identity") +
  scale_fill_brewer(palette = "Paired")+
  labs(title="Weekly incoming transaction volume - % of total",
       x ="week",
       y="")+
  scale_y_continuous(labels = scales::percent_format())

# the continued ACH has likely been unemployment insurance and subsequent stimulus checks (through state vehicles, not federal)
head(funding[day>='2020-05-01' & direction=='Incoming' & type=='ACH', c("company","amount_cents","description")][, .(cnt=.N, amount=sum(amount_cents)/100), by=.(company)][order(-amount),], 10)



# look at growth of inflows vs growth of outflows
mimo_funding <- funding[day>='2020-01-01',][, .(amount=sum(amount_cents)/100), by=.(week,direction)]
mimo_funding$amount <- ifelse(mimo_funding$direction=='Outgoing', mimo_funding$amount*-1, mimo_funding$amount)
# look at cumulative
mimo_funding <- spread(mimo_funding, direction, amount)
mimo_funding$run_in <- cumsum(mimo_funding[order(week),]$Incoming)
mimo_funding$run_out <- cumsum(mimo_funding[order(week),]$Outgoing)
mimo_funding$diff <- mimo_funding$run_in - mimo_funding$run_out

ggplot(mimo_funding, aes(x=week, y=run_in))+
  geom_line(color="blue")+
  geom_line(aes(y=run_out), color="red")+
  scale_y_continuous(labels = dollar)+
  labs(title="Cumulative inflows vs outflows since Jan 2020",
       x ="week", 
       y = "amount")

# the gap is getting larger
ggplot(mimo_funding, aes(x=week, y=diff))+
  geom_line()+
  scale_y_continuous(labels = dollar)+
  labs(title="Cumulative inflow surplus",
       x ="week", 
       y = "amount")

# users simply aren't spending as much as they are depositing
