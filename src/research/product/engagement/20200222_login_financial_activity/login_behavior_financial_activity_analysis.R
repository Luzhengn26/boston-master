
setwd('/Users/wendyvu/Documents/Engage_Analysis')
df <- read.csv('logins_activity_cohort_202002211107.csv',header=T)

summary(df)

hist(df$login_daily_avg)

# CORRELATION BETWEEN FEATURES
df.scaled = scale(df[,c(5:15)], center=TRUE, scale=TRUE)
res.cor <- cor(df.scaled,use="pairwise.complete.obs")

library(corrplot)
corrplot(res.cor, type="upper", order="hclust", 
         tl.col="black", tl.srt=45,diag=FALSE,addCoef.col="black")

# Are there any differences between the active mau groups?
library(dplyr)

cohort_mau <- as.data.frame(df %>% group_by(cohort,mau_act) %>% tally())
ggplot(cohort_mau, aes(fill=cohort, y= n, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Number of users")

med = as.data.frame(df[,c(2,8,5,6,7,10:15)] %>% 
                      group_by(cohort,mau_act) %>%
                      summarise_all("median",na.rm=T))

mean = as.data.frame(df[,c(2,8,5,6,7,10:15)] %>% 
                       group_by(cohort,mau_act) %>%
                       summarise_all("mean",na.rm=T))

cbPalette <- c("#0072B2", "#D55E00", "#CC79A7","#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442")

library('ggplot2') 
ggplot(mean, aes(fill=cohort, y= login_monthly_avg, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Avg Number of logins per month")

ggplot(mean, aes(fill=cohort, y= login_weekly_avg, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Avg Number of logins per week")

ggplot(mean, aes(fill=cohort, y= login_daily_avg, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Avg Number of logins per day")


ggplot(mean, aes(fill=cohort, y= kycc_log_days, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Avg Number of days from KYCc to Login")

ggplot(med, aes(fill=cohort, y= kycc_log_days, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Median Number of days from KYCc to Login")


ggplot(med, aes(fill=cohort, y= kycc_log_days, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=14, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=14, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Median Number of days from KYCc to Login")

ggplot(med, aes(fill=cohort, y= txns_monthly_avg, x=as.factor(mau_act))) + 
  geom_bar(position="dodge", stat="identity") + scale_fill_manual(values=cbPalette) + 
  theme(axis.text.x = element_text(face="bold", color="#000000", 
                                   size=16, angle=45),
        axis.text.y = element_text(face="bold", color="#000000", 
                                   size=16, angle=45)) +
  xlab("Total Number of Active Months (>= 1 Deposit per Month)") +
  ylab("Avg number of txns per Month")


users <-rownames(df)
sample <- sample(users, size=10000)
df_sample = df[rownames(df) %in% sample,]
plot(df_sample$login_weekly,df_sample$txns_monthly_avg,ylim=c(0,200),xlim=c(0,30),pch=2)


as.data.frame(table(df[,c(8,4)]))
