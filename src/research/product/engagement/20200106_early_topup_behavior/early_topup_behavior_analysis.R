setwd('/Users/wendyvu/Documents/Enable_Analysis')
df <- read.csv('txn_202001121633.csv',header=T,na.strings=c(""))
df <- read.csv('txn_202001132126.csv',header=T,na.strings=c(""))

df$mau_periods[df$mau_periods==7] <- 6

summary(df)

# Number of users that completed at least 1 CT in cohort
dim(df) # n = 224190

# How many users completed their first CT within 35 days of kycc?
df.ct35 <- df[df[,4]<=35,]
dim(df.ct35) # n = 183327

# How many users had at least 2 CT's within 35 days
df.sec.ct35 <- df.ct35[!is.na(df.ct35[,13]),]
dim(df.sec.ct35) # n = 115888

# plot the percent of users that complete kycc relative to days
ct <- as.data.frame(table(df$days_kycc_ct))
ct['Percent'] <- cumsum(ct[,2])/sum(ct[,2])
plot(ct[as.numeric(ct[,1]) <= 35,1],ct[as.numeric(ct[,1]) <= 35,3],xlim=c(0,35))

## REMOVE USERS THAT DEPOSIT > 5000 (THEY ONLY MAKE UP ~2% OF THE COHORT)
df1 <- df[df[,5]<5000,]


idx <- rownames(df1[(df1[,14] > 5000) & (!is.na(df1[,14])),])
df1 <- df1[!rownames(df1) %in% idx,]

#remove weird outlier from first_ct_35
idx <- rownames(df1[(df1[,12] > 5000) & (!is.na(df1[,12])),])
df1 <- df1[!rownames(df1) %in% idx,]
dim(df1) # n = 221414

################# CORRELATION BETWEEN FEATURES ##########################

# scale mean = 0 and unit variance
df.scaled = scale(df1[,c(4,5,7:9,11:18)], center=TRUE, scale=TRUE)

df.scaled = scale(df1[,c(4,5,7:9,14,17,18)], center=TRUE, scale=TRUE)



res.cor <- cor(df.scaled,use="pairwise.complete.obs")

# scale max/min (range 0,1)
range01 <- function(x){(x-min(x,na.rm=T))/(max(x,na.rm=T)-min(x,na.rm=T))}
df.scaled = range01(df[,c(4,5,7:9,11:16)])
res.cor <- cor(df.scaled,use="pairwise.complete.obs")

library(Hmisc)
rcorr.mat <- rcorr(df.scaled)
res.cor <- rcorr.mat$r
pval.cor <- rcorr.mat$P

library("lattice")
library(RColorBrewer)
coul <- colorRampPalette(brewer.pal(8, "PiYG"))(25)
levelplot(res.cor,scales=list(x=list(rot=90)),col.regions = coul)

library(corrplot)
corrplot(res.cor, type="upper", order="hclust", 
         tl.col="black", tl.srt=45,diag=FALSE,addCoef.col="black")

######### BREAK USERS UP INTO TOTAL MAU MONTHS #######################

# period groups and frequency
grp <- as.data.frame(table(df1$mau_periods))
grp$percent_users <- grp[,2]/sum(grp[,2])*100
barplot(grp[,3]~grp[,1], xlab='User groups by Total Number of MAU periods (35 days)',ylab='Percent of Users')


library(dplyr)
cohort_mau <- as.data.frame(df %>% group_by(cohort,mau_periods) %>% tally())

library('ggplot2')                              
ggplot(cohort_mau, aes(fill=cohort, y=n, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") + 
    labs(x='Total Months Active (MAU)',y='Count of number of Users')+
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))



# Are there any differences between the active mau groups?
med = as.data.frame(df1[,c(3:5,7:18)] %>% 
                                group_by(cohort,mau_periods) %>%
                                summarise_all("median",na.rm=T))
                                
mean = as.data.frame(df1[,c(3:5,7:18)] %>% 
                                group_by(cohort,mau_periods) %>%
                                summarise_all("mean",na.rm=T))

###################### GGPLOT ##########################
# are there any differences between Active MAU segments?  
library('ggplot2')                              
ggplot(med, aes(fill=cohort, y=sum_amount_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") 
    + theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))
   

ggplot(med, aes(fill=cohort, y=tot_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")

ggplot(med, aes(fill=cohort, y=days_kycc_ct, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")

ggplot(med, aes(fill=cohort, y= first_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")

ggplot(mean, aes(fill=cohort, y= sec_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")
    
ggplot(med, aes(fill=cohort, y= tot_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")
    
ggplot(med, aes(fill=cohort, y= tot_ext_txn35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")

ggplot(med, aes(fill=cohort, y= hrs_signup_kycc, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity")



ggplot(df1.pca, aes(x=as.factor(mau_periods), y=days_kycc_ct, fill=cohort)) + 
    geom_boxplot() + scale_y_continuous(limits=c(0,50)) +
    theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))
                           
ggplot(df1, aes(x=as.factor(mau_periods), y=tot_ct_35, fill=cohort)) + 
    geom_boxplot() + theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))

ggplot(df1, aes(x=as.factor(mau_periods), y=tot_ext_txn35, fill=cohort)) + 
    geom_boxplot() + scale_y_continuous(limits=c(0,250)) +
    theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))

ggplot(df1, aes(x=as.factor(mau_periods), y=first_ct_euro, fill=cohort)) + 
    geom_boxplot(na.rm=T)  +
    theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45)) + ylim(0,500)
                           
ggplot(df1, aes(x=as.factor(mau_periods), y=sec_ct_35, fill=cohort)) + 
    geom_boxplot(na.rm=T) + scale_y_continuous(limits=c(0,500)) +
    theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))  +
    geom_text(data=mean,aes(label=round(sec_ct_35,0),y=sec_ct_35 -100))                        

ggplot(df1, aes(x=as.factor(mau_periods), y=(sum_amount_ct_35), fill=cohort)) + 
    geom_boxplot(na.rm=T) + scale_y_continuous(limits=c(0,1500)) +
    theme(axis.text.x = element_text(face="bold", color="#993333", 
                           size=14, angle=45),
          axis.text.y = element_text(face="bold", color="#993333", 
                           size=14, angle=45))




################# PCA: ONLY looking at data within 35 days of kycc ###################

# DATA PREPROCESSING
# data from users that completed at least 2 CT's within the first 35 days
# This was done bc we are interested in understanding the magnitude of effect of first and second topup within first 35 days of kycc in future user activity and remove NA values from PCA 
# Resulting in using only 50% of all users that are txn active
df.pca <- df1[,c(3,10,4,11,12,13,14,15,16)] #first/second CT amount within 35 days
df.pca <- df1[,c(3,10,4,11,12,14,15,16)] #first CT amount within 35 days


# users that completed only 1 CT within 35 days 
df.pca <- df1[,c(3,10,4,11,12,13,14,15,16)] #first/second CT amount within 35 days
rm.sec <- df.pca[is.na(df.pca[,6]),]
rm.sec <- rm.sec[,c(1:3,5,7:9)]
df.pca <- na.omit(rm.sec) # N= 65K


# only first CT amount
df.pca <- df1[,c(3,10,4,11,12,14,15,16)] 
dim(df.pca)
df.pca <- na.omit(df.pca) # N=114854

# NOT within 35 days CTs 
df.pca <- df1[,c(3,10,4,5,14:17)]
dim(df.pca)
df.pca <- na.omit(df.pca) # N=114854



library("FactoMineR")
library("factoextra")

res.pca <- PCA(df.pca[,3:7], graph=FALSE,ncp=20, scale.unit=T)
eig.val <- get_eigenvalue(res.pca)
eig.val

# variance explained by each dimension/component
# ~40% of variance is explained by the first 3 components
# pc's with eigenvalue > 1 are kept
fviz_eig(res.pca, addlabels = T, ylim=c(0,70))

var <- get_pca_var(res.pca)

library("corrplot")
corrplot(var$cos2, is.corr=FALSE)

########### ORDINAL LOGISTIC REGRESSION #############

require(foreign)
require(ggplot2)
require(MASS)
require(Hmisc)
require(reshape2)

pc <- res.pca$ind$coord[,1:4]

colnames(pc) <- c("sec.CT35_sum.CT35","Tot.CT35_Tot.Ext35","signup.kycc","first.CT35","days.kycc.ct")
df1.pca <- na.omit(df1)

df.pca

m <- polr(as.factor(df1.pca$mau_periods) ~ pc[,1]+pc[,2]+pc[,3]+pc[,4]+df1.pca$days_kycc_ct, Hess=TRUE)

# remove second ct within 35 days
m <- polr(as.factor(df.pca$mau_periods) ~ pc[,1]+pc[,2]+pc[,3]+df.pca$days_kycc_ct, Hess=TRUE)


m <- polr(as.factor(df.pca$mau_periods) ~ pc[,1]+pc[,3]+pc[,4]+df.pca$days_kycc_ct, Hess=TRUE)

summary(m)

# store table
(ctable <- coef(summary(m)))
    
# calculate and store p values
p <- pnorm(abs(ctable[, "t value"]), lower.tail = FALSE) * 2


# get confidence interval
(ci <- confint(m)) # default method gives profiled CIs

# get odd ratio (OR) and CI
exp(coef(m))
or_table <- as.data.frame(exp(cbind(OR = coef(m), ci)))
rownames(or_table) <- c("sec.CT35_sum.CT35","Tot.CT35_Tot.Ext35","signup.kycc","first.CT35","days.kycc.ct")


################# CORRELATION BETWEEN FEATURES data within 35 days ##########################

# scale mean = 0 and unit variance
df.scaled = scale(df1.pca[,c(4,7:9,11:16)], center=TRUE, scale=TRUE)
res.cor <- cor(df.scaled,use="pairwise.complete.obs")


library(corrplot)
corrplot(res.cor, type="upper", order="hclust", 
         tl.col="black", tl.srt=45,diag=FALSE,addCoef.col="black")
         
# boxplot of first and second CT amounts
boxplot(df1.pca$sec_ct_35, ylim=c(0,1000))

################# MEDIAN AND MEAN ##################################
# Are there any differences between the active mau groups?
library(dplyr)
med = as.data.frame(df1.pca[,c(3:5,7:16)] %>% 
                                group_by(cohort,mau_periods) %>%
                                summarise_all("median",na.rm=T))
                                
mean = as.data.frame(df1.pca[,c(3:5,7:16)] %>% 
                                group_by(cohort,mau_periods) %>%
                                summarise_all("mean",na.rm=T))


################ PLOTTING OLR RESULTS ############################


ggplot(med, aes(fill=cohort, y=tot_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))


ggplot(med, aes(fill=cohort, y=days_kycc_ct, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))
        
ggplot(med, aes(fill=cohort, y= first_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))

ggplot(med, aes(fill=cohort, y= sec_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") + 
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))
    
ggplot(med, aes(fill=cohort, y= tot_ext_txn35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))
    

ggplot(med, aes(fill=cohort, y= hrs_signup_kycc, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))    

ggplot(med, aes(fill=cohort, y= sum_amount_ct_35, x=as.factor(mau_periods))) + 
    geom_bar(position="dodge", stat="identity") +
    theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))    

 scale_y_continuous(limits=c(0,40))
ggplot(df1.pca, aes(x=as.factor(mau_periods), y=days_kycc_ct, fill=cohort)) + 
    geom_boxplot() + ylim(0,35)+                         
     theme(
    	axis.title = element_text(color = "black", size = 14, face="bold"),
    	axis.text.x = element_text(face="bold", color="black", 
                           size=14, angle=45),
        axis.text.y = element_text(face="bold", color="black", 
                           size=14, angle=45),
        legend.position="top",
        legend.title = element_text(color="black", size=14, face='bold'),
        legend.text = element_text(color="black",size=12, face='bold'))
        

