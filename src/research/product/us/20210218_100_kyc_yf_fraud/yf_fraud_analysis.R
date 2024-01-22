# load libraries
library(n26)
library(data.table)
library(dplyr)
library(ggplot2)
library(tidyr)

# read and run query
fraud_query_loc <- 'src/research/product/deep_dive/100_kyc_yf_fraud/flags.sql' # will change the directory so that it's not tied to my machine
fraud_query <- readChar(fraud_query_loc, nchars = file.info(fraud_query_loc)$size)
base <- queryDB(fraud_query, "postgres-us")
base <- as.data.table(base)

# is the df unique on user id?
length(unique(base$user_id)) == length(base$user_id)

# selecting results from beta launch on
base <-
  base %>% filter(signup_date >= '2019-07-11') 

#### aggregate at signup cohort level ####
aggregated <- base[, .(
  signups = .N,
  kycc = sum(kycc),
  yf_kycc = sum(yf_kycc),
  yellow_flow_100 = sum(yellow_flow_100),
  aml_fraud = sum(aml_fraud_flag),
  socure_fraud = sum(socure_fraud_flag),
  aml_fraud_rate = sum(aml_fraud_flag / .N),
  socure_fraud_rate = sum(socure_fraud_flag / .N),
  ip_flag = sum(ip_flag),
  state_flag = sum(state_flag),
  speed_flag = sum(withdrawal_speed_flag),
  type_flag = sum(withdrawal_type_flag),
  dir_dep = sum(dir_dep_user),
  card_use = sum(card_user),
  card_activated = sum(card_activated),
  multiple_deposit_types = sum(multiple_deposit_types)
), by = .(signup_date)][order(signup_date),]

#### get aml fraud, socure fraud and KYCc rate ####
aggregated <-
  aggregated %>% mutate(
    `100_yf` = case_when(
      signup_date >= '2019-07-11' &
        signup_date < '2020-08-15' ~ 0,
      signup_date >= '2020-08-15' & signup_date < '2020-11-09' ~ 1,
      TRUE ~ 0
    ),
    post_yellow_flow = case_when(signup_date >= '2020-11-08' ~ 1,
                                 TRUE ~ 0)
  )

aggregated %>%
  group_by(post_yellow_flow, `100_yf`) %>%
  summarise(
    kycc = sum(kycc),
    user_count = sum(signups),
    kycc_cr = 100 * sum(kycc) / sum(signups),
    aml_fraud_rate = 100 * sum(aml_fraud) / sum(kycc),
    red_flow_rate = 100 * sum(socure_fraud) / sum(signups)
  )

aggregated %>%
  group_by(`100_yf`) %>%
  summarise(
    kycc = sum(kycc),
    user_count = sum(signups),
    kycc_cr = 100 * sum(kycc) / sum(signups),
    aml_fraud_rate = 100 * sum(aml_fraud) / sum(kycc),
    red_flow_rate = 100 * sum(socure_fraud) / sum(signups)
  )

#### correlation test ####
for (n in names(aggregated[,10:17])) {
  print(paste0("AML fraud and ", n))
  print(cor.test(aggregated$aml_fraud, aggregated[[n]]))
}


#### t-test ####
t.test(aml_fraud_rate ~ `100_yf`, data = aggregated)

# alternative calculation
with(aggregated, t.test(aml_fraud_rate[`100_yf` == 0], aml_fraud_rate[`100_yf` == 1]))


#### test of equal or given proportions ####
base <- base %>% mutate(
  `100_yf` = case_when(
    signup_date >= '2019-07-11' &
      signup_date < '2020-08-15' ~ 0,
    signup_date >= '2020-08-15' & signup_date < '2020-11-09' ~ 1,
    TRUE ~ 0
  ),
  post_yellow_flow = case_when(signup_date >= '2020-11-09' ~ 1,
                               TRUE ~ 0)
)

prop_df <-
  base %>% group_by(`100_yf`) %>% summarise(users = sum(kycc), fraud_rate = sum(aml_fraud_flag))
prop_df %>% summarise(fraud_rate / users)

res <-
  prop.test(prop_df$fraud_rate,
            prop_df$users,
            correct = F,
            conf.level = 0.95)

res$p.value # statistically significant -> we reject the null hypothesis
# the proportion of fraudsters across both groups is not the same

#### fraud rates by group ####
base %>% group_by(post_yellow_flow, kycc, yf_kycc) %>% summarise(
  fraud_rate = mean(aml_fraud_flag),
  red_flow = mean(socure_fraud_flag),
  count = n()
)

base %>% group_by(`100_yf`, kycc, yf_kycc) %>% summarise(
  fraud_rate = mean(aml_fraud_flag),
  red_flow = mean(socure_fraud_flag),
  count = n()
) 

###
# keeping the graphs below for the next step of this research project
###

#### over time trend of various flags ####
# calculate ratio of flags over total signups count
aggregated %>%
  gather(variable, value, signups:multiple_deposit_types) -> agg_long

aggregated %>%
  mutate(
    ip_flag = ip_flag / signups,
    state_flag = state_flag / signups,
    speed_flag = speed_flag / signups,
    type_flag = type_flag / signups,
    dir_dep = dir_dep / signups,
    card_use = card_use / signups,
    card_activated = card_activated / signups,
    multiple_deposit_types = multiple_deposit_types / signups
  ) %>%
  gather(variable, value, signups:multiple_deposit_types) -> agg_long_ratio

# every variable is a ratio over signup cohort size
ggplot(filter(
  agg_long_ratio,
  variable %in% c(
    # 'signups',
    # 'socure_fraud',
    'aml_fraud_rate',
    'ip_flag',
    'state_flag',
    'dir_dep',
    'speed_flag',
    'card_use'
    # 'card_activated'
  )
), aes(x = signup_date)) + geom_line(aes(y = value, col = variable)) +
  labs(
    x = 'Signup date',
    y = 'Value count',
    title = 'Signup fraud and relative flags trend',
    subtitle = 'All metrics calculated as % of signup cohort'
  ) +
  geom_vline(xintercept = as.Date('2020-08-15'), show.legend = T) +
  theme_classic()


# non-ratio version
ggplot(filter(
  agg_long,
  variable %in% c(
    'signups',
    # 'socure_fraud',
    'aml_fraud_rate',
    'ip_flag',
    'state_flag',
    'dir_dep',
    'speed_flag',
    'card_use'
    # 'card_activated'
  )
), aes(x = signup_date)) + geom_line(aes(y = value, col = variable)) +
  labs(
    x = 'Signup date',
    y = 'Value count',
    title = 'Signup fraud and relative flags trend',
    subtitle = 'Line indicates 100% KYC YF introduction'
  ) +
  geom_vline(xintercept = as.Date('2020-08-15'), show.legend = T) +
  theme_classic()