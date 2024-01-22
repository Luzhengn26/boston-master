  with base as (
    -- base of all users
    select distinct
    kp.user_id,
    cu.user_created::date as signup_date,
    a.state,
    cu.first_ip,
    zu.account_status,
    case
    when kp.primary_document != 'SSN_CARD'
    and kp.provider = 'SOCURE'
    and zu.kyc_first_completed is not null then 1 else 0
    end as yf_kycc,
    case
    when zu.kyc_first_completed is not null then 1 else 0
    end as kycc
    from km_processes kp
    join dbt.zrh_users zu on zu.user_id = kp.user_id
    join etl_reporting.cmd_users cu on cu.id = kp.user_id
    join etl_reporting.cmd_address a on a.user_id = kp.user_id and a.type = 'FIRST_SHIPPING'
    where kp.created between '2019-07-01' and current_date
    and kp.status <> 'TIMED_OUT'
  )
  , fraud_labels as (
    select distinct
    lel.entity_id as user_id
    from public.lin_entity_label lel
    join public.lin_label ll on ll.id = lel.label_id
    where ll.name in ('AML - General',
                      'AML - Terrorist Financing',
                      'Fraud',
                      'Fraud - ACH',
                      'Fraud - Card - Chargeback',
                      'Fraud - Card - Stripe Top Up',
                      'Fraud - KYC - ID Theft',
                      'Fraud - OCT - CashApp',
                      'Fraud - Presentment Refund')
  )
  , socure_fails as (
    -- Users who immediately fail Socure (highest risk)
    select distinct
    u.id as user_id
    from cmd_users u
    join km_processes k on k.user_id = u.id
    where k.initiated is not null
    and k.status = 'FAILED'
    and k.primary_document = 'SSN_CARD'
    group by 1
  )
  , suspected_ip_fraud as (
    select
    u.id as user_id,
    count (distinct uu.id) ip_count
    from etl_reporting.cmd_users u
    left join etl_reporting.cmd_users uu on uu.first_ip = u.first_ip
    group by 1
  )
  , transaction_base as (
    select distinct
    user_id,
    case when direction ='Incoming' then min(completed_tstamp) over (partition by user_id, direction='Incoming') end as first_incoming_ts,
    case when direction ='Outgoing' then min(completed_tstamp) over (partition by user_id, direction='Outgoing') end as first_outgoing_ts,
    case when direction ='Incoming' then first_value(type) over (partition by user_id, direction='Incoming' order by completed_tstamp) end as first_incoming_type,
    case when direction ='Outgoing' then first_value(type) over (partition by user_id, direction='Outgoing' order by completed_tstamp) end as first_outgoing_type,
    case when direction ='Incoming' and type = 'DIR_DEP' then min(completed_tstamp) over (partition by user_id, direction, type) end as first_dir_dep_ts,
    case when direction ='Outgoing' and type = 'Card' then min(completed_tstamp) over (partition by user_id, direction, type) end as first_card_ts
    from dbt.zrh_transactions
    order by 1
  )
  , transaction_speed as (
    select
    tb.user_id,
    max(tb.first_incoming_ts) as first_incoming_ts,
    max(tb.first_outgoing_ts) as first_outgoing_ts,
    max(tb.first_incoming_type) as first_incoming_type,
    max(tb.first_outgoing_type) as first_outgoing_type,
    max(tb.first_dir_dep_ts) as first_dir_dep_ts,
    max(tb.first_card_ts) as first_card_ts,
    count(distinct zt.type) as multiple_deposit_types
    from transaction_base tb
    left join dbt.zrh_transactions zt on zt.user_id = tb.user_id and zt.direction = 'Incoming'
    group by 1
  )
  select
  b.user_id,
  b.signup_date,
  b.account_status,
  max(b.kycc) as kycc,
  max(b.yf_kycc) as yf_kycc,
  max(case when b.signup_date between '2020-08-15' and '2020-11-09' then 1 else 0 end) as yellow_flow_100,
  max(case when fl.user_id is not null then 1 else 0 end) as aml_fraud_flag,
  max(case when sf.user_id is not null then 1 else 0 end) as socure_fraud_flag,
  max(case when sif.ip_count > 3 then 1 else 0 end) as ip_flag,
  max(case when b.state in ('GA','FL','TX','IL','CA','NY') then 1 else 0 end) as state_flag,
  max(case when ts.first_outgoing_ts - ts.first_incoming_ts < interval '24 hours' then 1 else 0 end) as withdrawal_speed_flag,
  max(case when ts.first_outgoing_type in ('ATM', 'MoneyBeam', 'AFT') then 1 else 0 end) as withdrawal_type_flag,
  max(case when ts.first_dir_dep_ts is not null then 1 else 0 end) as dir_dep_user,
  max(case when ts.first_card_ts is not null then 1 else 0 end) as card_user,
  max(case when ts.multiple_deposit_types > 1 then 1 else 0 end) as multiple_deposit_types,
  max(case when c.card_activation_date is not null then 1 else 0 end) as card_activated
  from base b
  left join fraud_labels fl on fl.user_id = b.user_id
  left join socure_fails sf on sf.user_id = b.user_id
  left join suspected_ip_fraud sif on sif.user_id = b.user_id
  left join transaction_speed ts on ts.user_id = b.user_id
  left join public.th_cards c on c.user_id = b.user_id
  group by 1,2,3