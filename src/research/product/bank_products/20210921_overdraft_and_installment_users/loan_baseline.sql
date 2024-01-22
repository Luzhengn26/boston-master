
-- we start from mambu data to get information on the loan account and interst.
with mmb_tbil as (
    select user_created,
        approved_date as mmb_approved_date,
        encoded_key,
        interest_rate, 
        principal_balance_cents,
        principal_paid_cents,
        account_state,
        row_number() over(partition by user_created order by approved_date) as aud_order
    from mmb_loan_account mla 
    where loan_name in ('Transaction-based instalment loans (public beta)', 'Transaction-based instalment loans (public beta and public launch)', 'Transaction Based Installment Loan')
        and approved_date between '2021-07-01' and '2021-08-31' --timeframe of the research
), 
--in nihohium we get more specific loan information such as the transaction id and disbursement date
nh_tbil as (
    select 
        user_id,
        u.user_created,
        approved_date as nh_approved_date,
        nh.id as nh_id,
        amount as loan_amount_eur,
        mcc_category,
        row_number() over(partition by user_id order by approved_date) as aud_order
    from nh_transaction_instalment_loan nh
    inner join dbt.zrh_users u using (user_id)
    inner join dbt.zrh_card_transactions t
        on nh.transaction_id = t.id
    where approved_date between '2021-07-01' and '2021-08-31'
)
-- since there is no common key to join both mambu and nihonium, we will use the loan order to join these
select *,
    max(aud_order) over (partition by user_created) as n_loans
from mmb_tbil
inner join nh_tbil using (user_created, aud_order)