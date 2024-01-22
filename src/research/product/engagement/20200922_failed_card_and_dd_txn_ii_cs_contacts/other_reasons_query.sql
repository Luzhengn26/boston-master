with totals as (
Select
case when response_code = 5 then 'Do not honour'
when response_code = 55 then 'Incorrect PIN'
when response_code = 59 then 'Suspected fraud'
when response_code = 54 then 'Expired card'
when response_code = 75 then 'PIN tries'
when reject_reason = 'INSUFFICIENT_FUNDS' or response_code = 51 then 'Insufficient funds'
when reject_reason = 'LIMIT_EXCEEDED' then 'Card limits'
when reject_reason = 'NOT_ACCEPTABLE_ENTRY_MODE' or reject_reason = 'NOT_ACCEPTABLE_COUNTRY' then 'Settings'
when reject_reason = 'ACCOUNT_SEIZED' then 'Account seized'
when reject_reason = 'CARD_DISABLED' then 'Card disabled'
when response_code = 0 then 'Approved'
when response_code = 1 then 'Refer to Card Issuer'
when response_code = 2 then 'Refer to Card Issuer, special condition'
when response_code = 3 then 'Invalid Merchant'
when response_code = 4 then 'Pick up card'
when response_code = 5 then 'Do not honour'
when response_code = 6 then 'Error'
when response_code = 7 then 'Pick up card, special condition'
when response_code = 10 then 'Partial Approval'
when response_code = 12 then 'Invalid Transaction'
when response_code = 13 then 'Invalid Amount'
when response_code = 14 then 'Invalid card number'
when response_code = 19 then 'Re-enter transaction'
when response_code = 21 then 'No action taken'
when response_code = 30 then 'Format Error'
when response_code = 41 then 'Lost card pick up'
when response_code = 43 then 'Stolen card pick up'
when response_code = 52 then 'No checking account'
when response_code = 53 then 'No savings account'
when response_code = 54 then 'Expired card'
when response_code = 55 then 'PIN incorrect'
when response_code = 57 then 'Transaction not allowed for cardholder'
when response_code = 58 then 'Transaction not allowed for merchant'
when response_code = 59 then 'Suspected fraud'
when response_code = 61 then 'Exceeds withdrawal amount limit'
when response_code = 62 then 'Restricted card'
when response_code = 63 then 'Security violation'
when response_code = 65 then 'Activity count limit exceeded'
when response_code = 75 then 'PIN tries exceeded'
when response_code = 77 then 'Inconsistent with original'
when response_code = 78 then 'No account'
when response_code = 84 then 'Preauthorisation time too great'
when response_code = 86 then 'Cannot verify PIN'
when response_code = 91 then 'Issuer unavailable'
when response_code = 92 then 'Invalid receiving institution id'
when response_code = 93 then 'Transaction violates law'
when response_code = 94 then 'Duplicate transaction'
when response_code = 96 then 'System malfunction'
else reject_reason 
end as rejection_reason,
count(*) as ar_count
from dwh_sneaky_transaction
where type = 'AR'
and created::date between '2020-06-01' and '2020-08-31'
group by 1
)
select
rejection_reason as reason_detail,
ar_count
from totals 
order by 2 desc