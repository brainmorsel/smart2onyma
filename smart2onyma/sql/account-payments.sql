SELECT acc.account_number, pay.payment_date, tx.transaction_id, tx.sum
FROM core.payments pay
INNER JOIN core.tx_items tx ON pay.tx_id = tx.transaction_id and tx.sum > 0
INNER JOIN core.accounts acc ON tx.account_id = acc.id

{% if sql_dialect == 'oracle' %}
WHERE pay.payment_date > TRUNC(CURRENT_DATE, 'MONTH')
{% else %}
WHERE pay.payment_date > date_trunc('month', CURRENT_DATE)
{% endif %}
AND pay.rollback_date IS NULL AND pay.status = 1
AND acc.account_number = :account_number

