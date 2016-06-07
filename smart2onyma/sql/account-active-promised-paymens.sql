SELECT
	 ac.account_number
	,pp.amount
	,pp.expire_date


FROM core.accounts ac
JOIN core.tx_items tx ON tx.account_id = ac.id AND tx.type = 2
JOIN core.promised_payments pp ON pp.tx_id = tx.transaction_id AND pp.rb_tx_id IS NULL AND pp.expire_date > CURRENT_DATE

WHERE
	ac.parent_id IS NULL AND ac.account_number IS NOT NULL
