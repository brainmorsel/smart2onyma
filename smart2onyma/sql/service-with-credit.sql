SELECT DISTINCT
	 pr.id as account_id
	,pr.account_number
	,ac.id as conn_id
	,si.credit_first_payment
	,si.credit_monthly_payment
	,si.type_id
	,st.name as name
	,ch_s.ch_start_date as start_date -- дата следующего списания
	,ch_e.ch_count       -- количество оставшихся списаний
	,ch_s.ch_start_date + ch_e.ch_count * interval '1 month' as end_date

FROM core.accounts ac
JOIN core.accounts pr ON pr.id = ac.parent_id

JOIN core.service_items si ON si.account_id = ac.id
	AND CAST(si.have_credit AS INT) = 1
JOIN core.service_types st ON st.id = si.type_id

JOIN (
	SELECT item_id, MIN(start_date) as ch_start_date FROM core.service_item_charges
	WHERE start_date > date_trunc('month', CURRENT_DATE)
	GROUP BY item_id
) ch_s ON ch_s.item_id = si.id
JOIN (
	SELECT item_id, COUNT(start_date) as ch_count FROM core.service_item_charges
	WHERE start_date > date_trunc('month', CURRENT_DATE)
	GROUP BY item_id
) ch_e ON ch_e.item_id = si.id

--WHERE pr.account_number = :account_number
;
