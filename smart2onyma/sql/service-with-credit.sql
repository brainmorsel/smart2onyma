SELECT DISTINCT
	 pr.id
	,pr.account_number
	,si.id as conn_id
	,si.credit_first_payment
	,si.credit_monthly_payment
	,si.type_id
	,st.name as svc_name
	,ch_s.ch_start_date  -- дата следующего списания
	,ch_e.ch_count       -- количество оставшихся списаний

FROM core.accounts ac
JOIN core.accounts pr ON pr.id = ac.parent_id

JOIN core.service_items si ON si.account_id = ac.id
	AND CAST(si.have_credit AS INT) = 1
JOIN core.service_types st ON st.id = si.type_id

JOIN (
	SELECT item_id, MIN(start_date) as ch_start_date FROM core.service_item_charges
	WHERE start_date > CURRENT_DATE
	GROUP BY item_id
) ch_s ON ch_s.item_id = si.id
JOIN (
	SELECT item_id, COUNT(start_date) as ch_count FROM core.service_item_charges
	WHERE start_date > CURRENT_DATE
	GROUP BY item_id
) ch_e ON ch_e.item_id = si.id

WHERE pr.account_number = :account_number
;
