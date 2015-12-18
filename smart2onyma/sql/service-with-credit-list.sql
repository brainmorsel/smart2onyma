SELECT DISTINCT
	st.id
	,st.name as svc_name

FROM core.accounts ac
JOIN core.accounts pr ON pr.id = ac.parent_id

JOIN core.service_items si ON si.account_id = ac.id
	AND CAST(si.have_credit AS INT) = 1
JOIN core.service_types st ON st.id = si.type_id
;
