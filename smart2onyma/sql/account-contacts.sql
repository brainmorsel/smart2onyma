SELECT DISTINCT
	 ac.id
	,i.info
	,CASE i.type
		WHEN 1 THEN 'phone-payment'  -- обычный
		WHEN 2 THEN 'phone-payment'  -- домашний
		WHEN 3 THEN 'phone-payment'  -- рабочий
		WHEN 4 THEN 'notify-fax'    -- факс
		WHEN 5 THEN 'phone-payment'  -- мобильный
		WHEN 6 THEN 'phone-payment'  -- контактный
		WHEN 1001 THEN 'extra-email'
	END as type_name

FROM core.accounts ac
JOIN core.contact_infos i ON (i.company_id = ac.company_id AND ac.company_id IS NOT NULL) OR (i.person_id = ac.person_id AND ac.person_id IS NOT NULL)

WHERE
	ac.account_number = :account_number
