-- в процессе
SELECT DISTINCT
	 ac.id
	,co.name as co_name
	,ei.external_id as eisup
	,c_info.law_name
	,c_info.inn
	,c_info.kpp
	,c_info.ogrn
	,c_info.okonh
	,c_info.okpo
	,cpt.short_name as cpt_short
	,cpt.full_name as cpt_full

FROM core.accounts ac

JOIN core.companies co ON ac.company_id = co.id AND co.status = 1
JOIN core.company_law_info_enddate c_info ON ac.company_id = c_info.company_id AND c_info.end_date IS NULL
JOIN core.company_property_types cpt ON cpt.id = c_info.property_type
JOIN eisup.contractors ei ON ei.company_id = co.id

WHERE
	ac.account_number = :account_number
