SELECT DISTINCT
	 ac.id
	,person.birth_day
	,person.birth_place
	,person.secret_word
	,p_info.first_name
	,p_info.second_name
	,p_info.last_name
	,p_info.inn
	,p_info.passport_type
	,p_info.passport_series
	,p_info.passport_number
	,p_info.passport_date
	,p_info.passport_issuer
	
FROM core.accounts ac
JOIN core.persons person ON ac.person_id = person.id AND person.status = 1
JOIN core.person_infos_enddate p_info ON ac.person_id = p_info.person_id AND p_info.end_date IS NULL

WHERE
	ac.parent_id IS NULL
	AND ac.account_number = :account_number
