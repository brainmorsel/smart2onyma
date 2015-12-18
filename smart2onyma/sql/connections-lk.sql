SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,wu.id as conn_id
	,wu.login
	,wu.password
	,wu.name as description

FROM core.accounts ac
JOIN core.web_users wu ON (wu.person_id = ac.person_id AND ac.person_id IS NOT NULL) OR (wu.company_id = ac.company_id AND ac.company_id IS NOT NULL)

WHERE
	wu.status <= 2
	AND ac.account_number = :account_number
