SELECT DISTINCT
	 ac.id
	,ac.balance
	,ac.child_balance
	,ac.account_number
	,ac.notification_email
	,ac.notification_fax
	,ac.notification_sms
	,ac.create_date
	,grp.name AS group_name
	,CASE coalesce(ac.person_id, -1)
		WHEN -1 THEN 'company'
		ELSE 'person'
	 END AS acc_type
	,mn.name as manager
	,CURRENT_DATE as now

FROM core.accounts ac
JOIN core.groups grp ON ac.group_id = grp.id
LEFT JOIN core.managers mn ON ac.manager_id = mn.id

WHERE
	ac.parent_id IS NULL
	AND ac.account_number = :account_number
