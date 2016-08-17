SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,ip_u.login

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

JOIN core.users u ON child.id = u.account_id
JOIN iptraf.users ip_u ON ip_u.user_id = u.id AND ip_u.end_date IS NULL

WHERE
(status.status IN (1, 3)
  OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))

	AND u.service_type = 3
	AND ip_u.user_type = 8
