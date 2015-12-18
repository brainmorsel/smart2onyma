SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,child.id as conn_id
	,case status.status
		when 1 then 'active'  -- новый
		when 3 then 'active'  -- активный
		when 4 then 'suspended' -- приостановленый
		when 5 then 'suspended' -- заблокированый
	 end as status
	,sst.name as subservice
	,u.name as conn_name
	,u.description
	,u.service_type
	,u.max_concurent_sessions
	,ip_u.login
	,ip_u.password
	,ip_u.start_ip
	,ip_u.end_ip
	,ip_u.router_id
	,ip_u.user_type
	,case ip_u.user_type
		when 8 then 'pppoe'
		when 9 then 'ipoe'
	 end as conn_type
	,th.tariff_id
	,t.name as tariff_name
	,ip_r.name as router

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

JOIN core.users u ON child.id = u.account_id
LEFT JOIN core.service_sub_types sst ON u.user_service_sub_type_id = sst.id

JOIN iptraf.users ip_u ON ip_u.user_id = u.id AND ip_u.end_date IS NULL
JOIN core.tariff_history_enddate th ON (
    status.account_id = th.account_id AND th.start_date <= CURRENT_DATE
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
JOIN core.tariffs t ON t.id = th.tariff_id

LEFT JOIN iptraf.routers ip_r ON ip_u.router_id = ip_r.id

WHERE
(status.status IN (1, 3)
  OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))

	AND u.service_type = 3  -- 3 - internet, 4 - пользователь телефонии, 10 - телевидение
	AND ac.account_number = '142006678'
	--AND ac.account_number = '142000001'
