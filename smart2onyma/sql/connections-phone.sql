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
	
	,ph_u.real_start_num as phone_number
	,ats.name as ats_name
	,ats.zone_code
	 
	,th.tariff_id
	,t.name as tariff_name

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

JOIN core.users u ON child.id = u.account_id
LEFT JOIN core.service_sub_types sst ON u.user_service_sub_type_id = sst.id

JOIN phone.users ph_u ON ph_u.user_id = u.id AND ph_u.end_date IS NULL
JOIN phone.exchanges ats ON ats.id = ph_u.exchange_id

JOIN core.tariff_history_enddate th ON (
    status.account_id = th.account_id AND th.start_date <= CURRENT_DATE
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
JOIN core.tariffs t ON t.id = th.tariff_id

WHERE
(status.status IN (1, 3)
  OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))

	AND u.service_type = 4  -- 3 - internet, 4 - пользователь телефонии, 10 - телевидение
	--AND ats.name NOT IN ('si2000', 'UUDE_ICS')
	--AND ac.account_number = :account_number
;
