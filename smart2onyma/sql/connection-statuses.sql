SELECT
   status.start_date
	,case status.status
		when 1 then 'active'  -- новый
		when 3 then 'active'  -- активный
		when 4 then 'suspended' -- приостановленый
		when 5 then 'suspended' -- заблокированый
	 end as status
FROM core.account_statuses status
WHERE status.account_id = :conn_id
ORDER BY status.start_date
