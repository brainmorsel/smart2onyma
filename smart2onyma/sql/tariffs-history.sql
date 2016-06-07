SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,child.id as conn_id
	,th.tariff_id
	,th.start_date
	,t.name as tariff_name

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.users u ON child.id = u.account_id

JOIN core.tariff_history_enddate th ON (
    child.id = th.account_id AND th.start_date >= to_date(:date_from, 'yyyy-mm-dd')
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
JOIN core.tariffs t ON t.id = th.tariff_id

WHERE
	child.id = :conn_id
