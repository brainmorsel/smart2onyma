SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,child.id as conn_id
	,st.id
	,st.name
	,spr.price
	,spr.count_price
	,sist.start_date as status_date
	,case sist.status
		when 1 then 'suspended'  -- выкл
		when 2 then 'active'  -- вкл
	 end as status
	,sist.amount

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

JOIN core.users u ON child.id = u.account_id

JOIN core.tariff_history_enddate th ON (
    status.account_id = th.account_id AND th.start_date <= CURRENT_DATE
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
JOIN core.tariffs t ON t.id = th.tariff_id

JOIN core.service_items si ON si.account_id = child.id
JOIN core.service_types st ON si.type_id = st.id AND st.type = 3
JOIN core.service_pricelists spl ON spl.tariff_id = t.id AND spl.end_date IS NULL
JOIN core.service_prices spr ON spr.pricelist_id = spl.id AND spr.type_id = si.type_id

JOIN core.service_item_statuses sist ON sist.service_item_id = si.id AND sist.end_date IS NULL AND sist.status = 2


WHERE
(status.status IN (1, 3)
  OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))

	AND u.service_type = 3
