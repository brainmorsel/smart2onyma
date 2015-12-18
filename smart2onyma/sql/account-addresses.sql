SELECT DISTINCT
	 ac.id
{% if acc_type == 'person' %}
	,CASE addr.address_type
		WHEN 0 THEN 'address-person-reg'
		WHEN 1 THEN 'address-person-live'
	 END AS address_type
{% else %}
	,CASE addr.address_type
		WHEN 0 THEN 'address-company'
		WHEN 1 THEN 'address-company-post'
	 END AS address_type
{% endif %}
	,addr.zip
	,addr.num
	,addr.building
	,addr.block
	,addr.flat
	,addr.entrance
	,addr.floor
	,addr.postbox
	,street.name as street
	,city.name as city
	,state.name as state
	
FROM core.accounts ac
JOIN core.addresses_enddate addr
{% if acc_type == 'person' %}
	ON ac.person_id = addr.person_id AND ac.person_id IS NOT NULL
{% else %}
	ON ac.company_id = addr.company_id AND ac.company_id IS NOT NULL
{% endif %}
		AND addr.end_date IS NULL
		AND addr.address_type < 2

JOIN core.streets street ON addr.street_id = street.id
JOIN core.cities city ON street.city_id = city.id
JOIN core.states state ON city.state_id = state.id

WHERE
	ac.parent_id IS NULL
	AND ac.account_number = :account_number
