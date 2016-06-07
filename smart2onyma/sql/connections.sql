SELECT DISTINCT
	 ac.id as account_id
	,ac.account_number
	,child.id as conn_id
	,case status.status
		when 1 then 'inactive'  -- новый
		when 3 then 'active'  -- активный
		when 4 then 'suspended' -- приостановленый
		when 5 then 'suspended' -- заблокированый
	 end as status
	,sst.name as subservice
	,u.name as conn_name
	,u.description
	,u.service_type
	,u.max_concurent_sessions
{% if c_type == 'internet' %}
	,ip_u.login
	,ip_u.password
	,ip_u.start_ip
	,ip_u.end_ip
	,ip_r.name as router
	,case ip_u.user_type
		when 8 then 'pppoe'
		when 9 then 'ipoe'
		when 1 then 'ipoe'
		when 6 then 'ipoe'
	 end as conn_type
{% elif c_type == 'phone' %}
	,ph_u.real_start_num as phone_number
	,ats.name as ats_name
{% elif c_type == 'ctv' %}
{% elif c_type == 'npl' %}
  ,n_plf1.address as platform1
  ,n_plf2.address as platform2
{% endif %}
	,th.tariff_id
	,t.name as tariff_name
	,pr.fee as tariff_fee

FROM core.accounts ac
JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

JOIN core.users u ON child.id = u.account_id
LEFT JOIN core.service_sub_types sst ON u.user_service_sub_type_id = sst.id

JOIN core.tariff_history_enddate th ON (
    status.account_id = th.account_id AND th.start_date <= CURRENT_DATE
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
JOIN core.tariffs t ON t.id = th.tariff_id

{% if c_type == 'internet' %}
JOIN iptraf.users ip_u ON ip_u.user_id = u.id AND ip_u.end_date IS NULL
LEFT JOIN iptraf.routers ip_r ON ip_u.router_id = ip_r.id
LEFT JOIN iptraf.pricelists_enddate pr ON pr.tariff_id = th.tariff_id AND pr.end_date IS NULL
{% elif c_type == 'phone' %}
JOIN phone.users ph_u ON ph_u.user_id = u.id AND ph_u.end_date IS NULL
JOIN phone.exchanges ats ON ats.id = ph_u.exchange_id
LEFT JOIN phone.pricelists_enddate pr ON pr.tariff_id = th.tariff_id AND pr.end_date IS NULL
{% elif c_type == 'ctv' %}
LEFT JOIN tv.pricelists_enddate pr ON pr.tariff_id = th.tariff_id AND pr.end_date IS NULL
{% elif c_type == 'npl' %}
LEFT JOIN npl.pricelists_enddate pr ON pr.tariff_id = th.tariff_id AND pr.end_date IS NULL
LEFT JOIN npl.users nu ON nu.user_id = u.id
LEFT JOIN npl.platforms n_plf1 ON nu.platform1 = n_plf1.id
LEFT JOIN npl.platforms n_plf2 ON nu.platform2 = n_plf2.id
{% endif %}

WHERE
(status.status IN (1, 3, 4)
  OR (status.status = 5 AND status.start_date > (CURRENT_DATE - 360)))

{% if c_type == 'internet' %}
	AND u.service_type = 3
{% elif c_type == 'phone' %}
	AND u.service_type = 4
	AND t.name != '---БЕЗ ТАРИФА---'  -- игнорируем ОА с МТС
{% elif c_type == 'ctv' %}
	AND u.service_type = 10
{% elif c_type == 'npl' %}
  AND u.service_type = 2
{% endif %}
	AND ac.account_number = :account_number
