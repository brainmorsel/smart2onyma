SELECT t.id,
       t.name,
       t.service_type,
       t.prepay_fee,
       t.status,
       t.create_date,
       t.modify_date,
       ttl.period,
       ttl.next_tariff_id,
       pr.fee,
       tc.cnt
FROM core.tariffs t
JOIN core.tariff_base_companies tbc ON tbc.tariff_id = t.id
LEFT JOIN core.tariff_time_limits ttl ON (t.id = ttl.tariff_id)

{% if phone_tariffs %}
LEFT JOIN phone.pricelists_enddate pr
{% elif ctv_tariffs %}
LEFT JOIN tv.pricelists_enddate pr
{% else %}
LEFT JOIN iptraf.pricelists_enddate pr
{% endif %}
    ON (t.id = pr.tariff_id AND pr.end_date IS NULL)

JOIN (
  SELECT
    th.tariff_id,
    COUNT(status.account_id) cnt
  FROM core.account_statuses_enddate status
  JOIN core.tariff_history_enddate th ON (
    status.account_id = th.account_id AND th.start_date <= CURRENT_DATE
    AND (th.end_date > CURRENT_DATE OR th.end_date IS NULL)
    )
  WHERE
    status.end_date IS NULL AND
    {% include 'where-recent-90days.sql' %}
  GROUP BY th.tariff_id
) tc ON (tc.tariff_id = t.id)

WHERE
{% if phone_tariffs %}
  t.service_type in (4, 6)
{% elif ctv_tariffs %}
	t.service_type = 10
{% else %}
-- интернет
  t.service_type = 3
{% endif %}

{% if company %}
  AND t.forcompany = 1
{% elif person %}
  AND t.forperson = 1
{% endif %}
{% if 'base_company' in filters %}
  AND tbc.base_company_id = {{filters.base_company.id}}
{% endif %}
