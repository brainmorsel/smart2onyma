SELECT ac.base_company_id, c.name,  count(ac.id) as cnt
FROM core.accounts ac
JOIN core.companies c ON c.id = ac.base_company_id
JOIN core.account_statuses_enddate status ON ac.id = status.account_id AND status.end_date IS NULL AND status.status != 6
GROUP BY ac.base_company_id, c.name
ORDER BY ac.base_company_id
