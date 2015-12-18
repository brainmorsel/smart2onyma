SELECT
	{% if estimate_count %}
	COUNT(*) as count
	{% else %}
	account_number
	{% endif %}
FROM (
	SELECT DISTINCT
		ac.account_number

	FROM core.accounts ac
	JOIN core.accounts child ON child.parent_id = ac.id AND ac.parent_id IS NULL
	JOIN core.account_statuses_enddate status ON child.id = status.account_id AND status.end_date IS NULL

	WHERE
		(status.status IN (1, 3)
			OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))
		{% if 'person' in filters %}
		AND ac.person_id IS NOT NULL
		{% endif %}
		{% if 'company' in filters %}
		AND ac.company_id IS NOT NULL
		{% endif %}
		{% if 'account' in filters %}
		AND ac.account_number = '{{filters.account.number}}'
		{% endif %}
		{% if 'prefix' in filters %}
		AND ac.account_number LIKE '{{filters.prefix.value}}%'
		{% endif %}
) lst
