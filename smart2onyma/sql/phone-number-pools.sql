SELECT p.start_ani, p.end_ani, p.comments, ex.zone_code
FROM phone.number_pools p
JOIN phone.exchanges ex ON p.exchange_id = ex.id
WHERE
	end_date IS NULL
ORDER BY p.start_ani
