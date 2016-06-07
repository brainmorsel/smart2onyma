SELECT 
       dh.discount_id,
       dh.start_date,
       dh.end_date,
       dh.description
FROM core.discount_history dh, core.users u
WHERE (end_date IS NULL OR end_date > now())
AND u.id = dh.user_id
AND u.account_id = :conn_id
