(status.status IN (1, 3)
  OR (status.status IN (4, 5) AND status.start_date > (CURRENT_DATE - 90)))
