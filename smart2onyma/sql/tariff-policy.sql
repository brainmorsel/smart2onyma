SELECT pi.value
FROM iptraf.pricelists_enddate pr
JOIN iptraf.price_list_policy_links plpl ON pr.id = plpl.price_list_id
JOIN iptraf.policy_items pi ON plpl.policy_id = pi.policy_id AND pi.attribute_id = 1
WHERE pr.end_date IS NULL AND pr.tariff_id = :tariff_id
