SELECT
     pol.id
    ,pol.name
    ,case pi.attribute_id
        when 1 then 'Cisco-AVPair'
        when 10 then 'Cisco-Account-Info'
        when 13 then 'Cisco-Service-Info'
    end as attribute
    ,pi.value
FROM iptraf.policy pol
JOIN iptraf.policy_items pi ON pol.id = pi.policy_id
WHERE pol.type = 2 AND pol.status = 1
ORDER BY pol.name
