SELECT client_ip, COUNT(*) as access_count -- top 10 client_ip que realizaram mais acesso
FROM silver.access_logs
GROUP BY client_ip
ORDER BY access_count DESC
LIMIT 10;