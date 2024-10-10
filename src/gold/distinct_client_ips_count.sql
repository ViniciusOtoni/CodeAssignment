SELECT COUNT(DISTINCT client_ip) as distinct_client_ips --quantidade de IPs distintos 
FROM silver.access_logs;