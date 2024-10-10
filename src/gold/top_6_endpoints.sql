SELECT request_url, COUNT(*) as access_count
FROM silver.access_logs
WHERE request_url NOT REGEXP '\.(jpg|png|css|js|gif|ico)$' -- regex feito para desconsiderar os endpoints que representam arquivos
GROUP BY request_url
ORDER BY access_count DESC
LIMIT 6;