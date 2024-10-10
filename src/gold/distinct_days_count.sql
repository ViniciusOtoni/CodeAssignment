SELECT COUNT(DISTINCT TO_DATE(timestamp_normalized)) as distinct_days --quantidade de dias que o arquivo cobre
FROM silver.access_logs;