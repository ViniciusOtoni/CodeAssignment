SELECT DAYOFWEEK(timestamp_normalized) as day_of_week, COUNT(*) as error_count --pegando a quantidade total de dados pelo dia da semana
FROM silver.access_logs
WHERE response_category = 'Client Error' -- condição para pegar apenas os status code entre 400 e 500
GROUP BY day_of_week
ORDER BY error_count DESC; 