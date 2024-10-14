SELECT 
    SUM(response_size) as total_volume, -- Volume total de dados em bytes
    MAX(response_size) as max_volume, -- O maior volume em uma única requisição
    MIN(CASE WHEN response_size >= 0 THEN response_size END) as min_volume, -- O menor volume
    AVG(response_size) as avg_volume -- O volume médio de dados retornado
FROM silver.access_logs;
