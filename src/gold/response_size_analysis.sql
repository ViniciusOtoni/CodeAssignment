SELECT 
    SUM(response_size) as total_volume, --Volume total de dados em bytes
    MAX(response_size) as max_volume, -- O maior volúme em uma única requisição
    MIN(response_size) as min_volume, -- O menor volúme em uma única requisição
    AVG(response_size) as avg_volume -- O volume médio de dados retornado
FROM silver.access_logs;
