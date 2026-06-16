-- Análisis: Zonas de Alto Poder Adquisitivo
-- Objetivo: Identificar zonas con clientes de alta liquidez financiera
--           descartando viajes con peajes para aislar tarifa base pura.

SELECT
    PULocationID            AS Zona_Origen,
    ROUND(AVG(fare_amount), 2)   AS tarifa_base_promedio,
    ROUND(AVG(total_amount), 2)  AS gasto_total_promedio,
    COUNT(*)                AS cantidad_viajes
FROM
    `<PROJECT_ID>.datos_historicos.diciembre_yellow_taxi`
WHERE
    tolls_amount = 0
    AND fare_amount > 15
GROUP BY
    Zona_Origen
HAVING
    cantidad_viajes > 100
ORDER BY
    tarifa_base_promedio DESC
LIMIT 10;
