-- Análisis: Hotspot Navideño de Compras y Turismo
-- Objetivo: Localizar zonas de máxima concentración peatonal durante
--           la época crítica de compras (10-24 dic, 12:00-21:00 hrs).

SELECT
    PULocationID                  AS Zona_Origen,
    COUNT(*)                      AS total_viajes,
    ROUND(AVG(total_amount), 2)   AS tarifa_promedio
FROM
    `<PROJECT_ID>.datos_historicos.diciembre_yellow_taxi`
WHERE
    EXTRACT(DAY  FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) BETWEEN 10 AND 24
    AND EXTRACT(HOUR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) BETWEEN 12 AND 21
GROUP BY
    Zona_Origen
ORDER BY
    total_viajes DESC
LIMIT 10;
