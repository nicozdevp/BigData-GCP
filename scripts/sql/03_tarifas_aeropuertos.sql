-- Análisis: Tarifas a Aeropuertos
-- Objetivo: Identificar zonas con mayor volumen de viajes a aeropuertos JFK/Newark.
-- RatecodeID 2 = JFK, 3 = Newark Airport (tarifa fija)

SELECT
    PULocationID              AS Zona_Origen,
    COUNT(*)                  AS cantidad_viajes_aeropuertos
FROM
    `<PROJECT_ID>.datos_historicos.diciembre_yellow_taxi`
WHERE
    RatecodeID_limpio IN (2.0, 3.0)
GROUP BY
    Zona_Origen
ORDER BY
    cantidad_viajes_aeropuertos DESC
LIMIT 10;
