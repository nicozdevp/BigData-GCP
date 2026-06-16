-- Análisis: Viajes Top 10 Premium (Mayor Gasto General)
-- Objetivo: Identificar zonas de origen con mayor costo total promedio,
--           localizando puntos de partida de clientes con mayor presupuesto.

SELECT
    PULocationID                  AS Zona_Origen,
    ROUND(AVG(total_amount), 2)   AS costo_promedio,
    ROUND(MAX(total_amount), 2)   AS costo_maximo,
    COUNT(*)                      AS cantidad_viajes
FROM
    `<PROJECT_ID>.datos_historicos.diciembre_yellow_taxi`
WHERE
    total_amount > 0
GROUP BY
    Zona_Origen
HAVING
    cantidad_viajes > 50
ORDER BY
    costo_promedio DESC
LIMIT 10;
