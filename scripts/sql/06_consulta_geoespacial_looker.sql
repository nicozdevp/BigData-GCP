-- Consulta geoespacial para Looker Studio (mapa coroplético)
-- Combina los viajes con la tabla de zonas geográficas (taxi_zone_oficial)
-- que contiene polígonos en formato WKT convertidos a geometría de BigQuery.
-- Se usa custom query en Looker para evitar errores con datos geoespaciales.

SELECT
    viajes.*,
    ST_GEOGFROMTEXT(geo_destino.WKT)  AS poligonos,
    geo_destino.zone                  AS nombre_destino,
    geo_origen.zone                   AS nombre_origen
FROM
    `<PROJECT_ID>.datos_historicos.diciembre_yellow_taxi` AS viajes
LEFT JOIN
    `<PROJECT_ID>.datos_historicos.taxi_zone_oficial` AS geo_destino
    ON CAST(viajes.DOLocationID AS STRING) = CAST(geo_destino.LocationID AS STRING)
LEFT JOIN
    `<PROJECT_ID>.datos_historicos.taxi_zone_oficial` AS geo_origen
    ON CAST(viajes.PULocationID AS STRING) = CAST(geo_origen.LocationID AS STRING);
