-- Transformación SQL aplicada en Google Cloud Dataflow
-- Se ejecuta sobre PCOLLECTION (stream del CSV procesado).
-- COALESCE maneja nulos: RatecodeID sin valor → 99, congestion_surcharge → 0.

SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    PULocationID,
    DOLocationID,

    store_and_fwd_flag,
    COALESCE(RatecodeID, 99)          AS RatecodeID_limpio,

    fare_amount,
    COALESCE(congestion_surcharge, 0) AS congestion_surcharge_limpio,
    tolls_amount,
    total_amount

FROM PCOLLECTION;
