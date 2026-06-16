"""
Preprocesamiento de datos Yellow Taxi NYC - Temporada de Diciembre
Extrae las semanas 2-3 de diciembre para los años 2023, 2024 y 2025
desde archivos Parquet almacenados en Google Cloud Storage.

Dependencias: pip install pandas pyarrow gcsfs fsspec
"""

import pandas as pd
import gc

BUCKET = "gs://datos_batch_2023_2025"

# (nombre-archivo, dia_inicio, dia_fin)
ARCHIVOS = [
    ("2023-12", 11, 24),
    ("2024-12",  9, 23),
    ("2025-12",  8, 21),
]

COLUMNAS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


def procesar_año(nombre: str, dia_inicio: int, dia_fin: int) -> pd.DataFrame:
    ruta = f"{BUCKET}/yellow_tripdata_{nombre}.parquet"
    print(f"--- Leyendo {nombre} ---")
    df = pd.read_parquet(ruta, columns=COLUMNAS)
    df = df[
        (df["tpep_pickup_datetime"].dt.month == 12)
        & (df["tpep_pickup_datetime"].dt.day.between(dia_inicio, dia_fin))
    ]
    print(f"Filtrado {nombre}: {len(df):,} filas.")
    return df


def main():
    dataframes = []
    for nombre, inicio, fin in ARCHIVOS:
        df_temp = procesar_año(nombre, inicio, fin)
        dataframes.append(df_temp)
        del df_temp
        gc.collect()

    print("\nUniendo todo y exportando a CSV...")
    df_final = pd.concat(dataframes, ignore_index=True)

    ruta_salida = f"{BUCKET}/procesado/diciembre_final.csv"
    df_final.to_csv(ruta_salida, index=False)
    print(f"\nArchivo CSV creado en: {ruta_salida}")
    print(f"Total de filas: {len(df_final):,}")


if __name__ == "__main__":
    main()
