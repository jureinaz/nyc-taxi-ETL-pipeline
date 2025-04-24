from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan
import logging
import os

# Inicializar Spark
spark = SparkSession.builder.appName("NYC Taxi Transform").getOrCreate()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
RAW_PATH = "data/raw/yellow_tripdata_2023-01.parquet"
LOOKUP_PATH = "resources/taxi_zone_lookup.csv"
TRUSTED_OUTPUT_PATH = "data/trusted/trusted_2023_01.parquet"

def main():
    try:
        logger.info("Leyendo datos RAW...")
        df = spark.read.parquet(RAW_PATH)

        logger.info(f"Registros originales: {df.count()}")

        # 1. Validaciones y limpieza
        logger.info("Aplicando validaciones de calidad...")

        df_clean = df.filter(
            (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")) &
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0)
        )

        logger.info(f"Registros luego de validaciones: {df_clean.count()}")

        # 2. Estandarización de columnas
        df_clean = df_clean.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

        # 3. Enriquecimiento con Taxi Zone Lookup
        logger.info("Cargando tabla Taxi Zone Lookup...")

        lookup_df = spark.read.option("header", True).csv(LOOKUP_PATH)

        df_enriched = df_clean.join(
            lookup_df.withColumnRenamed("LocationID", "PULocationID"),
            on="PULocationID",
            how="left"
        )

        logger.info(f"Registros luego del enriquecimiento: {df_enriched.count()}")

        # 4. Guardar en capa Trusted
        os.makedirs(os.path.dirname(TRUSTED_OUTPUT_PATH), exist_ok=True)

        df_enriched.write.mode("overwrite").parquet(TRUSTED_OUTPUT_PATH)
        logger.info("Transformación completa. Datos guardados en capa Trusted.")

    except Exception as e:
        logger.error(f"Error durante la transformación: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logger.info("SparkSession finalizada correctamente.")

if __name__ == "__main__":
    main()