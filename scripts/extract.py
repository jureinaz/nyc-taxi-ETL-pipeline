# scripts/extract.py

import os
import urllib.request
from pyspark.sql import SparkSession
import logging

# === Configuración de logs ===
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# === Inicializar SparkSession ===
spark = SparkSession.builder \
    .appName("NYC Taxi Data - Raw Layer") \
    .getOrCreate()

# === Variables ===
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
RAW_DIR = os.path.join("data", "raw")
LOCAL_FILE = os.path.join(RAW_DIR, "yellow_tripdata_2023-01.parquet")

def download_file(url, dest):
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        logging.info(f"Descargando archivo desde {url}")
        urllib.request.urlretrieve(url, dest)
        logging.info(f"Archivo guardado en {dest}")
    except Exception as e:
        logging.error(f"Error al descargar el archivo: {e}")
        raise

def load_parquet_to_spark(file_path):
    try:
        logging.info(f"Leyendo archivo con PySpark desde: {file_path}")
        df = spark.read.parquet(file_path)
        logging.info(f"Número de registros leídos: {df.count()}")
        df.printSchema()
        return df
    except Exception as e:
        logging.error(f"Error al leer archivo con Spark: {e}")
        raise

def main():
    try:
        download_file(URL, LOCAL_FILE)
        df = load_parquet_to_spark(LOCAL_FILE)
        # Puedes guardar una versión reescrita si deseas:
        # # df.write.mode("overwrite").parquet(os.path.join(RAW_DIR, "spark_output"))
    except Exception as e:
        logging.error(f"Error durante la extracción: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logging.info("SparkSession finalizada correctamente.")

    logging.info("Extracción completada exitosamente.")

if __name__ == "__main__":
    main()