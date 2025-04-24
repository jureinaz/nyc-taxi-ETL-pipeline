from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, avg, count, sum, when, unix_timestamp
import logging
import os
import json
import time

# Inicializar Spark
spark = SparkSession.builder.appName("NYC Taxi Load - KPIs").getOrCreate()

# Paths
TRUSTED_PATH = "data/trusted/trusted_2023_01.parquet"
REFINED_OUTPUT_PATH = "data/refined/"
REPORT_PATH = "data/reports/kpi_summary_2023_01.json"

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    start_time = time.time()
    try:
        logger.info("Cargando datos Trusted...")
        df = spark.read.parquet(TRUSTED_PATH)

        total_records = df.count()

        # A. Demanda y Tiempos Pico
        logger.info("Calculando KPIs de demanda y tiempos pico...")
        df_time = df.withColumn("hour", hour("pickup_datetime")) \
                    .withColumn("weekday", dayofweek("pickup_datetime"))

        kpi_time = df_time.groupBy("hour", "weekday").agg(
            count("*").alias("total_viajes"),
            avg("trip_distance").alias("distancia_promedio"),
            avg("fare_amount").alias("tarifa_promedio")
        )

        # B. Eficiencia Geográfica y Económica
        logger.info("Calculando KPIs de eficiencia geográfica y económica...")
        df_duration = df.withColumn("trip_duration_min",
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)

        kpi_geo = df_duration.groupBy("Borough", "Zone").agg(
            count("*").alias("total_viajes"),
            avg("trip_distance").alias("distancia_prom"),
            avg("fare_amount").alias("tarifa_prom"),
            avg(col("fare_amount") / col("trip_distance")).alias("ingreso_por_milla"),
            avg(col("fare_amount") / col("trip_duration_min")).alias("ingreso_por_minuto"),
            avg(col("trip_distance") / col("trip_duration_min")).alias("velocidad_prom_milla_x_min")
        )

        # C. Impacto de la Calidad de Datos (reglas de validación básicas)
        logger.info("Calculando impacto de la calidad de los datos...")

        df_quality = df.withColumn("registro_valido", when(
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0) &
            (col("pickup_datetime") < col("dropoff_datetime")),
            1
        ).otherwise(0))

        total_valid = df_quality.filter(col("registro_valido") == 1).count()
        porcentaje_descartados = round(100 * (total_records - total_valid) / total_records, 2)

        # Guardar resultados
        logger.info("Guardando resultados en capa Refined...")
        os.makedirs(REFINED_OUTPUT_PATH, exist_ok=True)
        kpi_time.write.mode("overwrite").parquet(os.path.join(REFINED_OUTPUT_PATH, "kpi_demanda_hora_dia.parquet"))
        kpi_geo.write.mode("overwrite").parquet(os.path.join(REFINED_OUTPUT_PATH, "kpi_eficiencia_geografica.parquet"))

        # Guardar reporte resumen
        logger.info("Guardando reporte resumen de ejecución...")
        os.makedirs("data/reports", exist_ok=True)
        report = {
            "registros_totales": total_records,
            "registros_validos": total_valid,
            "porcentaje_descartados": porcentaje_descartados,
            "duracion_total_seg": round(time.time() - start_time, 2)
        }
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=4)

        logger.info("Proceso Refined completado exitosamente.")

    except Exception as e:
        logger.error(f"Error en el cálculo de KPIs: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()