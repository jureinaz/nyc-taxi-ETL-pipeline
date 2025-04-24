# nyc-taxi-ETL-pipeline

## Descripción General

Este proyecto Consiste en un pipeline ETL para el análisis de tráfico de Taxis en NYC.


## Arquitectura de la Solución

La solución propuesta se basa en una arquitectura de pipeline de datos que consta de cuatro capas principales:

1. **Extracción**: Lectura y extracción de datos desde las fuentes permitidas.
2. **Transformación**: Limpieza, transformación y validación de los datos utilizando Apache Spark.
3. **Carga**: Escritura de los datos procesados en formato Parquet para almacenamiento eficiente.
4. **Generación JSON**: Generación de archivo JSON para ser consumido

![Arquitectura de la Solución](Diagrama%20Arquitectónico%20ETL.png)

---

## Descripción del Pipeline

El pipeline de datos implementa un proceso ETL completo:

- **Extracción**:
  - Lectura de los viajes realizados por taxis en NYC.

- **Transformación**:
  - Limpieza de datos inconsistentes o erróneos.
  - Normalización de columnas y tipos de datos.
  - Aplicación de reglas de negocio para validar y corregir información.

- **Carga**:
  - Escritura de los datos transformados en archivos Parquet.
  - Almacenamiento local o en AWS S3 (dependiendo del entorno de ejecución).
  - Inclusión de metadatos como el nombre del archivo fuente, nombre de la tabla y timestamp de procesamiento.

---

## Implementación Técnica

### Lectura de Datos (Integración)

- **Lectura de Archivo fuente**:
  - Se utiliza la url que contiene el archivo fuente en formato .parquet.
  - https://d37ci6vzurychx.cloudfront.net/trip-data/

### Procesamiento de Datos (Transformación)

- **Limpieza y Transformación**:
  - Se aplican las siguientes transformaciones:
    - Renombrado de columnas para estandarización.
    - Conversión de tipos de datos.
    - Limpieza de valores nulos o inconsistentes.
    - Validación y corrección de correos electrónicos y nombres.
    - Formateo de fechas y eliminación de valores mal formateados.
  - Se incluyen todas las transformaciones solicitadas acorde a la prueba.

### Almacenamiento de Datos

- **Formato de Almacenamiento**: Parquet
  - Elegido por su eficiencia en la compresión y lectura de datos.
- **Ubicación de Almacenamiento**:
  - **Local**: Los archivos Parquet se almacenan en `data/` para pruebas locales y unitarias.
  - **AWS S3 (Opcional)**: Para despliegue en producción, los archivos pueden almacenarse en un bucket de S3.

### Consumo de Datos

- Los datos almacenados en formato Parquet pueden ser consumidos por diversas herramientas de análisis.
- **Integración con AWS**:
  - Servicios como AWS Athena o Redshift Spectrum pueden utilizarse para consultar los datos almacenados en S3.

---
## Instrucciones para Ejecutar el Proyecto

### Requisitos Previos

- **Python 3.11+**
- **Apache Spark 3.x**
- **Hadoop 3.2.2**
- **Java 8 o 11**

### Instalación de Dependencias

1. Crear un entorno virtual:
   ```
   python -m venv venv
   ```
2. Activar el entorno virtual:
   - En Windows:
     ```
     venv\Scripts\activate
     ```
   - En Unix o MacOS:
     ```
     source venv/bin/activate
     ```
3. Instalar las dependencias:
   ```
   pip install -r requirements.txt
   ```

### Ejecución del Pipeline

1. Ejecutar el script principal:
   ```
   python src/main.py
   ```
2. Los archivos Parquet procesados se guardarán en `data/refined/`.

## Despliegue en AWS (Opcional)

Para desplegar el pipeline en AWS y almacenar los datos procesados en S3:

1. Configurar las credenciales de AWS utilizando IAM roles o variables de entorno.
2. Modificar el archivo `main.py` para incluir la configuración de AWS S3 en `SparkSession`:
   ```python
   spark = SparkSession.builder \
       .appName("DataPipelineApp") \
       .config("spark.hadoop.fs.s3a.access.key", "<YOUR_AWS_ACCESS_KEY>") \
       .config("spark.hadoop.fs.s3a.secret.key", "<YOUR_AWS_SECRET_KEY>") \
       .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
       .getOrCreate()
   ```
3. Cambiar las rutas de salida para que apunten al bucket de S3 deseado:
   ```python
   output_path = f"s3a://your-bucket-name/processed_data/{sheet_name}"
   ```
4. Ejecutar el script en un entorno compatible, como una instancia EC2 o EMR.

Este paso no se hizo por falta de capa gratuita con S3.

---

## Conclusiones

Este proyecto cumple con los requisitos establecidos en la prueba técnica:

1. **Desarrollo de Diagramas de Arquitectura**: Se proporcionó un diagrama que muestra la arquitectura del pipeline de datos.

2. **Implementación de ETL con Python y Apache Spark**: Se desarrolló un pipeline ETL utilizando Python y Apache Spark para procesar los datos.

3. **Capas del Pipeline**: El pipeline está compuesto por las capas de integración, almacenamiento, procesamiento y consumo.

4. **Despliegue en AWS (Opcional)**: No se hizo un despliegue pero quedaron listos los archivos parquet que serían funcionales en un gestor como S3.

5. **Verificación y Limpieza de Datos**: Se incluyó un proceso de validación y limpieza de datos para asegurar su consistencia y calidad.

6.**Principios SOLID**: Las subrutinas implementadas dentro de cada script están diseñadas siguiendo los principios SOLID 

---

## Contacto

github.com/jureinaz

---

¡Muchas Gracias y Bienvenido al proyecto!
