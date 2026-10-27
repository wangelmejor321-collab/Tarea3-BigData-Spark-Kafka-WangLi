# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

Curso: Big Data - UNAD  
Estudiante: Wang Li Chía Salas

## Descripción

Este repositorio contiene el desarrollo práctico de la Tarea 3 del curso Big Data.

El proyecto contempla dos componentes:

1. Procesamiento batch utilizando Apache Spark y PySpark.
2. Procesamiento de datos en tiempo real utilizando Apache Kafka y Spark Structured Streaming.

## Problema seleccionado

Identificación de patrones temporales y geográficos en los homicidios asociados a accidentes de tránsito en Colombia mediante Apache Spark.

Para el procesamiento batch se utilizó un conjunto de datos público de Datos Abiertos Colombia correspondiente a homicidios en accidentes de tránsito.

## Archivos

### tarea3_batch.py

Realiza el procesamiento batch del conjunto de datos utilizando PySpark.

Incluye:

- Lectura del archivo desde HDFS.
- Limpieza y transformación de datos.
- Análisis exploratorio.
- Estadísticas generales.
- Agrupación por departamento y municipio.
- Análisis por año y mes.
- Generación de visualizaciones.

### kafka_producer.py

Genera datos simulados de sensores con:

- Identificador del sensor.
- Temperatura.
- Humedad.
- Marca de tiempo.

Los datos son enviados al topic `sensor_data` de Apache Kafka.

### spark_streaming_consumer.py

Consume los eventos del topic `sensor_data` mediante Spark Structured Streaming y calcula promedios de temperatura y humedad por sensor en ventanas de tiempo.

## Ejecución del procesamiento batch

```bash
spark-submit tarea3_batch.py
```

## Ejecución del productor Kafka

```bash
python3 kafka_producer.py
```

## Ejecución de Spark Streaming

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
spark_streaming_consumer.py
```

## Tecnologías utilizadas

- Ubuntu Server
- Hadoop 3.3.6
- HDFS
- Apache Spark 3.5.8
- PySpark
- Apache Kafka 3.9.2
- Python 3
