from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, sum as spark_sum,
    count, desc
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ==========================================================
# 1. CREAR SESION DE SPARK
# ==========================================================
spark = SparkSession.builder \
    .appName("Tarea3_Homicidios_Transito") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n============================================")
print("TAREA 3 - ANALISIS BATCH CON APACHE SPARK")
print("============================================")

# ==========================================================
# 2. CARGAR DATASET DESDE HDFS
# ==========================================================
ruta = "hdfs://localhost:9000/Tarea3/homicidios_transito.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(ruta)

print("\n1. ESQUEMA ORIGINAL")
df.printSchema()

print("\n2. PRIMEROS REGISTROS")
df.show(10, truncate=False)

print("\nCantidad de registros originales:", df.count())

# ==========================================================
# 3. LIMPIEZA Y TRANSFORMACION
# ==========================================================

df_limpio = df \
    .withColumnRenamed("FECHA HECHO", "FECHA_HECHO") \
    .withColumnRenamed("COD_DEPTO", "COD_DEPTO") \
    .withColumnRenamed("DEPARTAMENTO", "DEPARTAMENTO") \
    .withColumnRenamed("COD_MUNI", "COD_MUNI") \
    .withColumnRenamed("MUNICIPIO", "MUNICIPIO") \
    .withColumnRenamed("CANTIDAD", "CANTIDAD")

# Convertir fecha y cantidad
df_limpio = df_limpio \
    .withColumn("FECHA_HECHO", to_date(col("FECHA_HECHO"), "dd/MM/yyyy")) \
    .withColumn("CANTIDAD", col("CANTIDAD").cast("integer"))

# Eliminar registros inválidos
df_limpio = df_limpio.dropna(
    subset=["FECHA_HECHO", "DEPARTAMENTO", "MUNICIPIO", "CANTIDAD"]
)

df_limpio = df_limpio.filter(col("CANTIDAD") > 0)

# Crear variables derivadas
df_limpio = df_limpio \
    .withColumn("ANIO", year(col("FECHA_HECHO"))) \
    .withColumn("MES", month(col("FECHA_HECHO")))

print("\n3. DATOS DESPUES DE LA LIMPIEZA")
print("Cantidad de registros limpios:", df_limpio.count())

df_limpio.show(10, truncate=False)

# ==========================================================
# 4. ESTADISTICAS GENERALES
# ==========================================================

print("\n============================================")
print("4. ESTADISTICAS GENERALES")
print("============================================")

df_limpio.select("CANTIDAD").describe().show()

total_casos = df_limpio.agg(
    spark_sum("CANTIDAD").alias("TOTAL_CASOS")
)

print("\nTOTAL DE CASOS REGISTRADOS")
total_casos.show()

# ==========================================================
# 5. ANALISIS POR DEPARTAMENTO
# ==========================================================

print("\n============================================")
print("5. TOP 10 DEPARTAMENTOS")
print("============================================")

por_departamento = df_limpio \
    .groupBy("DEPARTAMENTO") \
    .agg(spark_sum("CANTIDAD").alias("TOTAL")) \
    .orderBy(desc("TOTAL"))

por_departamento.show(10, truncate=False)

# ==========================================================
# 6. ANALISIS POR MUNICIPIO
# ==========================================================

print("\n============================================")
print("6. TOP 10 MUNICIPIOS")
print("============================================")

por_municipio = df_limpio \
    .groupBy("DEPARTAMENTO", "MUNICIPIO") \
    .agg(spark_sum("CANTIDAD").alias("TOTAL")) \
    .orderBy(desc("TOTAL"))

por_municipio.show(10, truncate=False)

# ==========================================================
# 7. EVOLUCION POR ANIO
# ==========================================================

print("\n============================================")
print("7. CASOS POR ANIO")
print("============================================")

por_anio = df_limpio \
    .groupBy("ANIO") \
    .agg(spark_sum("CANTIDAD").alias("TOTAL")) \
    .orderBy("ANIO")

por_anio.show(30, truncate=False)

# ==========================================================
# 8. ANALISIS POR MES
# ==========================================================

print("\n============================================")
print("8. CASOS POR MES")
print("============================================")

por_mes = df_limpio \
    .groupBy("MES") \
    .agg(spark_sum("CANTIDAD").alias("TOTAL")) \
    .orderBy("MES")

por_mes.show(12, truncate=False)

# ==========================================================
# 9. GENERAR VISUALIZACIONES
# ==========================================================

print("\n============================================")
print("9. GENERANDO GRAFICAS")
print("============================================")

# Top 10 departamentos
top10 = por_departamento.limit(10).toPandas()

plt.figure(figsize=(12, 6))
plt.bar(top10["DEPARTAMENTO"], top10["TOTAL"])
plt.xticks(rotation=45, ha="right")
plt.xlabel("Departamento")
plt.ylabel("Cantidad de casos")
plt.title("Top 10 departamentos - Homicidios en accidentes de transito")
plt.tight_layout()
plt.savefig("/home/vboxuser/top10_departamentos.png")
plt.close()

# Evolucion anual
anios = por_anio.toPandas()

plt.figure(figsize=(12, 6))
plt.plot(anios["ANIO"], anios["TOTAL"], marker="o")
plt.xlabel("Anio")
plt.ylabel("Cantidad de casos")
plt.title("Evolucion anual de homicidios en accidentes de transito")
plt.grid()
plt.tight_layout()
plt.savefig("/home/vboxuser/evolucion_anual.png")
plt.close()

print("Grafica creada: /home/vboxuser/top10_departamentos.png")
print("Grafica creada: /home/vboxuser/evolucion_anual.png")

print("\n============================================")
print("ANALISIS BATCH FINALIZADO CORRECTAMENTE")
print("============================================")

spark.stop()
