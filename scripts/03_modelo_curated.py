"""
03_modelo_curated.py — Modelo dimensional (esquema estrella) en 03_curated.

Entradas (02_staging):  peliculas_limpias, calificaciones_limpias,
                        directores_limpios, tmdb_financiero_limpio
Salidas  (03_curated):  tabla_hechos_peliculas (particionada por decada),
                        dimension_generos, dimension_directores
Metadatos:              _metadata/registro_ejecuciones (conteos por tabla)

Reglas de negocio (ver config.py e informe, sección 4):
  * INNER JOIN con calificaciones: sólo películas con al menos un voto.
  * LEFT JOIN con TMDB: se conservan películas sin datos financieros.
  * roi_porcentaje sólo si presupuesto > 10.000 USD e ingresos > 0
    (un ingreso 0 en TMDB significa "no informado", no "pérdida total").
  * es_joya_oculta: rating >= 7.5, votos >= 10.000 y presupuesto válido
    dentro del percentil 25, calculado dinámicamente sobre los datos.
"""
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (RUTA_STAGING, RUTA_CURATED, RUTA_METADATA,
                    PRESUPUESTO_MINIMO_ROI, JOYA_RATING_MIN, JOYA_VOTOS_MIN,
                    JOYA_PERCENTIL_PRESUPUESTO)

spark = SparkSession.builder.appName("StreamingDataLake_Curated").getOrCreate()
fecha_proceso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------
# 1. LECTURA DE STAGING
# ---------------------------------------------------------
peliculas = spark.read.parquet(f"{RUTA_STAGING}/peliculas_limpias")
calificaciones = spark.read.parquet(f"{RUTA_STAGING}/calificaciones_limpias")
directores = spark.read.parquet(f"{RUTA_STAGING}/directores_limpios")
tmdb = spark.read.parquet(f"{RUTA_STAGING}/tmdb_financiero_limpio")

# ---------------------------------------------------------
# 2. TABLA DE HECHOS
# ---------------------------------------------------------
base = (peliculas.dropDuplicates(["id_pelicula"])
        .join(calificaciones.dropDuplicates(["id_pelicula"]), "id_pelicula", "inner")
        .join(tmdb, "id_pelicula", "left"))

presupuesto_valido = F.col("presupuesto_usd") > PRESUPUESTO_MINIMO_ROI

base = base.withColumn(
    "roi_porcentaje",
    F.when(presupuesto_valido & (F.col("ingresos_usd") > 0),
           F.round((F.col("ingresos_usd") - F.col("presupuesto_usd"))
                   / F.col("presupuesto_usd") * 100, 2))
)

# Umbral dinámico de "presupuesto bajo" = percentil 25 de presupuestos válidos
umbral_p25 = (base.filter(presupuesto_valido)
              .approxQuantile("presupuesto_usd", [JOYA_PERCENTIL_PRESUPUESTO], 0.001))
umbral_p25 = umbral_p25[0] if umbral_p25 else 0
print(f"Percentil {int(JOYA_PERCENTIL_PRESUPUESTO * 100)} de presupuesto: "
      f"USD {umbral_p25:,.0f}")

hechos = (base
          .withColumn("es_joya_oculta",
                      F.coalesce(
                          (F.col("calificacion_promedio") >= JOYA_RATING_MIN)
                          & (F.col("cantidad_votos") >= JOYA_VOTOS_MIN)
                          & presupuesto_valido
                          & (F.col("presupuesto_usd") <= F.lit(umbral_p25)),
                          F.lit(False)))
          .withColumn("decada", (F.floor(F.col("anio_estreno") / 10) * 10).cast("int"))
          .withColumn("fecha_proceso", F.lit(fecha_proceso))
          .select("id_pelicula", "titulo_principal", "anio_estreno", "decada",
                  "duracion_minutos", "generos_raw", "calificacion_promedio",
                  "cantidad_votos", "presupuesto_usd", "ingresos_usd",
                  "roi_porcentaje", "es_joya_oculta", "idioma_original",
                  "pais_produccion", "fecha_proceso"))

hechos.cache()

# repartition("decada") junta cada década en una sola tarea: se escribe un
# archivo por partición en lugar de uno por cada combinación tarea-década, lo
# que evita cientos de archivos chicos en HDFS (y acelera la escritura).
(hechos.repartition("decada")
 .write.mode("overwrite")
 .partitionBy("decada")
 .parquet(f"{RUTA_CURATED}/tabla_hechos_peliculas"))

ids = hechos.select("id_pelicula")

# ---------------------------------------------------------
# 3. DIMENSIÓN GÉNEROS (tabla puente: 1 fila por película-género)
# ---------------------------------------------------------
dim_generos = (hechos
               .filter(F.col("generos_raw").isNotNull())
               .select("id_pelicula",
                       F.explode(F.split("generos_raw", ",")).alias("genero"))
               .withColumn("genero", F.trim("genero"))
               .dropDuplicates(["id_pelicula", "genero"]))

dim_generos.write.mode("overwrite").parquet(f"{RUTA_CURATED}/dimension_generos")

# ---------------------------------------------------------
# 4. DIMENSIÓN DIRECTORES (tabla puente: 1 fila por película-director)
#    INNER JOIN con hechos = integridad referencial (sin huérfanos)
# ---------------------------------------------------------
dim_directores = (directores
                  .join(ids, "id_pelicula", "inner")
                  .select("id_pelicula", "id_director", "nombre_director")
                  .dropDuplicates(["id_pelicula", "id_director"]))

dim_directores.write.mode("overwrite").parquet(f"{RUTA_CURATED}/dimension_directores")

# ---------------------------------------------------------
# 5. REGISTRO DE EJECUCIÓN (metadatos operativos / linaje)
# ---------------------------------------------------------
# Las dimensiones se cuentan sobre los Parquet ya escritos (lectura de
# metadatos) en lugar de recalcular explode/joins desde staging.
conteos = [
    ("tabla_hechos_peliculas", hechos.count()),
    ("dimension_generos",
     spark.read.parquet(f"{RUTA_CURATED}/dimension_generos").count()),
    ("dimension_directores",
     spark.read.parquet(f"{RUTA_CURATED}/dimension_directores").count()),
]
registro = spark.createDataFrame(
    [(fecha_proceso, "03_modelo_curated.py", t, n, float(umbral_p25)) for t, n in conteos],
    "fecha_proceso string, script string, tabla string, filas long, umbral_p25_presupuesto double")
registro.write.mode("append").parquet(f"{RUTA_METADATA}/registro_ejecuciones")

for tabla, filas in conteos:
    print(f"  {tabla}: {filas:,} filas")
print("Modelo CURATED generado con éxito.")
spark.stop()
