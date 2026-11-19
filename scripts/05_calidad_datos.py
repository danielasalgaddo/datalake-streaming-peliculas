"""
05_calidad_datos.py — Validaciones automáticas de calidad sobre 03_curated.

Implementa en código las reglas de gobierno del informe (sección 6):
unicidad, integridad referencial, validez de rangos y validez financiera.

Salidas:
  * _metadata/calidad_datos (Parquet, histórico append por ejecución)
  * ./resultados/reporte_calidad.csv
El script termina con código 1 si falla alguna regla CRÍTICA, para que el
orquestador detenga la publicación de datos inconsistentes.
"""
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (RUTA_CURATED, RUTA_METADATA, SALIDA_LOCAL, ANIO_MINIMO,
                    PRESUPUESTO_MINIMO_ROI)

spark = SparkSession.builder.appName("StreamingDataLake_Calidad").getOrCreate()
os.makedirs(SALIDA_LOCAL, exist_ok=True)
fecha = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
anio_actual = datetime.now(timezone.utc).year

hechos = spark.read.parquet(f"{RUTA_CURATED}/tabla_hechos_peliculas")
generos = spark.read.parquet(f"{RUTA_CURATED}/dimension_generos")
directores = spark.read.parquet(f"{RUTA_CURATED}/dimension_directores")

total = hechos.count()
resultados = []


def regla(nombre, dimension, severidad, filas_incumplen, descripcion):
    if filas_incumplen == 0:
        estado = "OK"
    else:
        estado = "FALLA" if severidad == "CRITICA" else "REVISAR"
    resultados.append((fecha, nombre, dimension, severidad, int(filas_incumplen),
                       int(total), estado, descripcion))


# --- Unicidad ---------------------------------------------------------------
regla("pk_unica_hechos", "Unicidad", "CRITICA",
      total - hechos.select("id_pelicula").distinct().count(),
      "id_pelicula no se repite en tabla_hechos_peliculas")
regla("pk_unica_generos", "Unicidad", "CRITICA",
      generos.count() - generos.select("id_pelicula", "genero").distinct().count(),
      "(id_pelicula, genero) no se repite en dimension_generos")
regla("pk_unica_directores", "Unicidad", "CRITICA",
      directores.count() - directores.select("id_pelicula", "id_director").distinct().count(),
      "(id_pelicula, id_director) no se repite en dimension_directores")

# --- Integridad referencial ---------------------------------------------------
ids = hechos.select("id_pelicula")
regla("fk_generos_huerfanos", "Integridad", "CRITICA",
      generos.join(ids, "id_pelicula", "left_anti").count(),
      "Todo id_pelicula de dimension_generos existe en hechos")
regla("fk_directores_huerfanos", "Integridad", "CRITICA",
      directores.join(ids, "id_pelicula", "left_anti").count(),
      "Todo id_pelicula de dimension_directores existe en hechos")

# --- Validez de rangos --------------------------------------------------------
regla("rating_en_rango", "Validez", "CRITICA",
      hechos.filter(~F.col("calificacion_promedio").between(1.0, 10.0)).count(),
      "calificacion_promedio entre 1.0 y 10.0")
regla("votos_positivos", "Validez", "CRITICA",
      hechos.filter(F.col("cantidad_votos") <= 0).count(),
      "cantidad_votos > 0")
regla("anio_en_alcance", "Validez", "CRITICA",
      hechos.filter(F.col("anio_estreno") < ANIO_MINIMO).count(),
      f"anio_estreno >= {ANIO_MINIMO}")
regla("anio_no_futuro", "Validez", "ADVERTENCIA",
      hechos.filter(F.col("anio_estreno") > anio_actual).count(),
      f"anio_estreno <= {anio_actual} (títulos anunciados con votos)")
regla("montos_no_negativos", "Validez", "CRITICA",
      hechos.filter((F.col("presupuesto_usd") < 0) | (F.col("ingresos_usd") < 0)).count(),
      "presupuesto_usd e ingresos_usd >= 0")

# --- Validez financiera / reglas de negocio ----------------------------------
regla("roi_solo_con_presupuesto_valido", "Validez financiera", "CRITICA",
      hechos.filter(F.col("roi_porcentaje").isNotNull()
                    & ~(F.col("presupuesto_usd") > PRESUPUESTO_MINIMO_ROI)).count(),
      f"roi_porcentaje sólo existe si presupuesto_usd > {PRESUPUESTO_MINIMO_ROI}")
regla("roi_extremo", "Validez financiera", "ADVERTENCIA",
      hechos.filter(F.col("roi_porcentaje") > 100_000).count(),
      "Películas con ROI > 100.000% (revisar posibles errores de carga en TMDB)")

# --- Completitud ------------------------------------------------------------------
regla("titulo_completo", "Completitud", "CRITICA",
      hechos.filter(F.col("titulo_principal").isNull()).count(),
      "titulo_principal no nulo")
regla("sin_match_tmdb", "Completitud", "ADVERTENCIA",
      hechos.filter(F.col("idioma_original").isNull()).count(),
      "Películas sin cruce con TMDB (sin idioma ni datos financieros)")

esquema = ("fecha_ejecucion string, regla string, dimension string, severidad string, "
           "filas_incumplen long, filas_evaluadas long, estado string, descripcion string")
reporte = spark.createDataFrame(resultados, esquema)
reporte.write.mode("append").parquet(f"{RUTA_METADATA}/calidad_datos")
reporte.toPandas().to_csv(f"{SALIDA_LOCAL}/reporte_calidad.csv", index=False)
reporte.select("regla", "severidad", "filas_incumplen", "estado").show(50, truncate=False)

criticas = [r for r in resultados if r[3] == "CRITICA" and r[6] == "FALLA"]
spark.stop()
if criticas:
    print(f"{len(criticas)} regla(s) CRÍTICA(s) fallaron: {[r[1] for r in criticas]}")
    sys.exit(1)
print("Todas las reglas críticas de calidad se cumplen.")
