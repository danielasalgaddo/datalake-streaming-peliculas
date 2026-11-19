"""
04_kpis.py — Cálculo de los 5 KPIs de negocio sobre la zona 03_curated.

Cada KPI se guarda:
  * en 03_curated/kpis/<nombre> (Parquet) para consumo desde Hive / Power BI;
  * en ./resultados/<nombre>.csv (archivo chico) para las tablas del informe.

KPI 1  ROI promedio (y mediana) por género
KPI 2  Joyas ocultas (alta calidad, bajo presupuesto)
KPI 3  Score ponderado de consistencia de directores
KPI 4  Saturación de oferta vs. demanda por década
KPI 5  Índice de expansión internacional (idiomas distintos del inglés)
Extra  Cobertura de datos financieros (indicador de calidad del dato)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (RUTA_CURATED, RUTA_KPIS, SALIDA_LOCAL, MIN_PELICULAS_GENERO,
                    MIN_PELICULAS_DIRECTOR, IDIOMA_EXCLUIDO, TOP_N)

spark = SparkSession.builder.appName("StreamingDataLake_KPIs").getOrCreate()
os.makedirs(SALIDA_LOCAL, exist_ok=True)

hechos = spark.read.parquet(f"{RUTA_CURATED}/tabla_hechos_peliculas")
generos = spark.read.parquet(f"{RUTA_CURATED}/dimension_generos")
directores = spark.read.parquet(f"{RUTA_CURATED}/dimension_directores")

NOMBRES_IDIOMA = {
    "hi": "Hindi", "fr": "Francés", "ja": "Japonés", "es": "Español",
    "ko": "Coreano", "de": "Alemán", "it": "Italiano", "zh": "Chino",
    "pt": "Portugués", "ru": "Ruso", "ta": "Tamil", "te": "Telugu",
    "ml": "Malayalam", "tr": "Turco", "sv": "Sueco", "da": "Danés",
    "cn": "Cantonés", "fa": "Persa", "bn": "Bengalí", "ar": "Árabe",
}


def guardar(df, nombre, n_csv=None):
    """Persiste el KPI en el lake y exporta un CSV local para el informe."""
    df.write.mode("overwrite").parquet(f"{RUTA_KPIS}/{nombre}")
    local = df.limit(n_csv) if n_csv else df
    local.toPandas().to_csv(f"{SALIDA_LOCAL}/{nombre}.csv", index=False)
    print(f"\n=== {nombre} ===")
    df.show(n_csv or 20, truncate=False)


# ---------------------------------------------------------
# KPI 1: ROI por género
# Se agrega la MEDIANA y la tasa de éxito (ROI > 0) porque el promedio del ROI
# es muy sensible a outliers (una película de USD 15k que recauda USD 100M
# aporta un ROI de ~660.000%).
# ---------------------------------------------------------
kpi1 = (generos.join(hechos.filter(F.col("roi_porcentaje").isNotNull()), "id_pelicula")
        .groupBy("genero")
        .agg(F.count("*").alias("peliculas_analizadas"),
             F.round(F.avg("roi_porcentaje"), 2).alias("roi_promedio_pct"),
             F.round(F.percentile_approx("roi_porcentaje", 0.5), 2).alias("roi_mediana_pct"),
             F.round(F.avg((F.col("roi_porcentaje") > 0).cast("int")) * 100, 1)
              .alias("tasa_exito_pct"),
             F.round(F.avg("presupuesto_usd")).cast("long").alias("presupuesto_promedio_usd"))
        .filter(F.col("peliculas_analizadas") >= MIN_PELICULAS_GENERO)
        .orderBy(F.desc("roi_promedio_pct")))
guardar(kpi1, "kpi1_roi_por_genero")

# ---------------------------------------------------------
# KPI 2: Joyas ocultas
# ---------------------------------------------------------
kpi2 = (hechos.filter("es_joya_oculta")
        .select("id_pelicula", "titulo_principal", "anio_estreno",
                "calificacion_promedio", "cantidad_votos", "presupuesto_usd",
                "idioma_original")
        .orderBy(F.desc("calificacion_promedio"), F.desc("cantidad_votos")))
guardar(kpi2, "kpi2_joyas_ocultas", TOP_N)

# ---------------------------------------------------------
# KPI 3: Score = RatingPromedio x log10(VotosTotales), directores con >= 3 películas
# ---------------------------------------------------------
kpi3 = (directores.join(hechos, "id_pelicula")
        .groupBy("id_director", "nombre_director")
        .agg(F.count("*").alias("peliculas"),
             F.round(F.avg("calificacion_promedio"), 2).alias("rating_promedio"),
             F.sum("cantidad_votos").alias("votos_totales"))
        .filter(F.col("peliculas") >= MIN_PELICULAS_DIRECTOR)
        .withColumn("score_ponderado",
                    F.round(F.col("rating_promedio") * F.log10("votos_totales"), 2))
        .orderBy(F.desc("score_ponderado")))
guardar(kpi3, "kpi3_score_directores", TOP_N)

# ---------------------------------------------------------
# KPI 4: Oferta vs. demanda por década
# anios_con_datos permite advertir que la década actual está incompleta.
# ---------------------------------------------------------
kpi4 = (hechos.groupBy("decada")
        .agg(F.count("*").alias("cantidad_estrenos"),
             F.round(F.avg("cantidad_votos")).cast("long").alias("votos_promedio_por_pelicula"),
             F.round(F.avg("calificacion_promedio"), 2).alias("rating_promedio"),
             F.countDistinct("anio_estreno").alias("anios_con_datos"))
        .withColumn("estrenos_por_anio",
                    F.round(F.col("cantidad_estrenos") / F.col("anios_con_datos")).cast("long"))
        .orderBy("decada"))
guardar(kpi4, "kpi4_oferta_demanda_decada")

# ---------------------------------------------------------
# KPI 5: Expansión internacional (idioma original distinto del inglés)
# ---------------------------------------------------------
mapa_idiomas = F.create_map(*[F.lit(x) for kv in NOMBRES_IDIOMA.items() for x in kv])
kpi5 = (hechos.filter(F.col("idioma_original").isNotNull()
                      & (F.col("idioma_original") != IDIOMA_EXCLUIDO))
        .groupBy("idioma_original")
        .agg(F.count("*").alias("peliculas"),
             F.sum("cantidad_votos").alias("votos_totales"),
             F.round(F.avg("calificacion_promedio"), 2).alias("rating_promedio"))
        .withColumn("idioma", F.coalesce(mapa_idiomas[F.col("idioma_original")],
                                         F.col("idioma_original")))
        .withColumn("votos_por_pelicula",
                    F.round(F.col("votos_totales") / F.col("peliculas")).cast("long"))
        .select("idioma_original", "idioma", "peliculas", "votos_totales",
                "votos_por_pelicula", "rating_promedio")
        .orderBy(F.desc("votos_totales")))
guardar(kpi5, "kpi5_expansion_internacional", TOP_N)

# ---------------------------------------------------------
# Extra: cobertura de datos (cuánto del catálogo tiene datos de TMDB)
# ---------------------------------------------------------
cobertura = hechos.agg(
    F.count("*").alias("peliculas_catalogo"),
    F.round(F.avg(F.col("idioma_original").isNotNull().cast("int")) * 100, 1)
     .alias("pct_con_match_tmdb"),
    F.round(F.avg(F.coalesce((F.col("presupuesto_usd") > 0).cast("int"), F.lit(0))) * 100, 1)
     .alias("pct_con_presupuesto"),
    F.round(F.avg(F.col("roi_porcentaje").isNotNull().cast("int")) * 100, 1)
     .alias("pct_con_roi_calculable"),
    F.sum(F.col("es_joya_oculta").cast("int")).alias("joyas_ocultas_detectadas"))
guardar(cobertura, "extra_cobertura_datos")

print(f"\nKPIs calculados. CSV exportados en {os.path.abspath(SALIDA_LOCAL)}")
spark.stop()
