from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType

from config import RUTA_RAW, RUTA_STAGING, ANIO_MINIMO

spark = SparkSession.builder \
    .appName("StreamingDataLake_Staging") \
    .getOrCreate()

# Las rutas se toman de config.py (por defecto hdfs:///datalake/...)

# ---------------------------------------------------------
# 1. LIMPIEZA DE TITLE.BASICS (IMDb)
# ---------------------------------------------------------
df_basics_raw = spark.read.csv(
    f"{RUTA_RAW}/imdb/title.basics.tsv.gz",
    sep="\t", header=True, nullValue="\\N"
)

df_peliculas_limpias = df_basics_raw \
    .filter(F.col("titleType") == "movie") \
    .filter(F.col("isAdult") == "0") \
    .select(
        F.col("tconst").alias("id_pelicula"),
        F.col("primaryTitle").alias("titulo_principal"),
        F.col("startYear").cast(IntegerType()).alias("anio_estreno"),
        F.col("runtimeMinutes").cast(IntegerType()).alias("duracion_minutos"),
        F.col("genres").alias("generos_raw")
    ) \
    .filter(F.col("anio_estreno").isNotNull() & (F.col("anio_estreno") >= ANIO_MINIMO))

df_peliculas_limpias.write.mode("overwrite").parquet(f"{RUTA_STAGING}/peliculas_limpias")

# ---------------------------------------------------------
# 2. LIMPIEZA DE TITLE.RATINGS (IMDb)
# ---------------------------------------------------------
df_ratings_raw = spark.read.csv(
    f"{RUTA_RAW}/imdb/title.ratings.tsv.gz",
    sep="\t", header=True, nullValue="\\N"
)

df_calificaciones_limpias = df_ratings_raw \
    .select(
        F.col("tconst").alias("id_pelicula"),
        F.col("averageRating").cast(DoubleType()).alias("calificacion_promedio"),
        F.col("numVotes").cast(IntegerType()).alias("cantidad_votos")
    ) \
    .filter(F.col("calificacion_promedio").isNotNull() & (F.col("cantidad_votos") > 0))

df_calificaciones_limpias.write.mode("overwrite").parquet(f"{RUTA_STAGING}/calificaciones_limpias")

# ---------------------------------------------------------
# 3. LIMPIEZA DE DIRECTORES (TITLE.CREW + NAME.BASICS)
# ---------------------------------------------------------
df_crew_raw = spark.read.csv(
    f"{RUTA_RAW}/imdb/title.crew.tsv.gz",
    sep="\t", header=True, nullValue="\\N"
)

df_names_raw = spark.read.csv(
    f"{RUTA_RAW}/imdb/name.basics.tsv.gz",
    sep="\t", header=True, nullValue="\\N"
)

# Explotamos la lista de directores (ej. "nm0001,nm0002" -> una fila por director)
df_crew_explotado = df_crew_raw \
    .filter(F.col("directors").isNotNull()) \
    .select(
        F.col("tconst").alias("id_pelicula"),
        F.explode(F.split(F.col("directors"), ",")).alias("id_director")
    )

df_nombres_directores = df_names_raw.select(
    F.col("nconst").alias("id_director"),
    F.col("primaryName").alias("nombre_director")
)

df_directores_limpios = df_crew_explotado.join(
    df_nombres_directores, on="id_director", how="inner"
)

df_directores_limpios.write.mode("overwrite").parquet(f"{RUTA_STAGING}/directores_limpios")

# ---------------------------------------------------------
# 4. LIMPIEZA DE TMDB FINANCIERO
# ---------------------------------------------------------
# Nota: Usamos multiLine=True y escape='"' porque los resúmenes/títulos de TMDB suelen tener comas y saltos de línea
df_tmdb_raw = spark.read.csv(
    f"{RUTA_RAW}/tmdb/TMDB_all_movies.csv",
    header=True, multiLine=True, escape='"'
)

df_tmdb_limpio = df_tmdb_raw \
    .filter(F.col("imdb_id").isNotNull() & F.col("imdb_id").startswith("tt")) \
    .select(
        F.trim(F.col("imdb_id")).alias("id_pelicula"),
        # Conversión segura: TMDB guarda los montos como texto decimal
        # ("15000000.0" o incluso "1.5E7"). Pasar primero por DOUBLE evita que
        # un cast directo a BIGINT devuelva NULL; los valores inválidos quedan NULL.
        F.col("budget").cast(DoubleType()).cast(LongType()).alias("presupuesto_usd"),
        F.col("revenue").cast(DoubleType()).cast(LongType()).alias("ingresos_usd"),
        F.col("original_language").alias("idioma_original"),
        F.col("production_countries").alias("pais_produccion")
    ) \
    .dropDuplicates(["id_pelicula"])

df_tmdb_limpio.write.mode("overwrite").parquet(f"{RUTA_STAGING}/tmdb_financiero_limpio")

print("Procesamiento a zona STAGING completado con éxito.")
spark.stop()
