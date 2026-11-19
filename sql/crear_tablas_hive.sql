-- =====================================================================
-- Tablas externas de Hive sobre la zona 03_curated (esquema estrella).
-- Son EXTERNAL: borrar la tabla en Hive no borra los Parquet del lake.
-- Ejecutar con:  hive -f sql/crear_tablas_hive.sql   (o beeline -f ...)
-- =====================================================================
CREATE DATABASE IF NOT EXISTS streaming_curated
COMMENT 'Zona 03_curated del Data Lake de adquisiciones de películas';

USE streaming_curated;

DROP TABLE IF EXISTS tabla_hechos_peliculas;
CREATE EXTERNAL TABLE tabla_hechos_peliculas (
    id_pelicula            STRING   COMMENT 'PK. tconst de IMDb',
    titulo_principal       STRING   COMMENT 'Título comercial principal',
    anio_estreno           INT      COMMENT 'Año de estreno (>= 1980)',
    duracion_minutos       INT      COMMENT 'Duración en minutos (IMDb)',
    generos_raw            STRING   COMMENT 'Lista original de géneros separada por comas',
    calificacion_promedio  DOUBLE   COMMENT 'Rating IMDb 1.0-10.0',
    cantidad_votos         INT      COMMENT 'Cantidad de votos IMDb',
    presupuesto_usd        BIGINT   COMMENT 'Presupuesto (TMDB)',
    ingresos_usd           BIGINT   COMMENT 'Recaudación mundial (TMDB)',
    roi_porcentaje         DOUBLE   COMMENT 'ROI % si presupuesto > 10k e ingresos > 0',
    es_joya_oculta         BOOLEAN  COMMENT 'Rating >= 7.5, votos >= 10k, presupuesto <= P25',
    idioma_original        STRING   COMMENT 'Código ISO 639-1 (TMDB)',
    pais_produccion        STRING   COMMENT 'Países de producción (TMDB)',
    fecha_proceso          STRING   COMMENT 'Fecha UTC de generación de la fila'
)
PARTITIONED BY (decada INT)
STORED AS PARQUET
LOCATION '/datalake/03_curated/tabla_hechos_peliculas';

-- Registra las particiones decada=1980, decada=1990, ... creadas por Spark
MSCK REPAIR TABLE tabla_hechos_peliculas;

DROP TABLE IF EXISTS dimension_generos;
CREATE EXTERNAL TABLE dimension_generos (
    id_pelicula  STRING COMMENT 'FK a tabla_hechos_peliculas',
    genero       STRING COMMENT 'Género individual (1 fila por género)'
)
STORED AS PARQUET
LOCATION '/datalake/03_curated/dimension_generos';

DROP TABLE IF EXISTS dimension_directores;
CREATE EXTERNAL TABLE dimension_directores (
    id_pelicula      STRING COMMENT 'FK a tabla_hechos_peliculas',
    id_director      STRING COMMENT 'nconst de IMDb',
    nombre_director  STRING COMMENT 'primaryName de IMDb'
)
STORED AS PARQUET
LOCATION '/datalake/03_curated/dimension_directores';

-- ---------------------------------------------------------------------
-- Consultas de ejemplo (validan que el modelo responde a los KPIs)
-- ---------------------------------------------------------------------
-- KPI 1: ROI promedio por género
-- SELECT g.genero, COUNT(*) AS peliculas, ROUND(AVG(h.roi_porcentaje), 2) AS roi_promedio
-- FROM tabla_hechos_peliculas h JOIN dimension_generos g ON h.id_pelicula = g.id_pelicula
-- WHERE h.roi_porcentaje IS NOT NULL
-- GROUP BY g.genero HAVING COUNT(*) >= 20 ORDER BY roi_promedio DESC LIMIT 5;

-- KPI 4: oferta vs demanda (aprovecha la poda de particiones por década)
-- SELECT decada, COUNT(*) AS estrenos, ROUND(AVG(cantidad_votos)) AS votos_prom
-- FROM tabla_hechos_peliculas WHERE decada >= 2000 GROUP BY decada ORDER BY decada;
