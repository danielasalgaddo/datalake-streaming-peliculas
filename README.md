# Data Lake de Adquisiciones — Plataforma de Streaming

Obligatorio de Big Data (UCU). Pipeline en Python/PySpark sobre HDFS que construye un Data Lake de tres zonas (arquitectura Medallion) a partir de IMDb y TMDB, y calcula los KPIs de adquisición de películas descritos en el informe.

## Arquitectura

```
hdfs:///datalake/
├── 01_raw/        Bronce: archivos originales, inmutables (IMDb .tsv.gz, TMDB .csv)
├── 02_staging/    Plata: datos limpios y tipados en Parquet
├── 03_curated/    Oro: esquema estrella en Parquet + tablas externas Hive
│   ├── tabla_hechos_peliculas/decada=YYYY/
│   ├── dimension_generos/
│   ├── dimension_directores/
│   └── kpis/
└── _metadata/     Manifiesto de ingesta, registro de ejecuciones, reporte de calidad
```

## Pipeline

| Script | Etapa | Entrada → Salida |
|---|---|---|
| `scripts/01_ingesta_raw.py` | Ingesta (sin sobreescribir raw, con checksum MD5) | archivos locales → `01_raw` |
| `scripts/02_limpieza_staging.py` | Limpieza, filtrado y tipado | `01_raw` → `02_staging` |
| `scripts/03_modelo_curated.py` | Modelo dimensional, ROI y joyas ocultas | `02_staging` → `03_curated` |
| `scripts/05_calidad_datos.py` | Validaciones de calidad (corta el pipeline si falla una regla crítica) | `03_curated` → `_metadata/calidad_datos` |
| `scripts/04_kpis.py` | Cálculo de los 5 KPIs | `03_curated` → `03_curated/kpis` + `resultados/*.csv` |
| `scripts/06_visualizaciones.py` | Gráficos PNG para el informe | `resultados/*.csv` → `resultados/graficos/` |
| `sql/crear_tablas_hive.sql` | Tablas externas Hive sobre la zona curated | — |

Las rutas y umbrales de negocio están centralizados en `scripts/config.py`.

## Cómo ejecutarlo

1. Descargar los 5 archivos fuente en `./data_cruda` (`title.basics.tsv.gz`, `title.ratings.tsv.gz`, `title.crew.tsv.gz`, `name.basics.tsv.gz`, `TMDB_all_movies.csv`).
2. Instalar dependencias: `pip install -r requirements.txt`.
3. Correr todo: `./run_pipeline.sh` (o `./run_pipeline.sh --sin-ingesta` para reprocesar desde `01_raw`).
4. Crear las tablas en Hive: `hive -f sql/crear_tablas_hive.sql`.

Para probar sin cluster (Spark local, sin HDFS), copiar los archivos a `datalake_local/01_raw/imdb|tmdb/` y:

```bash
export DATALAKE_BASE=file://$(pwd)/datalake_local
cd scripts && spark-submit --master "local[*]" 02_limpieza_staging.py   # y así con 03, 05, 04
```

## Fuentes

* IMDb Non-Commercial Datasets (https://datasets.imdbws.com/)
* TMDB — dataset abierto `TMDB_all_movies.csv` (cruce por `imdb_id`)
