# Data Lake de Adquisiciones — Plataforma de Streaming

Obligatorio de Big Data (UCU). Pipeline en Python/PySpark sobre HDFS que construye un Data Lake de tres zonas (arquitectura Medallion) a partir de IMDb y TMDB, y calcula los KPIs de adquisición de películas descritos en el informe. Incluye un dashboard web (Streamlit) para visualizar los resultados.

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
| `scripts/05_calidad_datos.py` | Validaciones de calidad (corta el pipeline si falla una regla crítica) | `03_curated` → `_metadata/calidad_datos` + `resultados/reporte_calidad.csv` |
| `scripts/04_kpis.py` | Cálculo de los 5 KPIs | `03_curated` → `03_curated/kpis` + `resultados/*.csv` |
| `scripts/06_visualizaciones.py` | Gráficos PNG para el informe | `resultados/*.csv` → `resultados/graficos/` |
| `sql/crear_tablas_hive.sql` | Tablas externas Hive sobre la zona curated | — |
| `app/dashboard.py` | Dashboard web de los KPIs y del reporte de calidad | `resultados/*.csv` → navegador |

Las rutas y umbrales de negocio están centralizados en `scripts/config.py`.

`run_pipeline.sh` ejecuta las etapas en orden (01 → 02 → 03 → 05 → 04 → 06) y **se detiene en la primera que falla**, de modo que nunca se publiquen KPIs sobre datos que no pasaron las validaciones de calidad. Cada ejecución queda registrada en `logs/pipeline_<fecha>.log`.

> **Dónde quedan los resultados:** `run_pipeline.sh` hace `cd scripts` antes de ejecutar, así que la carpeta `./resultados` de `config.py` queda en **`scripts/resultados/`**. Se puede cambiar con la variable de entorno `SALIDA_LOCAL`.

## Datos fuente

Se necesitan 5 archivos (ninguno se versiona en el repo por su tamaño):

| Archivo | Origen | Qué aporta |
|---|---|---|
| `title.basics.tsv.gz` | [IMDb](https://datasets.imdbws.com/) | Título, año, duración y géneros |
| `title.ratings.tsv.gz` | [IMDb](https://datasets.imdbws.com/) | Calificación promedio y cantidad de votos |
| `title.crew.tsv.gz` | [IMDb](https://datasets.imdbws.com/) | Directores de cada película (KPI 3) |
| `name.basics.tsv.gz` | [IMDb](https://datasets.imdbws.com/) | Nombres de los directores |
| `TMDB_all_movies.csv` | [Kaggle — The Ultimate 1Million Movies Dataset (TMDB + IMDb)](https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates) | Presupuesto, recaudación, idioma y país (KPIs 1, 2 y 5) |

* Los archivos de IMDb se descargan directamente (no requieren cuenta) y se usan **comprimidos**, con el nombre exacto de la tabla. Las versiones `.tsv` descomprimidas no sirven.
* El de TMDB requiere una cuenta gratuita de Kaggle: botón **Download**, descomprimir el `.zip` y tomar `TMDB_all_movies.csv` (~790 MB). De ese archivo se usan las columnas `imdb_id`, `budget`, `revenue`, `original_language` y `production_countries`; el cruce con IMDb se hace por `imdb_id`.
* El dataset de TMDB **se actualiza diariamente**, por lo que los KPIs pueden variar levemente según la fecha de descarga. Conviene registrar esa fecha en el informe.

## Cómo ejecutarlo

### Requisitos

* **Java 17 o 21** (Spark lo necesita): `java -version`.
* **Python 3.10 – 3.12**. PySpark 3.5 no está probado con Python 3.13+; si `spark-submit` falla con errores de `cloudpickle` o `TypeError` al iniciar, recrear el entorno con una versión anterior (por ejemplo `brew install python@3.11`).
* No hace falta instalar Spark aparte: `pip install pyspark` incluye `spark-submit`.

### Opción A — Modo local (sin cluster ni HDFS)

Todos los comandos se ejecutan desde la raíz del repositorio.

**1. Crear el entorno virtual e instalar dependencias** (solo la primera vez)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

En ejecuciones posteriores alcanza con `source .venv/bin/activate`.

**2. Copiar los 5 archivos fuente a la zona raw local**

```bash
mkdir -p datalake_local/01_raw/imdb datalake_local/01_raw/tmdb
cp <carpeta_descargas>/title.basics.tsv.gz <carpeta_descargas>/title.ratings.tsv.gz \
   <carpeta_descargas>/title.crew.tsv.gz <carpeta_descargas>/name.basics.tsv.gz \
   datalake_local/01_raw/imdb/
cp <carpeta_descargas>/TMDB_all_movies.csv datalake_local/01_raw/tmdb/
```

La estructura debe quedar así (`datalake_local/` está en `.gitignore`):

```
datalake_local/01_raw/
├── imdb/  title.basics.tsv.gz  title.ratings.tsv.gz  title.crew.tsv.gz  name.basics.tsv.gz
└── tmdb/  TMDB_all_movies.csv
```

**3. Ejecutar el pipeline**

```bash
export DATALAKE_BASE=file://$(pwd)/datalake_local
SPARK_SUBMIT='spark-submit --master local[*] --driver-memory 6g' ./run_pipeline.sh --sin-ingesta
```

* `DATALAKE_BASE` hace que los scripts lean y escriban en `datalake_local/` en lugar de HDFS.
* `--sin-ingesta` saltea la etapa 01, que usa comandos `hdfs dfs` y solo funciona en el cluster; el paso 2 la reemplaza.
* `--driver-memory 6g`: en modo local Spark usa 1 GB por defecto, insuficiente para estos volúmenes. Ajustar según la RAM disponible (dejar al menos la mitad libre para el sistema).

**4. Abrir el dashboard**

```bash
streamlit run app/dashboard.py
```

Se abre en el navegador en http://localhost:8501. Para detenerlo: `Ctrl + C`.

### Opción B — Cluster con HDFS

1. Copiar los 5 archivos fuente a `scripts/data_cruda/` (la etapa 01 los busca en `./data_cruda` relativo a `scripts/`), o indicar otra carpeta con `export RUTA_LOCAL=/ruta/absoluta/a/los/archivos`.
2. Instalar dependencias: `pip install -r requirements.txt`.
3. Correr todo: `./run_pipeline.sh`. Para reprocesar desde un `01_raw` ya cargado: `./run_pipeline.sh --sin-ingesta`. La ingesta no sobreescribe archivos existentes en raw salvo que se ejecute `01_ingesta_raw.py --forzar`.
4. Crear las tablas en Hive: `hive -f sql/crear_tablas_hive.sql`.
5. Abrir el dashboard: `streamlit run app/dashboard.py`.

### Qué esperar durante la ejecución

* **El pipeline termina solo.** Si todo sale bien, la última línea es `>>> [HH:MM:SS] Pipeline finalizado. Log: ../logs/pipeline_<fecha>.log`. Recién entonces tiene sentido abrir el dashboard.
* **Duración orientativa en modo local:** entre 10 y 30 minutos con los archivos completos, según la máquina. Las etapas más lentas son la 02 (lectura de los archivos crudos: los `.gz` y el CSV `multiLine` de TMDB no se pueden paralelizar) y la 03 (cruces). La 06 tarda segundos.
* **Para saber si está avanzando:** Spark muestra barras del tipo `[Stage 3:=====>   (5 + 6) / 11]`; mientras cambien, está trabajando. Es normal que una barra quede quieta un par de minutos al leer un archivo grande. Durante la ejecución, la interfaz de Spark está en http://localhost:4040.
* **Para cortarlo:** `Ctrl + C`.

### Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `hdfs: command not found` | Se ejecutó la etapa 01 sin cluster. En modo local usar `--sin-ingesta` (Opción A). |
| `spark-submit: command not found` | No está activado el entorno virtual (`source .venv/bin/activate`) o faltó `pip install -r requirements.txt`. |
| `Path does not exist: file:/.../01_raw/...` | Falta un archivo fuente, tiene otro nombre o no se exportó `DATALAKE_BASE` en la misma consola. Revisar la estructura del paso 2. |
| `java.lang.OutOfMemoryError` o ejecución extremadamente lenta | Poca memoria para Spark. Aumentar `--driver-memory`. |
| `N regla(s) CRÍTICA(s) fallaron` en la etapa 05 | Corte intencional: los datos no pasaron una validación crítica y no se calculan los KPIs. El detalle queda en `scripts/resultados/reporte_calidad.csv` y en la pestaña *Calidad* del dashboard. |
| El dashboard muestra "Todavía no hay resultados del pipeline" | El pipeline no terminó o los CSV no están en `scripts/resultados/`. Correr el pipeline y tocar *Recargar datos*. |
| `Port 8501 is already in use` | Ya hay un dashboard abierto. Cerrarlo o usar `streamlit run app/dashboard.py --server.port 8502`. |

## Dashboard

`app/dashboard.py` es un frontend web en Streamlit que muestra **únicamente los CSV generados por los scripts del pipeline**; no necesita Spark, HDFS ni Hive para ejecutarse, y no incluye datos de ejemplo.

| Sección | Contenido | Archivo que lee |
|---|---|---|
| Resumen | Películas en catálogo, % con match TMDB, % con presupuesto, % con ROI calculable, joyas ocultas y estado de calidad | `extra_cobertura_datos.csv`, `reporte_calidad.csv` |
| ROI por género | KPI 1: ROI mediano, promedio o tasa de éxito, con filtro por cantidad mínima de películas | `kpi1_roi_por_genero.csv` |
| Joyas ocultas | KPI 2: tabla con buscador por título y enlace a la ficha de IMDb | `kpi2_joyas_ocultas.csv` |
| Directores | KPI 3: ranking por score ponderado y dispersión calidad vs. popularidad | `kpi3_score_directores.csv` |
| Décadas | KPI 4: oferta (estrenos por año) vs. demanda (votos por película); advierte si la década actual está incompleta | `kpi4_oferta_demanda_decada.csv` |
| Idiomas | KPI 5: idiomas distintos del inglés por votos totales, votos por película o cantidad | `kpi5_expansion_internacional.csv` |
| Calidad | Resultado de cada regla de `05_calidad_datos.py` (OK / REVISAR / FALLA) | `reporte_calidad.csv` |

Busca los CSV, en este orden, en `$SALIDA_LOCAL` (si está definida), `scripts/resultados/` y `resultados/`. Si falta algún archivo, la sección correspondiente indica qué script lo genera; por ejemplo, si una regla crítica de calidad corta el pipeline, se ve el reporte de calidad pero no los KPIs. Después de volver a correr el pipeline, el botón **Recargar datos** de la barra lateral toma los resultados nuevos.

## Fuentes

* IMDb Non-Commercial Datasets — https://datasets.imdbws.com/
* TMDB — *The Ultimate 1Million Movies Dataset (TMDB + IMDb)*, Kaggle, licencia Apache 2.0 — https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates
