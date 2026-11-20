# Data Lake de Adquisiciones — Plataforma de Streaming

Obligatorio de Big Data (UCU). Pipeline en PySpark que construye un Data Lake de tres zonas (arquitectura Medallion) sobre HDFS a partir de IMDb y TMDB, y calcula los KPIs de adquisición de películas descritos en el informe. Todo corre en un **cluster local en Docker** (HDFS + Spark + Hive) basado en [Marcel-Jan/docker-hadoop-spark](https://github.com/Marcel-Jan/docker-hadoop-spark), e incluye un dashboard web (Streamlit) para visualizar los resultados.

## Arquitectura

```
hdfs://namenode:9000/datalake/
├── 01_raw/        Bronce: archivos originales, inmutables (IMDb .tsv.gz, TMDB .csv)
├── 02_staging/    Plata: datos limpios y tipados en Parquet
├── 03_curated/    Oro: esquema estrella en Parquet + tablas externas Hive
│   ├── tabla_hechos_peliculas/decada=YYYY/
│   ├── dimension_generos/
│   ├── dimension_directores/
│   └── kpis/
└── _metadata/     Manifiesto de ingesta, registro de ejecuciones, reporte de calidad
```

### Servicios del cluster (`docker-compose.yml`)

| Servicio | Imagen | Para qué | URL en el host |
|---|---|---|---|
| `namenode`, `datanode` | `bde2020/hadoop-*:2.0.0-hadoop3.2.1-java8` | HDFS: zonas raw, staging y curated | http://localhost:9870 |
| `spark-master`, `spark-worker-1` | `datalake-spark:3.5.3` (se construye desde `docker/spark/`) | Ejecuta las etapas 02–06 | http://localhost:8080 · http://localhost:8081 · job en curso: http://localhost:4040 |
| `hive-server`, `hive-metastore`, `hive-metastore-postgresql` | `bde2020/hive:2.3.2-postgresql-metastore` | Tablas externas sobre `03_curated` | JDBC `jdbc:hive2://localhost:10000` |
| `dashboard` | `datalake-dashboard` (se construye desde `docker/dashboard/`) | Dashboard Streamlit | http://localhost:8501 |

Los contenedores se llaman `dl-<servicio>` (por ejemplo `dl-namenode`) para no chocar con otros clusters de Docker de la misma máquina. Dentro de la red de Docker se usan los nombres de servicio (`namenode`, `spark-master`…).

**Diferencias con el repositorio de referencia:**
* **Spark 3.5** (imagen oficial `apache/spark`) en lugar de Spark 3.0 de bde2020: el KPI 1 usa `F.percentile_approx`, que existe desde Spark 3.1, y las etapas 04, 05 y 06 necesitan `pandas` y `matplotlib` en el driver. Corre nativa en Apple Silicon.
* **Sin YARN, history server ni Presto**: Spark corre en modo standalone y el pipeline no usa esos servicios, que suman consumo de memoria.
* **HDFS en `hdfs://namenode:9000`** para todos los servicios (el `hadoop-hive.env` original apunta a 8020), con permisos desactivados y replicación 1, ya que hay un solo datanode.
* Las imágenes `bde2020` solo se publican para Intel: en Macs con Apple Silicon corren emuladas (`platform: linux/amd64`). Funcionan, pero tardan más en arrancar.

## Pipeline

| Script | Etapa | Dónde corre | Entrada → Salida |
|---|---|---|---|
| `scripts/01_ingesta_raw.py` | Ingesta (sin sobreescribir raw, con checksum MD5) | host → `dl-namenode` | `data_cruda/` → `01_raw` |
| `scripts/02_limpieza_staging.py` | Limpieza, filtrado y tipado | Spark | `01_raw` → `02_staging` |
| `scripts/03_modelo_curated.py` | Modelo dimensional, ROI y joyas ocultas | Spark | `02_staging` → `03_curated` |
| `scripts/05_calidad_datos.py` | Validaciones de calidad (corta el pipeline si falla una regla crítica) | Spark | `03_curated` → `_metadata/calidad_datos` + `resultados/reporte_calidad.csv` |
| `scripts/04_kpis.py` | Cálculo de los 5 KPIs | Spark | `03_curated` → `03_curated/kpis` + `resultados/*.csv` |
| `scripts/06_visualizaciones.py` | Gráficos PNG para el informe | `dl-spark-master` | `resultados/*.csv` → `resultados/graficos/` |
| `sql/crear_tablas_hive.sql` | Tablas externas Hive sobre la zona curated | `dl-hive-server` | — |
| `app/dashboard.py` | Dashboard web de los KPIs y del reporte de calidad | `dl-dashboard` | `resultados/*.csv` → navegador |

`./cluster.sh pipeline` ejecuta las etapas en orden (01 → 02 → 03 → 05 → 04 → 06) y **se detiene en la primera que falla**, de modo que nunca se publiquen KPIs sobre datos que no pasaron las validaciones de calidad. Cada ejecución queda registrada en `logs/pipeline_<fecha>.log`.

* **01 (ingesta)** corre en el host con `python3` y envía cada archivo al namenode por la entrada estándar (`docker exec -i dl-namenode hdfs dfs -put - <destino>`), sin copias intermedias dentro del contenedor.
* **02 a 05** se envían con `spark-submit` al master (`spark://spark-master:7077`) desde el contenedor `dl-spark-master`.
* Los CSV y gráficos quedan en **`scripts/resultados/`** del host: la carpeta `scripts/` está montada en los contenedores de Spark y `scripts/resultados/` en el del dashboard.

Las rutas y umbrales de negocio están centralizados en `scripts/config.py`.

## Datos fuente

Se necesitan 5 archivos en la carpeta **`data_cruda/`** de la raíz del repositorio (está en `.gitignore`; ninguno se versiona por su tamaño):

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

* **Docker Desktop** con al menos **10 GB de memoria** asignada (*Settings → Resources → Memory*). Con menos, ver *Ajustar la memoria* más abajo.
* **`python3`** en el host, solo para la ingesta (usa la biblioteca estándar; no hace falta instalar paquetes). En macOS ya viene con las *Command Line Tools*.
* Los puertos 8080, 8081, 4040, 8501, 9010, 9864, 9870 y 10000 libres. Si hay otro cluster de Hadoop/Spark corriendo en Docker, detenerlo antes (`docker compose stop` en su carpeta).

No hace falta instalar Java, Spark, Hadoop, Hive ni Streamlit: todo está en las imágenes.

### Paso a paso

Desde la raíz del repositorio:

```bash
# 1. Copiar los 5 archivos fuente a data_cruda/
mkdir -p data_cruda
cp <carpeta_descargas>/{title.basics,title.ratings,title.crew,name.basics}.tsv.gz data_cruda/
cp <carpeta_descargas>/TMDB_all_movies.csv data_cruda/

# 2. Levantar el cluster (la primera vez construye las imágenes y descarga ~3-4 GB)
./cluster.sh up

# 3. Ejecutar el pipeline completo dentro del cluster
./cluster.sh pipeline

# 4. Crear las tablas externas de Hive y consultarlas
./cluster.sh hive
./cluster.sh beeline
#   p. ej.: SELECT decada, COUNT(*) FROM streaming_curated.tabla_hechos_peliculas GROUP BY decada;

# 5. Ver los resultados en http://localhost:8501 (botón "Recargar datos")

# 6. Al terminar, apagar el cluster (los datos de HDFS y Hive se conservan)
./cluster.sh down
```

### Todos los comandos

| Comando | Qué hace |
|---|---|
| `./cluster.sh up` | Construye las imágenes, levanta los 8 contenedores y espera a que HDFS esté listo |
| `./cluster.sh pipeline` | Ingesta + etapas 02–06 |
| `./cluster.sh pipeline --sin-ingesta` | Reprocesa desde el `01_raw` que ya está en HDFS |
| `./cluster.sh pipeline --forzar` | Vuelve a subir los archivos fuente aunque ya estén en `01_raw` (recarga controlada; por defecto raw es inmutable) |
| `./cluster.sh pipeline --desde 04` | Corre solo desde esa etapa en adelante (`02`, `03`, `05`, `04` o `06`), usando lo que ya está en HDFS. Por ejemplo, `--desde 04` recalcula solo KPIs y gráficos |
| `./cluster.sh hive` | Crea o actualiza las tablas externas de Hive (`sql/crear_tablas_hive.sql`) |
| `./cluster.sh beeline` | Abre una consola SQL de Hive |
| `./cluster.sh status` | Estado de los contenedores y espacio usado por cada zona en HDFS |
| `./cluster.sh down` | Detiene el cluster; HDFS y el metastore se conservan en volúmenes de Docker |
| `./cluster.sh reset` | Detiene el cluster y **borra** HDFS y el metastore (pide confirmación) |

Variables opcionales: `RUTA_LOCAL` (carpeta con los archivos fuente, por defecto `data_cruda/`), `DRIVER_MEMORY` y `EXECUTOR_MEMORY` (memoria de Spark para `pipeline`, por defecto `2g` y `3g`), `SHUFFLE_PARTITIONS` (particiones de los cruces y agregaciones, por defecto `16`), `SPARK_WORKER_MEMORY` y `SPARK_WORKER_CORES` (recursos del worker para `up`, por defecto `4g` y `6`).

### Rendimiento

* **Cuando se está ajustando el análisis**, conviene no repetir todo: `--sin-ingesta` evita los chequeos de la ingesta y `--desde <etapa>` arranca en la etapa que se modificó. Las etapas anteriores reutilizan lo que ya está en HDFS.
* **`spark.sql.shuffle.partitions=16`** (en lugar de 200): con unos 100 MB de datos, 200 particiones generan cientos de tareas y archivos chicos en HDFS, y crear cada archivo es lo que más cuesta en un HDFS emulado.
* **La tabla de hechos se agrupa por década antes de escribirse** (`repartition("decada")` en `03_modelo_curated.py`), así queda un archivo por partición.
* **La mayor limitación es la emulación:** en Apple Silicon el namenode y el datanode corren como Intel emulado, por lo que toda lectura y escritura en HDFS es más lenta que en una máquina Intel o en un cluster real.

### Ajustar la memoria

Con menos de 10 GB para Docker, levantar el worker con menos memoria y achicar el executor en la misma proporción:

```bash
SPARK_WORKER_MEMORY=3g ./cluster.sh up
EXECUTOR_MEMORY=2g ./cluster.sh pipeline
```

`EXECUTOR_MEMORY` siempre tiene que ser menor que `SPARK_WORKER_MEMORY`; si no, el job queda esperando recursos indefinidamente.

### Qué esperar durante la ejecución

* **El pipeline termina solo.** Si todo sale bien, la última línea es `>>> [HH:MM:SS] Pipeline finalizado. Log: logs/pipeline_<fecha>.log`.
* **Duración orientativa:** unos **2–3 minutos** el pipeline completo (medido en un Mac con Apple Silicon y 8 GB para Docker: 2 min 12 s), y menos de 30 segundos con `--desde 04`. La primera ingesta (subir ~1,4 GB a HDFS) suma alrededor de un minuto. Las etapas de Spark más lentas son la 02 (los `.gz` y el CSV `multiLine` de TMDB no se pueden paralelizar) y la 03 (cruces y escritura del modelo). La 06 tarda segundos.
* **Para saber si está avanzando:** Spark muestra barras del tipo `[Stage 3:=====>   (5 + 6) / 11]`; mientras cambien, está trabajando. Es normal que una barra quede quieta un par de minutos al leer un archivo grande. El detalle del job está en http://localhost:4040 y el estado del cluster en http://localhost:8080.
* **Para cortarlo:** `Ctrl + C`.

### Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `The container name "/dl-..." is already in use` | Quedó un contenedor de una ejecución anterior. `./cluster.sh down` y volver a `./cluster.sh up`. |
| `Bind for 0.0.0.0:<puerto> failed: port is already allocated` | Otro programa o cluster usa ese puerto (8080, 9870, 10000…). Detenerlo, o cambiar el puerto del host en `docker-compose.yml`. |
| `El cluster no está levantado` | Falta `./cluster.sh up`, o Docker Desktop no está abierto. |
| `ERROR: no se encontró .../data_cruda/<archivo>` | Falta un archivo fuente o tiene otro nombre. Revisar la tabla de *Datos fuente*. |
| Un contenedor se reinicia, aparece `OutOfMemoryError` o el job queda en `WAITING` en http://localhost:8080 | Falta memoria en Docker, o `EXECUTOR_MEMORY` es mayor que `SPARK_WORKER_MEMORY`. Ver *Ajustar la memoria*. |
| `N regla(s) CRÍTICA(s) fallaron` en la etapa 05 | Corte intencional: los datos no pasaron una validación crítica y no se calculan los KPIs. El detalle queda en `scripts/resultados/reporte_calidad.csv` y en la pestaña *Calidad* del dashboard. |
| El dashboard muestra "Todavía no hay resultados del pipeline" | El pipeline no terminó o falló antes de la etapa 05. Correrlo y tocar *Recargar datos*. |
| `./cluster.sh hive` espera mucho | HiveServer2 puede tardar 1–2 minutos (más con emulación) en aceptar conexiones después de `up`. El script espera hasta 5 minutos. |

## Dashboard

`app/dashboard.py` es un frontend web en Streamlit que corre en el contenedor `dl-dashboard` (http://localhost:8501) y muestra **únicamente los CSV generados por el pipeline** en `scripts/resultados/`; no incluye datos de ejemplo.

| Sección | Contenido | Archivo que lee |
|---|---|---|
| Resumen | Películas en catálogo, % con match TMDB, % con presupuesto, % con ROI calculable, joyas ocultas y estado de calidad | `extra_cobertura_datos.csv`, `reporte_calidad.csv` |
| ROI por género | KPI 1: ROI mediano, promedio o tasa de éxito, con filtro por cantidad mínima de películas | `kpi1_roi_por_genero.csv` |
| Joyas ocultas | KPI 2: tabla con buscador por título y enlace a la ficha de IMDb | `kpi2_joyas_ocultas.csv` |
| Directores | KPI 3: ranking por score ponderado y dispersión calidad vs. popularidad | `kpi3_score_directores.csv` |
| Décadas | KPI 4: oferta (estrenos por año) vs. demanda (votos por película); advierte si la década actual está incompleta | `kpi4_oferta_demanda_decada.csv` |
| Idiomas | KPI 5: idiomas distintos del inglés por votos totales, votos por película o cantidad | `kpi5_expansion_internacional.csv` |
| Calidad | Resultado de cada regla de `05_calidad_datos.py` (OK / REVISAR / FALLA) | `reporte_calidad.csv` |

Si falta algún archivo, la sección correspondiente indica qué script lo genera; por ejemplo, si una regla crítica de calidad corta el pipeline, se ve el reporte de calidad pero no los KPIs. Después de volver a correr el pipeline, el botón **Recargar datos** de la barra lateral toma los resultados nuevos. Si se modifica `app/dashboard.py`, hay que reconstruir el contenedor con `./cluster.sh up`.

## Estructura del repositorio

```
├── cluster.sh               Administra el cluster y ejecuta el pipeline
├── docker-compose.yml       Servicios del cluster
├── docker/
│   ├── hadoop.env           Configuración de HDFS
│   ├── hadoop-hive.env      Configuración de Hive y su metastore
│   ├── spark/               Imagen de Spark 3.5 + pandas/matplotlib
│   └── dashboard/           Imagen del dashboard
├── scripts/                 Etapas del pipeline y config.py
├── sql/                     Tablas externas de Hive
├── app/                     Dashboard (Streamlit)
└── data_cruda/              Archivos fuente (no versionados)
```

## Fuentes

* IMDb Non-Commercial Datasets — https://datasets.imdbws.com/
* TMDB — *The Ultimate 1Million Movies Dataset (TMDB + IMDb)*, Kaggle, licencia Apache 2.0 — https://www.kaggle.com/datasets/alanvourch/tmdb-movies-daily-updates
* Cluster de referencia — https://github.com/Marcel-Jan/docker-hadoop-spark
