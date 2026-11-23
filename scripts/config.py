"""
Configuración central del Data Lake.

Todas las rutas y umbrales de negocio viven acá para que los scripts del
pipeline usen exactamente los mismos valores que se documentan en el informe.

Los scripts de Spark corren dentro del cluster de Docker (docker-compose.yml):
el Data Lake vive en el HDFS del servicio namenode y los CSV de resultados se
escriben en la carpeta scripts/resultados, montada desde el host.
"""
import os

# ---------------------------------------------------------------------------
# Rutas de las zonas
# ---------------------------------------------------------------------------
BASE = "hdfs://namenode:9000/datalake"

RUTA_RAW = f"{BASE}/01_raw"
RUTA_STAGING = f"{BASE}/02_staging"
RUTA_CURATED = f"{BASE}/03_curated"
RUTA_KPIS = f"{RUTA_CURATED}/kpis"
RUTA_METADATA = f"{BASE}/_metadata"

# Carpeta donde se dejan los CSV chicos con los resultados de los KPIs (los que
# se usan en Power BI, en el dashboard y en las tablas del informe). Es
# scripts/resultados en el host, montada en los contenedores de Spark.
SALIDA_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")

# ---------------------------------------------------------------------------
# Reglas de negocio (deben coincidir con el informe)
# ---------------------------------------------------------------------------
ANIO_MINIMO = 1980                 # Alcance temporal del catálogo
PRESUPUESTO_MINIMO_ROI = 10_000    # USD; por debajo se considera dato no confiable
MIN_PELICULAS_GENERO = 20          # KPI 1
JOYA_RATING_MIN = 7.5              # KPI 2
JOYA_VOTOS_MIN = 10_000            # KPI 2
JOYA_PERCENTIL_PRESUPUESTO = 0.25  # KPI 2 (cuartil inferior)
MIN_PELICULAS_DIRECTOR = 3         # KPI 3
IDIOMA_EXCLUIDO = "en"             # KPI 5
TOP_N = 10                         # Filas que se exportan por ranking
