"""
Configuración central del Data Lake.

Todas las rutas y umbrales de negocio viven acá para que los scripts del
pipeline usen exactamente los mismos valores que se documentan en el informe.

La ruta base se puede cambiar con la variable de entorno DATALAKE_BASE, por
ejemplo para correr el pipeline en modo local sin HDFS:

    export DATALAKE_BASE=file:///home/usuario/datalake
"""
import os

# ---------------------------------------------------------------------------
# Rutas de las zonas
# ---------------------------------------------------------------------------
BASE = os.environ.get("DATALAKE_BASE", "hdfs:///datalake").rstrip("/")

RUTA_RAW = f"{BASE}/01_raw"
RUTA_STAGING = f"{BASE}/02_staging"
RUTA_CURATED = f"{BASE}/03_curated"
RUTA_KPIS = f"{RUTA_CURATED}/kpis"
RUTA_METADATA = f"{BASE}/_metadata"

# Carpeta local donde se dejan los CSV chicos con los resultados de los KPIs
# (los que se usan en Power BI y en las tablas del informe).
SALIDA_LOCAL = os.environ.get("SALIDA_LOCAL", "./resultados")

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
