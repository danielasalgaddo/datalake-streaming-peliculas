#!/usr/bin/env bash
# Orquestador del pipeline: ejecuta las etapas en orden y se detiene ante el
# primer error (set -e), de modo que nunca se publiquen KPIs sobre datos que
# no pasaron las validaciones de calidad.
#
# Uso:  ./run_pipeline.sh            (pipeline completo sobre HDFS)
#       ./run_pipeline.sh --sin-ingesta   (reprocesa desde 01_raw existente)
set -euo pipefail
cd "$(dirname "$0")/scripts"

SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
LOG_DIR="../logs"; mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"

etapa () { echo -e "\n>>> [$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

if [[ "${1:-}" != "--sin-ingesta" ]]; then
  etapa "01 Ingesta a 01_raw"
  python3 01_ingesta_raw.py 2>&1 | tee -a "$LOG"
fi

etapa "02 Limpieza y tipado -> 02_staging"
$SPARK_SUBMIT 02_limpieza_staging.py 2>&1 | tee -a "$LOG"

etapa "03 Modelo dimensional -> 03_curated"
$SPARK_SUBMIT 03_modelo_curated.py 2>&1 | tee -a "$LOG"

etapa "05 Validaciones de calidad"
$SPARK_SUBMIT 05_calidad_datos.py 2>&1 | tee -a "$LOG"

etapa "04 Cálculo de KPIs"
$SPARK_SUBMIT 04_kpis.py 2>&1 | tee -a "$LOG"

etapa "06 Gráficos para el informe"
python3 06_visualizaciones.py 2>&1 | tee -a "$LOG"

etapa "Pipeline finalizado. Log: $LOG"
