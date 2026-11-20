#!/usr/bin/env bash
# Administra el cluster del Data Lake en Docker (docker-compose.yml) y ejecuta
# el pipeline dentro de él.
#
# Uso:
#   ./cluster.sh up                       Construye las imágenes y levanta el cluster
#   ./cluster.sh pipeline                 Ingesta (01) + etapas 02-06 en el cluster
#   ./cluster.sh pipeline --sin-ingesta   Reprocesa desde 01_raw ya cargado en HDFS
#   ./cluster.sh pipeline --forzar        Vuelve a subir los archivos fuente a 01_raw
#   ./cluster.sh pipeline --desde 04      Corre solo desde esa etapa (02, 03, 05, 04 o 06)
#   ./cluster.sh hive                     Crea/actualiza las tablas externas de Hive
#   ./cluster.sh beeline                  Abre una consola SQL de Hive
#   ./cluster.sh status                   Estado de los contenedores y de HDFS
#   ./cluster.sh down                     Detiene el cluster (conserva los datos)
#   ./cluster.sh reset                    Detiene el cluster y BORRA HDFS y el metastore
#
# Variables opcionales:
#   RUTA_LOCAL           carpeta con los 5 archivos fuente  (default: ./data_cruda)
#   DRIVER_MEMORY        memoria del driver de Spark        (default: 2g)
#   EXECUTOR_MEMORY      memoria del executor de Spark      (default: 3g)
#   SPARK_WORKER_MEMORY  memoria del worker, para `up`      (default: 4g)
#   SPARK_WORKER_CORES   núcleos del worker, para `up`      (default: 6)
#   SHUFFLE_PARTITIONS   spark.sql.shuffle.partitions       (default: 16)
set -euo pipefail
RAIZ="$(cd "$(dirname "$0")" && pwd)"
cd "$RAIZ"

HIVE_JDBC="jdbc:hive2://localhost:10000"

esperar () {  # esperar <descripción> <segundos> <comando...>
  local desc="$1" limite="$2"; shift 2
  printf "Esperando %s" "$desc"
  for ((i = 0; i < limite; i += 3)); do
    if "$@" >/dev/null 2>&1; then echo " listo."; return 0; fi
    printf "."; sleep 3
  done
  echo; echo "ERROR: $desc no respondió en ${limite}s. Revisar: docker compose logs" >&2
  return 1
}

hdfs_listo () { docker exec dl-namenode hdfs dfsadmin -safemode get | grep -q "OFF"; }
hive_listo () { docker exec dl-hive-server bash -c "beeline -u $HIVE_JDBC -n root -e 'SHOW DATABASES' 2>/dev/null | grep -q default"; }

requiere_cluster () {
  if [[ "$(docker inspect -f '{{.State.Running}}' dl-namenode 2>/dev/null)" != "true" ]]; then
    echo "El cluster no está levantado. Ejecutar primero: ./cluster.sh up" >&2
    exit 1
  fi
}

# ---------------------------------------------------------------- pipeline
# Ejecuta las etapas en orden y se detiene ante el primer error (set -e +
# pipefail), de modo que nunca se publiquen KPIs sobre datos que no pasaron
# las validaciones de calidad (05 termina con código 1 si falla una regla crítica).
pipeline () {
  local modo_ingesta="" desde="01"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --sin-ingesta|--forzar) modo_ingesta="$1"; shift ;;
      --desde) desde="${2:-}"; shift 2 || true ;;
      *) echo "Opción desconocida: $1 (usar --sin-ingesta, --forzar o --desde <etapa>)" >&2; exit 1 ;;
    esac
  done
  # Orden de ejecución: la calidad (05) va antes que los KPIs (04)
  local orden="01 02 03 05 04 06"
  if [[ " $orden " != *" $desde "* ]]; then
    echo "Etapa inválida para --desde: '$desde' (opciones: $orden)" >&2; exit 1
  fi
  local inicio; inicio=$(( $(echo "$orden" | tr ' ' '\n' | grep -n "^$desde$" | cut -d: -f1) ))
  corre () {  # corre <etapa>: ¿la etapa está en o después de --desde?
    (( $(echo "$orden" | tr ' ' '\n' | grep -n "^$1$" | cut -d: -f1) >= inicio ))
  }

  local log_dir="$RAIZ/logs"; mkdir -p "$log_dir" "$RAIZ/scripts/resultados"
  LOG="$log_dir/pipeline_$(date +%Y%m%d_%H%M%S).log"
  etapa () { echo -e "\n>>> [$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

  # Las etapas 02-06 corren en dl-spark-master; los scripts están montados en
  # /opt/proyecto/scripts y el driver envía el trabajo al worker.
  # shuffle.partitions=16: con ~100 MB de datos, las 200 particiones por defecto
  # generan cientos de tareas y archivos chicos en HDFS (ver README, rendimiento).
  local en_spark=(docker exec -i -w /opt/proyecto/scripts dl-spark-master)
  local spark_submit=("${en_spark[@]}" spark-submit --master spark://spark-master:7077
    --driver-memory "${DRIVER_MEMORY:-2g}" --executor-memory "${EXECUTOR_MEMORY:-3g}"
    --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-16}"
    --conf spark.hadoop.dfs.replication=1)

  [[ "$desde" != "01" ]] && echo "Ejecutando desde la etapa $desde (las anteriores usan lo que ya está en HDFS)." | tee -a "$LOG"

  if corre 01 && [[ "$modo_ingesta" != "--sin-ingesta" ]]; then
    etapa "01 Ingesta a 01_raw (desde ${RUTA_LOCAL:-data_cruda/})"
    local flag=(); [[ "$modo_ingesta" == "--forzar" ]] && flag=(--forzar)
    python3 scripts/01_ingesta_raw.py ${flag[@]+"${flag[@]}"} 2>&1 | tee -a "$LOG"
  fi

  if corre 02; then
    etapa "02 Limpieza y tipado -> 02_staging"
    "${spark_submit[@]}" 02_limpieza_staging.py 2>&1 | tee -a "$LOG"
  fi

  if corre 03; then
    etapa "03 Modelo dimensional -> 03_curated"
    "${spark_submit[@]}" 03_modelo_curated.py 2>&1 | tee -a "$LOG"
  fi

  if corre 05; then
    etapa "05 Validaciones de calidad"
    "${spark_submit[@]}" 05_calidad_datos.py 2>&1 | tee -a "$LOG"
  fi

  if corre 04; then
    etapa "04 Cálculo de KPIs"
    "${spark_submit[@]}" 04_kpis.py 2>&1 | tee -a "$LOG"
  fi

  etapa "06 Gráficos para el informe"
  "${en_spark[@]}" python3 06_visualizaciones.py 2>&1 | tee -a "$LOG"

  etapa "Pipeline finalizado. Log: ${LOG#"$RAIZ"/}"
  echo "Resultados en scripts/resultados/ · dashboard: http://localhost:8501 (botón 'Recargar datos')"
}

case "${1:-}" in
  up)
    mkdir -p scripts/resultados
    docker compose up -d --build
    esperar "a HDFS (namenode fuera de safe mode)" 180 hdfs_listo
    echo
    echo "Cluster levantado:"
    echo "  HDFS namenode  http://localhost:9870"
    echo "  Spark master   http://localhost:8080"
    echo "  Spark worker   http://localhost:8081"
    echo "  Dashboard      http://localhost:8501"
    echo "  Hive (JDBC)    $HIVE_JDBC   (puede tardar 1-2 min más en aceptar conexiones)"
    ;;

  pipeline)
    requiere_cluster
    shift
    pipeline "$@"
    ;;

  hive)
    requiere_cluster
    esperar "a HiveServer2" 300 hive_listo
    docker exec dl-hive-server beeline -u "$HIVE_JDBC" -n root -f /opt/proyecto/sql/crear_tablas_hive.sql
    ;;

  beeline)
    requiere_cluster
    docker exec -it dl-hive-server beeline -u "$HIVE_JDBC" -n root
    ;;

  status)
    docker compose ps
    if [[ "$(docker inspect -f '{{.State.Running}}' dl-namenode 2>/dev/null)" == "true" ]]; then
      echo; docker exec dl-namenode hdfs dfs -du -h /datalake 2>/dev/null || echo "(/datalake todavía no existe en HDFS)"
    fi
    ;;

  down)
    docker compose down
    ;;

  reset)
    read -r -p "Esto borra todo HDFS y el metastore de Hive. ¿Continuar? [s/N] " r
    [[ "$r" =~ ^[sS]$ ]] && docker compose down -v
    ;;

  *)
    sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
