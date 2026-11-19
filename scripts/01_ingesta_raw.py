"""
01_ingesta_raw.py — Ingesta de archivos fuente hacia la zona 01_raw (HDFS).

Reglas de gobierno que aplica:
  * Inmutabilidad: si un archivo ya existe en 01_raw NO se sobreescribe
    (salvo que se pase --forzar explícitamente para una recarga controlada).
  * Linaje: por cada archivo subido se registra una fila en
    _metadata/manifiesto_ingesta.csv con fuente, tamaño, checksum MD5 y fecha.
  * Fallo temprano: si un comando HDFS falla, el script termina con error
    para que el orquestador no siga con datos incompletos.
"""
import argparse
import csv
import hashlib
import io
import os
import subprocess
import sys
from datetime import datetime, timezone

# Configura aquí la ruta local donde descargaste los 5 archivos de Drive
RUTA_LOCAL = os.environ.get("RUTA_LOCAL", "./data_cruda")
RUTA_HDFS_RAW = "/datalake/01_raw"
RUTA_HDFS_METADATA = "/datalake/_metadata"
MANIFIESTO = f"{RUTA_HDFS_METADATA}/manifiesto_ingesta.csv"

FUENTES = {
    "imdb": [
        "title.basics.tsv.gz",
        "title.ratings.tsv.gz",
        "title.crew.tsv.gz",
        "name.basics.tsv.gz",
    ],
    "tmdb": ["TMDB_all_movies.csv"],
}


def hdfs(comando, permitir_fallo=False):
    resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
    if resultado.returncode != 0 and not permitir_fallo:
        print(f"ERROR ejecutando: {comando}\n{resultado.stderr}")
        sys.exit(1)
    return resultado


def existe_en_hdfs(ruta):
    return hdfs(f"hdfs dfs -test -e {ruta}", permitir_fallo=True).returncode == 0


def md5(ruta, bloque=8 * 1024 * 1024):
    h = hashlib.md5()
    with open(ruta, "rb") as f:
        for trozo in iter(lambda: f.read(bloque), b""):
            h.update(trozo)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--forzar", action="store_true",
                        help="Sobreescribe archivos existentes en 01_raw (recarga controlada)")
    args = parser.parse_args()

    # 1. Crear directorios en HDFS
    for fuente in FUENTES:
        hdfs(f"hdfs dfs -mkdir -p {RUTA_HDFS_RAW}/{fuente}")
    hdfs(f"hdfs dfs -mkdir -p {RUTA_HDFS_METADATA}")

    # 2. Subir archivos y registrar metadatos técnicos
    registros = []
    fecha = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for fuente, archivos in FUENTES.items():
        for archivo in archivos:
            origen = os.path.join(RUTA_LOCAL, archivo)
            destino = f"{RUTA_HDFS_RAW}/{fuente}/{archivo}"
            if not os.path.exists(origen):
                print(f"ERROR: no se encontró {origen}")
                sys.exit(1)

            if existe_en_hdfs(destino) and not args.forzar:
                print(f"OMITIDO (ya existe, raw es inmutable): {destino}")
                continue

            flag = "-f " if args.forzar else ""
            hdfs(f"hdfs dfs -put {flag}{origen} {destino}")
            print(f"OK: {origen} -> {destino}")
            registros.append({
                "fecha_ingesta_utc": fecha,
                "fuente": fuente,
                "archivo": archivo,
                "ruta_hdfs": destino,
                "bytes": os.path.getsize(origen),
                "md5": md5(origen),
                "modo": "recarga_forzada" if args.forzar else "carga_inicial",
            })

    # 3. Anexar al manifiesto de ingesta (linaje de la zona raw)
    if registros:
        buffer = io.StringIO()
        escritor = csv.DictWriter(buffer, fieldnames=list(registros[0].keys()))
        if not existe_en_hdfs(MANIFIESTO):
            escritor.writeheader()
        escritor.writerows(registros)
        tmp = "manifiesto_tmp.csv"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(buffer.getvalue())
        if existe_en_hdfs(MANIFIESTO):
            hdfs(f"hdfs dfs -appendToFile {tmp} {MANIFIESTO}")
        else:
            hdfs(f"hdfs dfs -put {tmp} {MANIFIESTO}")
        os.remove(tmp)

    print(f"\nIngesta a zona RAW completada ({len(registros)} archivo(s) nuevos).")


if __name__ == "__main__":
    main()
