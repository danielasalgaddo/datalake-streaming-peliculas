import subprocess
import os

# Configura aquí la ruta local donde descargaste los 5 archivos de Drive
RUTA_LOCAL = "./data_cruda" 
RUTA_HDFS_RAW = "/datalake/01_raw"

archivos_imdb = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "title.crew.tsv.gz",
    "name.basics.tsv.gz"
]
archivo_tmdb = "TMDB_all_movies.csv"

def ejecutar_comando_hdfs(comando):
    resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
    if resultado.returncode != 0:
        print(f"Error ejecutando: {comando}\n{resultado.stderr}")
    else:
        print(f"OK: {comando}")

# 1. Crear directorios en HDFS
ejecutar_comando_hdfs(f"hdfs dfs -mkdir -p {RUTA_HDFS_RAW}/imdb")
ejecutar_comando_hdfs(f"hdfs dfs -mkdir -p {RUTA_HDFS_RAW}/tmdb")

# 2. Subir archivos de IMDb (con -f para sobreescribir si se vuelve a correr)
for archivo in archivos_imdb:
    ruta_origen = os.path.join(RUTA_LOCAL, archivo)
    ejecutar_comando_hdfs(f"hdfs dfs -put -f {ruta_origen} {RUTA_HDFS_RAW}/imdb/")

# 3. Subir archivo de TMDB
ruta_origen_tmdb = os.path.join(RUTA_LOCAL, archivo_tmdb)
ejecutar_comando_hdfs(f"hdfs dfs -put -f {ruta_origen_tmdb} {RUTA_HDFS_RAW}/tmdb/")

print("\nIngesta a zona RAW completada.")
