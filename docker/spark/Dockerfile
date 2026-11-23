# Spark 3.5 con las librerías
# que usan los scripts en el driver: pandas (toPandas en 04 y 05) y
# matplotlib (06_visualizaciones.py). La imagen oficial es multi-arquitectura,
# así que corre nativa en Apple Silicon.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
COPY requirements.txt /tmp/requirements.txt
RUN (command -v pip3 >/dev/null || (apt-get update && apt-get install -y --no-install-recommends python3-pip)) \
    && pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    HADOOP_USER_NAME=root
WORKDIR /opt/proyecto/scripts
