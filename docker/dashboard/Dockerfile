# Dashboard Streamlit (app/dashboard.py). Lee los CSV que deja el pipeline en
# scripts/resultados, montados como volumen de solo lectura en /datos.
FROM python:3.12-slim

COPY docker/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
COPY app/dashboard.py /app/dashboard.py
ENV SALIDA_LOCAL=/datos
EXPOSE 8501
CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
