"""
06_visualizaciones.py — Gráficos estáticos (PNG) para el informe, generados a
partir de los CSV que exporta 04_kpis.py. No necesita Spark.

Complementan los visuales de Power BI con las métricas robustas que agrega el
pipeline (mediana de ROI, estrenos por año y votos por película).
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from config import SALIDA_LOCAL

AZUL, TINTA, TINTA_2, MUTED, FONDO = "#2a78d6", "#0b0b0b", "#52514e", "#898781", "#fcfcfb"
SALIDA = os.path.join(SALIDA_LOCAL, "graficos")
os.makedirs(SALIDA, exist_ok=True)


def barras_h(df, etiqueta, valor, titulo, subtitulo, archivo, formato="{:,.0f}", ordenar=True):
    # Barras horizontales: se dibujan de abajo hacia arriba
    df = df.sort_values(valor) if ordenar else df.iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 0.45 * len(df) + 1.4), facecolor=FONDO)
    ax.set_facecolor(FONDO)
    ax.barh(df[etiqueta].astype(str), df[valor], color=AZUL, height=0.6)
    for y, v in enumerate(df[valor]):
        ax.text(v, y, "  " + formato.format(v), va="center", fontsize=9, color=TINTA_2)
    for lado in ("top", "right", "left"):
        ax.spines[lado].set_visible(False)
    ax.spines["bottom"].set_color(MUTED)
    ax.tick_params(colors=TINTA_2, labelsize=9, length=0)
    ax.xaxis.grid(True, color="#e6e5e0", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlim(0, df[valor].max() * 1.18)
    fig.suptitle(titulo, x=0.02, ha="left", fontsize=12, color=TINTA, fontweight="bold")
    ax.set_title(subtitulo, loc="left", fontsize=9, color=MUTED)
    fig.tight_layout()
    fig.savefig(os.path.join(SALIDA, archivo), dpi=200)
    plt.close(fig)
    print(f"OK: {archivo}")


kpi1 = pd.read_csv(f"{SALIDA_LOCAL}/kpi1_roi_por_genero.csv")
barras_h(kpi1.nlargest(10, "roi_mediana_pct"), "genero", "roi_mediana_pct",
         "ROI mediano por género", "% sobre presupuesto · películas con presupuesto > USD 10k",
         "kpi1_roi_mediano_genero.png", "{:,.0f}%")

kpi4 = pd.read_csv(f"{SALIDA_LOCAL}/kpi4_oferta_demanda_decada.csv")
barras_h(kpi4.assign(decada=kpi4["decada"].astype(str) + "s"), "decada", "estrenos_por_anio",
         "Estrenos promedio por año, por década",
         "Normaliza la década en curso, que todavía no está completa",
         "kpi4_estrenos_por_anio.png", ordenar=False)

kpi5 = pd.read_csv(f"{SALIDA_LOCAL}/kpi5_expansion_internacional.csv")
barras_h(kpi5.head(8), "idioma", "votos_por_pelicula",
         "Votos promedio por película, por idioma original",
         "Idiomas distintos del inglés · top 8 por votos totales",
         "kpi5_votos_por_pelicula_idioma.png")
