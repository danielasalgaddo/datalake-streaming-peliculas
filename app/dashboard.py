"""
dashboard.py — Frontend del Data Lake de Adquisiciones (Streamlit).

Muestra únicamente los CSV que generan los scripts del pipeline
(04_kpis.py y 05_calidad_datos.py); no necesita Spark, HDFS ni Hive.
Si un archivo no existe, indica qué script hay que ejecutar.

Uso (desde la raíz del repo):
    streamlit run app/dashboard.py
    SALIDA_LOCAL=/ruta/a/resultados streamlit run app/dashboard.py
"""
import os
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

RAIZ = Path(__file__).resolve().parent.parent
AZUL, GRIS = "#2a78d6", "#898781"

# clave -> (archivo que genera el pipeline, script que lo genera)
ARCHIVOS = {
    "kpi1": ("kpi1_roi_por_genero.csv", "04_kpis.py"),
    "kpi2": ("kpi2_joyas_ocultas.csv", "04_kpis.py"),
    "kpi3": ("kpi3_score_directores.csv", "04_kpis.py"),
    "kpi4": ("kpi4_oferta_demanda_decada.csv", "04_kpis.py"),
    "kpi5": ("kpi5_expansion_internacional.csv", "04_kpis.py"),
    "cobertura": ("extra_cobertura_datos.csv", "04_kpis.py"),
    "calidad": ("reporte_calidad.csv", "05_calidad_datos.py"),
}


def carpetas_candidatas():
    """Dónde escriben los scripts su SALIDA_LOCAL (ver scripts/config.py).

    run_pipeline.sh hace `cd scripts`, así que ./resultados queda en
    scripts/resultados; si se corren desde la raíz, queda en ./resultados.
    """
    if os.environ.get("SALIDA_LOCAL"):
        yield Path(os.environ["SALIDA_LOCAL"]).expanduser().resolve()
    yield RAIZ / "scripts" / "resultados"
    yield RAIZ / "resultados"


def encontrar_resultados():
    for carpeta in carpetas_candidatas():
        if any((carpeta / archivo).exists() for archivo, _ in ARCHIVOS.values()):
            return carpeta
    return None


@st.cache_data
def cargar(carpeta: str):
    datos = {}
    for clave, (archivo, _) in ARCHIVOS.items():
        ruta = Path(carpeta) / archivo
        datos[clave] = pd.read_csv(ruta) if ruta.exists() else None
    return datos


def falta(clave):
    """Muestra un aviso y devuelve True si el pipeline no generó ese CSV."""
    if datos[clave] is not None:
        return False
    archivo, script = ARCHIVOS[clave]
    st.info(f"No se encontró `{archivo}`. Se genera al ejecutar `scripts/{script}` "
            "(o el pipeline completo con `./run_pipeline.sh`).")
    return True


def barras_h(df, etiqueta, valor, titulo_eje, formato=",.0f", orden="-x", alto=None):
    base = alt.Chart(df).encode(
        y=alt.Y(f"{etiqueta}:N", sort=orden, title=None),
        x=alt.X(f"{valor}:Q", title=titulo_eje),
        tooltip=[alt.Tooltip(c, format=formato) if pd.api.types.is_numeric_dtype(df[c])
                 else c for c in df.columns],
    )
    barras = base.mark_bar(color=AZUL, cornerRadiusEnd=3, height=16)
    textos = base.mark_text(align="left", dx=4, color=GRIS).encode(
        text=alt.Text(f"{valor}:Q", format=formato))
    return (barras + textos).properties(height=alto or max(28 * len(df), 160))


# ---------------------------------------------------------------------------
st.set_page_config(page_title="Data Lake · Adquisiciones", page_icon="🎬", layout="wide")

carpeta = encontrar_resultados()
with st.sidebar:
    st.header("🎬 Data Lake")
    st.caption("Adquisiciones de películas · IMDb + TMDB")
    st.markdown("**Fuente de datos**")
    st.code(str(carpeta.relative_to(RAIZ) if carpeta and carpeta.is_relative_to(RAIZ)
                else carpeta or "— sin resultados —"), language=None)
    if st.button("Recargar datos"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.markdown("**Pipeline**")
    st.markdown("01 Ingesta → 02 Staging → 03 Curated → 05 Calidad → 04 KPIs")

st.title("Adquisiciones de películas")
st.caption("KPIs calculados sobre la zona 03_curated del Data Lake (arquitectura Medallion).")

if carpeta is None:
    buscadas = "\n".join(f"- `{c}`" for c in carpetas_candidatas())
    st.warning("**Todavía no hay resultados del pipeline.** Ejecutá `./run_pipeline.sh` "
               "(o los scripts 02 → 03 → 05 → 04 en modo local, ver README) y luego "
               f"*Recargar datos*.\n\nSe buscaron los CSV en:\n{buscadas}", icon="⚠️")
    st.stop()

datos = cargar(str(carpeta))

# --- Resumen ----------------------------------------------------------------
cob = datos["cobertura"]
calidad = datos["calidad"]
if cob is not None:
    c = cob.iloc[0]
    m = st.columns(5)
    m[0].metric("Películas en catálogo", f"{int(c.peliculas_catalogo):,}".replace(",", "."))
    m[1].metric("Con match TMDB", f"{c.pct_con_match_tmdb}%")
    m[2].metric("Con presupuesto", f"{c.pct_con_presupuesto}%")
    m[3].metric("ROI calculable", f"{c.pct_con_roi_calculable}%")
    m[4].metric("Joyas ocultas", f"{int(c.joyas_ocultas_detectadas):,}".replace(",", "."))

if calidad is not None:
    fallas = (calidad.estado == "FALLA").sum()
    revisar = (calidad.estado == "REVISAR").sum()
    if fallas:
        st.error(f"Calidad: {fallas} regla(s) crítica(s) fallaron. Ver pestaña *Calidad*.")
    else:
        st.success(f"Calidad: las {(calidad.severidad == 'CRITICA').sum()} reglas críticas se "
                   f"cumplen · {revisar} advertencia(s) para revisar.")

tabs = st.tabs(["💰 ROI por género", "💎 Joyas ocultas", "🎬 Directores",
                "📅 Décadas", "🌍 Idiomas", "✅ Calidad"])

# --- KPI 1 ------------------------------------------------------------------
with tabs[0]:
    st.subheader("KPI 1 · Retorno de inversión por género")
    if not falta("kpi1"):
        df = datos["kpi1"]
        st.caption("El promedio es muy sensible a outliers; la mediana describe mejor la película típica.")
        metrica = st.radio("Métrica", ["roi_mediana_pct", "roi_promedio_pct", "tasa_exito_pct"],
                           horizontal=True, format_func={
                               "roi_mediana_pct": "ROI mediano (%)",
                               "roi_promedio_pct": "ROI promedio (%)",
                               "tasa_exito_pct": "Tasa de éxito (ROI > 0)"}.get)
        min_pel = st.slider("Mínimo de películas analizadas", 0,
                            int(df.peliculas_analizadas.max()), 20, step=10)
        filtrado = df[df.peliculas_analizadas >= min_pel]
        st.altair_chart(barras_h(filtrado, "genero", metrica, "%", ",.1f"), width="stretch")
        with st.expander("Ver tabla"):
            st.dataframe(filtrado, hide_index=True, width="stretch")

# --- KPI 2 ------------------------------------------------------------------
with tabs[1]:
    st.subheader("KPI 2 · Joyas ocultas")
    if not falta("kpi2"):
        df = datos["kpi2"]
        st.caption("Alta calificación y muchos votos, con presupuesto en el cuartil inferior: "
                   "candidatas a adquirir barato.")
        buscar = st.text_input("Buscar título", placeholder="Parte del título")
        if buscar:
            df = df[df.titulo_principal.str.contains(buscar, case=False, na=False)]
        df = df.assign(id_pelicula="https://www.imdb.com/title/" + df.id_pelicula)
        st.dataframe(
            df, hide_index=True, width="stretch",
            column_config={
                "id_pelicula": st.column_config.LinkColumn(
                    "IMDb", display_text=r"title/(tt\d+)", help="Abre la ficha en IMDb"),
                "titulo_principal": "Título",
                "anio_estreno": st.column_config.NumberColumn("Año", format="%d"),
                "calificacion_promedio": st.column_config.ProgressColumn(
                    "Rating", min_value=0, max_value=10, format="%.1f"),
                "cantidad_votos": st.column_config.NumberColumn("Votos", format="localized"),
                "presupuesto_usd": st.column_config.NumberColumn("Presupuesto", format="dollar"),
                "idioma_original": "Idioma",
            })

# --- KPI 3 ------------------------------------------------------------------
with tabs[2]:
    st.subheader("KPI 3 · Consistencia de directores")
    if not falta("kpi3"):
        df = datos["kpi3"]
        st.caption("Score = rating promedio × log₁₀(votos totales), directores con 3 o más películas.")
        izq, der = st.columns([3, 2])
        with izq:
            st.altair_chart(barras_h(df, "nombre_director", "score_ponderado", "Score", ",.2f"),
                            width="stretch")
        with der:
            disp = alt.Chart(df).mark_circle(color=AZUL, opacity=0.8).encode(
                x=alt.X("votos_totales:Q", scale=alt.Scale(type="log"), title="Votos totales (log)"),
                y=alt.Y("rating_promedio:Q", scale=alt.Scale(zero=False), title="Rating promedio"),
                size=alt.Size("peliculas:Q", title="Películas"),
                tooltip=["nombre_director", "peliculas", "rating_promedio",
                         alt.Tooltip("votos_totales", format=",")],
            ).properties(height=300, title="Calidad vs. popularidad")
            st.altair_chart(disp, width="stretch")

# --- KPI 4 ------------------------------------------------------------------
with tabs[3]:
    st.subheader("KPI 4 · Oferta vs. demanda por década")
    if not falta("kpi4"):
        df = datos["kpi4"].assign(etiqueta=lambda d: d.decada.astype(str) + "s")
        incompletas = df[df.anios_con_datos < 10]
        if not incompletas.empty:
            st.info(f"La década {', '.join(incompletas.etiqueta)} está incompleta "
                    f"({int(incompletas.anios_con_datos.iloc[0])} años con datos); "
                    "por eso se compara con *estrenos por año*.")
        a, b = st.columns(2)
        with a:
            st.markdown("**Oferta:** estrenos promedio por año")
            st.altair_chart(alt.Chart(df).mark_bar(color=AZUL, cornerRadiusEnd=3).encode(
                x=alt.X("etiqueta:N", title=None, sort=None),
                y=alt.Y("estrenos_por_anio:Q", title="Estrenos / año"),
                tooltip=["etiqueta", "cantidad_estrenos", "estrenos_por_anio", "anios_con_datos"],
            ).properties(height=300), width="stretch")
        with b:
            st.markdown("**Demanda:** votos promedio por película")
            st.altair_chart(alt.Chart(df).mark_line(color=AZUL, point=True).encode(
                x=alt.X("etiqueta:N", title=None, sort=None),
                y=alt.Y("votos_promedio_por_pelicula:Q", title="Votos / película"),
                tooltip=["etiqueta", "votos_promedio_por_pelicula", "rating_promedio"],
            ).properties(height=300), width="stretch")

# --- KPI 5 ------------------------------------------------------------------
with tabs[4]:
    st.subheader("KPI 5 · Expansión internacional")
    if not falta("kpi5"):
        df = datos["kpi5"]
        st.caption("Idiomas originales distintos del inglés.")
        metrica = st.radio("Ordenar por", ["votos_totales", "votos_por_pelicula", "peliculas"],
                           horizontal=True, format_func={
                               "votos_totales": "Votos totales",
                               "votos_por_pelicula": "Votos por película",
                               "peliculas": "Cantidad de películas"}.get)
        st.altair_chart(barras_h(df, "idioma", metrica, None), width="stretch")

# --- Calidad ----------------------------------------------------------------
with tabs[5]:
    st.subheader("Reporte de calidad de datos")
    if not falta("calidad"):
        st.caption(f"Ejecución: {calidad.fecha_ejecucion.iloc[0]} · "
                   "una regla CRÍTICA que falla detiene el pipeline antes de publicar KPIs.")
        icono = {"OK": "✅ OK", "REVISAR": "⚠️ REVISAR", "FALLA": "❌ FALLA"}
        st.dataframe(
            calidad.assign(estado=calidad.estado.map(icono).fillna(calidad.estado))
                   [["estado", "regla", "dimension", "severidad", "filas_incumplen",
                     "filas_evaluadas", "descripcion"]],
            hide_index=True, width="stretch",
            column_config={"filas_incumplen": st.column_config.NumberColumn(format="localized"),
                           "filas_evaluadas": st.column_config.NumberColumn(format="localized")})
