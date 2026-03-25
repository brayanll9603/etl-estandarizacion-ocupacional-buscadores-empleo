import os, sys, glob, base64, json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DATA_PROCESSED

try:
    from wordcloud import WordCloud
    _HAS_WORDCLOUD = True
except ImportError:
    _HAS_WORDCLOUD = False

COLOR_BAR  = "#1D6FA5"
FIG_DPI    = 130
OUTPUT_DIR = os.path.join(DATA_PROCESSED, "graficos")
os.makedirs(OUTPUT_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", font_scale=1.0)
plt.rcParams.update({"figure.dpi": FIG_DPI,
                     "axes.spines.right": False,
                     "axes.spines.top": False})


def fig_a_base64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def guardar(fig, nombre):
    fig.savefig(os.path.join(OUTPUT_DIR, nombre), bbox_inches="tight")
    plt.close(fig)
    print(f"  Guardado: {nombre}")


# ─────────────────────────────────────────────────────────────
# Cargar datos
# ─────────────────────────────────────────────────────────────

def cargar_datos():
    # Buscar primero en resultados/ (versión final), luego en processed/ (con timestamp)
    carpeta_resultados = os.path.join(os.path.dirname(DATA_PROCESSED), "resultados")
    archivos_a = glob.glob(os.path.join(carpeta_resultados, "Base de buscadores estandarizada.xlsx"))
    if not archivos_a:
        archivos_a = glob.glob(os.path.join(DATA_PROCESSED, "Base de buscadores estandarizada*.xlsx"))
    archivos_c = glob.glob(os.path.join(DATA_PROCESSED, "Resultados estandarizacion*.xlsx"))
    if not archivos_a:
        raise FileNotFoundError("Ejecuta primero: python src/pipeline.py")

    df = pd.read_excel(max(archivos_a, key=os.path.getmtime))
    df_c = pd.read_excel(max(archivos_c, key=os.path.getmtime)) if archivos_c else None
    print(f"[EDA] {len(df)} registros cargados")

    # Métricas de tiempo del pipeline
    metricas = {}
    ruta_m = os.path.join(DATA_PROCESSED, "metricas_pipeline.json")
    if os.path.exists(ruta_m):
        with open(ruta_m, encoding="utf-8") as f:
            metricas = json.load(f)
    return df, df_c, metricas


# ─────────────────────────────────────────────────────────────
# Gráficos
# ─────────────────────────────────────────────────────────────

def g_nube(df):
    if not _HAS_WORDCLOUD:
        return None
    print("[EDA] Nube de ocupaciones...")
    freq = df["cuoc_nombre_ocupacion"].dropna().value_counts().to_dict()
    wc = WordCloud(width=1400, height=550, background_color="white",
                   colormap="Blues", max_words=80, collocations=False
                   ).generate_from_frequencies(freq)
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.imshow(wc, interpolation="bilinear"); ax.axis("off")
    img = fig_a_base64(fig); guardar(fig, "01_nube_ocupaciones.png")
    return img


def g_top_ocupaciones(df):
    print("[EDA] Top 15 ocupaciones...")
    top = (df.groupby("cuoc_nombre_ocupacion").size()
           .reset_index(name="n").sort_values("n", ascending=True).tail(15))
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(top["cuoc_nombre_ocupacion"], top["n"],
                   color=COLOR_BAR, edgecolor="white", height=0.7)
    for b in bars:
        ax.text(b.get_width()+15, b.get_y()+b.get_height()/2,
                f"{int(b.get_width()):,}", va="center", ha="left", fontsize=9)
    ax.set_title("Top 15 ocupaciones más frecuentes", fontsize=14, pad=15)
    ax.set_xlabel("Personas", fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "02_top_ocupaciones.png")
    return img


def g_tabla_ocupaciones(df):
    print("[EDA] Tabla Top 20 ocupaciones...")
    top = (df.groupby("cuoc_nombre_ocupacion").size()
           .reset_index(name="registros").sort_values("registros", ascending=False)
           .head(20).reset_index(drop=True))
    top.index += 1
    top["pct"] = (top["registros"]/len(df)*100).round(2).astype(str)+"%"
    fig, ax = plt.subplots(figsize=(13, 7)); ax.axis("off")
    t = ax.table(cellText=top[["cuoc_nombre_ocupacion","registros","pct"]].values,
                 colLabels=["Ocupación","Registros","%"],
                 cellLoc="left", loc="center", colWidths=[0.60,0.20,0.20])
    t.auto_set_font_size(False); t.set_fontsize(9); t.scale(1, 1.6)
    for (r,c), cell in t.get_celld().items():
        if r==0: cell.set_facecolor("#1D6FA5"); cell.set_text_props(color="white", fontweight="bold")
        elif r%2==0: cell.set_facecolor("#EBF4FB")
        cell.set_edgecolor("white")
    img = fig_a_base64(fig); guardar(fig, "03_tabla_ocupaciones.png")
    return img


def g_areas(df):
    print("[EDA] Áreas de cualificación...")
    areas = (df["cuoc_area_cualificacion"].dropna().value_counts()
             .reset_index(name="n").rename(columns={"cuoc_area_cualificacion":"area"})
             .sort_values("n", ascending=True))
    colores = plt.cm.Blues([0.35 + 0.65*(i/len(areas)) for i in range(len(areas))])
    fig, ax = plt.subplots(figsize=(12, max(6, len(areas)*0.45)))
    bars = ax.barh(areas["area"], areas["n"], color=colores, edgecolor="white", height=0.7)
    for b in bars:
        ax.text(b.get_width()+10, b.get_y()+b.get_height()/2,
                f"{int(b.get_width()):,}", va="center", ha="left", fontsize=8)
    ax.set_title("Registros por área de cualificación CUOC", fontsize=14, pad=15)
    ax.set_xlabel("Personas", fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.tick_params(axis="y", labelsize=8)
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "04_areas_cualificacion.png")
    return img


def g_edu_edad(df):
    print("[EDA] Escolaridad vs edad...")
    oe = ["Ninguna","Básica primaria","Básica secundaria","Bachiller",
          "Técnico laboral","Técnico profesional","Tecnólogo",
          "Universitario","Especialización","Magíster","Doctorado"]
    oa = ["18 a 28 años","29 a 40 años","41 a 50 años","51 en adelante"]
    pivot = (df.groupby(["nivel_escolaridad","rango_edad"]).size()
             .reset_index(name="n")
             .pivot(index="nivel_escolaridad", columns="rango_edad", values="n").fillna(0))
    pivot = pivot.reindex([e for e in oe if e in pivot.index])
    pivot = pivot.reindex(columns=[e for e in oa if e in pivot.columns])
    fig, ax = plt.subplots(figsize=(13, 6))
    pivot.plot(kind="bar", ax=ax, colormap="Blues", edgecolor="white", width=0.75)
    ax.set_title("Nivel de escolaridad por rango de edad", fontsize=14, pad=15)
    ax.set_xlabel("Nivel de escolaridad", fontsize=11)
    ax.set_ylabel("Personas", fontsize=11)
    ax.tick_params(axis="x", rotation=35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.legend(title="Rango de edad", bbox_to_anchor=(1.01,1), loc="upper left")
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "05_escolaridad_vs_edad.png")
    return img


def g_edu_municipio(df):
    print("[EDA] Escolaridad vs municipio...")
    oe = ["Ninguna","Básica primaria","Básica secundaria","Bachiller",
          "Técnico laboral","Técnico profesional","Tecnólogo",
          "Universitario","Especialización","Magíster","Doctorado"]
    top10 = df["divipola_nombre_municipio"].value_counts().head(10).index.tolist()
    pivot = (df[df["divipola_nombre_municipio"].isin(top10)]
             .groupby(["divipola_nombre_municipio","nivel_escolaridad"]).size()
             .reset_index(name="n")
             .pivot(index="divipola_nombre_municipio", columns="nivel_escolaridad", values="n").fillna(0))
    pivot = pivot.reindex(columns=[e for e in oe if e in pivot.columns])
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]
    fig, ax = plt.subplots(figsize=(14, 7))
    pivot.plot(kind="bar", stacked=True, ax=ax, colormap="Blues", edgecolor="white", width=0.75)
    ax.set_title("Nivel de escolaridad por municipio (Top 10)", fontsize=14, pad=15)
    ax.set_xlabel("Municipio", fontsize=11); ax.set_ylabel("Personas", fontsize=11)
    ax.tick_params(axis="x", rotation=35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.legend(title="Nivel escolaridad", bbox_to_anchor=(1.01,1), loc="upper left", fontsize=8)
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "06_escolaridad_municipio.png")
    return img


# P4 — Relación nivel educativo vs grupo ocupacional
def g_edu_ocupacion(df):
    print("[EDA] P4 Educación vs ocupación (Top 10)...")
    oe = ["Ninguna","Básica primaria","Básica secundaria","Bachiller",
          "Técnico laboral","Técnico profesional","Tecnólogo",
          "Universitario","Especialización","Magíster","Doctorado"]
    top10 = df["cuoc_nombre_ocupacion"].value_counts().head(10).index.tolist()
    pivot = (df[df["cuoc_nombre_ocupacion"].isin(top10)]
             .groupby(["cuoc_nombre_ocupacion","nivel_escolaridad"]).size()
             .reset_index(name="n")
             .pivot(index="cuoc_nombre_ocupacion", columns="nivel_escolaridad", values="n").fillna(0))
    pivot = pivot.reindex(columns=[e for e in oe if e in pivot.columns])
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]
    fig, ax = plt.subplots(figsize=(14, 7))
    pivot.plot(kind="bar", stacked=True, ax=ax, colormap="Blues", edgecolor="white", width=0.75)
    ax.set_title("Nivel educativo por ocupación (Top 10 ocupaciones)", fontsize=14, pad=15)
    ax.set_xlabel("Ocupación", fontsize=10); ax.set_ylabel("Personas", fontsize=11)
    ax.tick_params(axis="x", rotation=40, labelsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.legend(title="Nivel escolaridad", bbox_to_anchor=(1.01,1), loc="upper left", fontsize=8)
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "07_edu_vs_ocupacion.png")
    return img


# P6 — Ocupaciones con mayor heterogeneidad en nivel educativo
def g_heterogeneidad(df):
    print("[EDA] P6 Heterogeneidad educativa por ocupación...")
    het = (df.groupby("cuoc_nombre_ocupacion")["nivel_escolaridad"]
           .nunique().reset_index(name="niveles_distintos")
           .merge(df.groupby("cuoc_nombre_ocupacion").size().reset_index(name="total"))
           .query("total >= 50")
           .sort_values("niveles_distintos", ascending=True)
           .tail(15))
    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(het["cuoc_nombre_ocupacion"], het["niveles_distintos"],
                   color=COLOR_BAR, edgecolor="white", height=0.7)
    for b in bars:
        ax.text(b.get_width()+0.05, b.get_y()+b.get_height()/2,
                str(int(b.get_width())), va="center", ha="left", fontsize=9)
    ax.set_title("Ocupaciones con mayor diversidad de niveles educativos", fontsize=13, pad=15)
    ax.set_xlabel("Niveles educativos distintos", fontsize=11)
    ax.tick_params(axis="y", labelsize=9)
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "08_heterogeneidad_educativa.png")
    return img


# P7 — Combinaciones municipio + ocupación con mayor concentración
def g_concentracion(df):
    print("[EDA] P7 Concentración municipio + ocupación...")
    top = (df.groupby(["divipola_nombre_municipio","cuoc_nombre_ocupacion"])
           .size().reset_index(name="n")
           .sort_values("n", ascending=False).head(15))
    top["combo"] = top["divipola_nombre_municipio"].str[:15] + " / " + top["cuoc_nombre_ocupacion"].str[:25]
    top = top.sort_values("n", ascending=True)
    fig, ax = plt.subplots(figsize=(13, 8))
    bars = ax.barh(top["combo"], top["n"], color=COLOR_BAR, edgecolor="white", height=0.7)
    for b in bars:
        ax.text(b.get_width()+5, b.get_y()+b.get_height()/2,
                f"{int(b.get_width()):,}", va="center", ha="left", fontsize=9)
    ax.set_title("Top 15 combinaciones municipio + ocupación", fontsize=13, pad=15)
    ax.set_xlabel("Personas", fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.tick_params(axis="y", labelsize=8)
    plt.tight_layout()
    img = fig_a_base64(fig); guardar(fig, "09_concentracion_municipio_ocupacion.png")
    return img


# ─────────────────────────────────────────────────────────────
# Dashboard HTML
# ─────────────────────────────────────────────────────────────

def generar_dashboard(df, imgs, metricas):
    print("[EDA] Generando dashboard HTML...")

    total    = len(df)
    n_mun    = df["divipola_nombre_municipio"].nunique()
    n_ocu    = df["cuoc_nombre_ocupacion"].nunique()
    top_ocu  = df["cuoc_nombre_ocupacion"].value_counts().index[0]
    top_mun  = df["divipola_nombre_municipio"].value_counts().index[0]
    top_edu  = df["nivel_escolaridad"].value_counts().index[0]
    top_area = df["cuoc_area_cualificacion"].dropna().value_counts().index[0]

    # KPIs de negocio
    kpi1  = metricas.get("kpi1_pct_cuoc_homologado", "N/A")
    kpi2  = metricas.get("kpi2_pct_reduccion_categorias", "N/A")
    k2_a  = metricas.get("ocupaciones_unicas_antes", "")
    k2_d  = metricas.get("ocupaciones_unicas_despues", "")
    kpi3  = metricas.get("kpi3_pct_municipios_validados", "N/A")
    kpi4  = metricas.get("kpi4_pct_educacion_normalizada", "N/A")

    # KPIs de tiempo
    t_total = metricas.get("t_total_seg", "N/A")
    t_ext   = metricas.get("t_extract_seg", "N/A")
    t_tra   = metricas.get("t_transform_seg", "N/A")
    t_load  = metricas.get("t_load_seg", "N/A")
    t_cuoc  = metricas.get("t_cuoc_seg", "N/A")
    t_div   = metricas.get("t_divipola_seg", "N/A")
    t_edu   = metricas.get("t_educacion_seg", "N/A")
    fecha   = metricas.get("fecha_ejecucion", "")

    def card(titulo, desc, key, ancho="full"):
        img64 = imgs.get(key)
        if not img64: return ""
        return f"""
        <div class="card card-{ancho}">
          <div class="card-header"><h3>{titulo}</h3><p>{desc}</p></div>
          <img src="data:image/png;base64,{img64}" alt="{titulo}">
        </div>"""

    def seccion(titulo, contenido):
        return f"""
        <div class="seccion">
          <h2>{titulo}</h2>
          <div class="cards">{contenido}</div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Estandarización Buscadores de Empleo — Dashboard EDA</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
       background:#f0f4f8;color:#2d3748}}
  header{{background:linear-gradient(135deg,#1D6FA5,#0d4f7a);
          color:white;padding:36px 40px}}
  header h1{{font-size:24px;font-weight:700;margin-bottom:6px}}
  header p{{font-size:13px;opacity:.85}}
  .bloque{{padding:24px 40px 8px}}
  .bloque h2{{font-size:16px;font-weight:600;color:#2d3748;
             border-left:4px solid #1D6FA5;padding-left:12px;margin-bottom:16px}}
  /* KPI grids */
  .kpi-grid{{display:grid;gap:14px;margin-bottom:8px}}
  .kpi-g4{{grid-template-columns:repeat(4,1fr)}}
  .kpi-g2{{grid-template-columns:repeat(2,1fr)}}
  .kpi-g3{{grid-template-columns:repeat(3,1fr)}}
  .kpi{{background:white;border-radius:12px;padding:18px 20px;
        box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:3px solid #1D6FA5}}
  .kpi.green{{border-top-color:#2f855a}}
  .kpi.amber{{border-top-color:#c05621}}
  .kpi.purple{{border-top-color:#553c9a}}
  .kpi .valor{{font-size:28px;font-weight:700;color:#1D6FA5;line-height:1}}
  .kpi.green .valor{{color:#2f855a}}
  .kpi.amber .valor{{color:#c05621}}
  .kpi.purple .valor{{color:#553c9a}}
  .kpi .label{{font-size:12px;color:#718096;margin-top:6px;font-weight:500}}
  .kpi .sub{{font-size:11px;color:#a0aec0;margin-top:3px}}
  .kpi .meta{{font-size:11px;color:#718096;margin-top:4px;
              background:#f7fafc;padding:4px 8px;border-radius:6px}}
  /* Cards */
  .seccion{{padding:16px 40px 28px}}
  .seccion>h2{{font-size:16px;font-weight:600;color:#2d3748;
              border-left:4px solid #1D6FA5;padding-left:12px;margin-bottom:16px}}
  .cards{{display:grid;gap:20px;grid-template-columns:repeat(auto-fit,minmax(520px,1fr))}}
  .card{{background:white;border-radius:12px;
         box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:hidden}}
  .card-full{{grid-column:1/-1}}
  .card-header{{padding:16px 20px 10px}}
  .card-header h3{{font-size:13px;font-weight:600;color:#2d3748}}
  .card-header p{{font-size:12px;color:#718096;margin-top:3px}}
  .card img{{width:100%;display:block}}
  footer{{text-align:center;padding:24px;font-size:12px;color:#a0aec0;
          border-top:1px solid #e2e8f0;margin-top:8px}}
  @media(max-width:900px){{
    .kpi-g4,.kpi-g3{{grid-template-columns:repeat(2,1fr)}}
    .kpi-g2{{grid-template-columns:1fr}}
    .cards{{grid-template-columns:1fr}}
  }}
</style>
</head>
<body>

<header>
  <h1>Estandarización Buscadores de Empleo</h1>
  <p>Dashboard de análisis exploratorio &middot; {total:,} registros procesados &middot; Última ejecución: {fecha}</p>
</header>

<!-- Resumen general -->
<div class="bloque">
  <h2>Resumen general</h2>
  <div class="kpi-grid kpi-g4">
    <div class="kpi"><div class="valor">{total:,}</div><div class="label">Total registros</div></div>
    <div class="kpi"><div class="valor">{n_mun}</div><div class="label">Municipios</div><div class="sub">Top: {top_mun}</div></div>
    <div class="kpi"><div class="valor">{n_ocu}</div><div class="label">Ocupaciones únicas CUOC</div></div>
    <div class="kpi"><div class="valor" style="font-size:15px">{top_edu}</div><div class="label">Nivel educativo más frecuente</div></div>
  </div>
</div>

<!-- KPIs de calidad -->
<div class="bloque">
  <h2>KPIs de calidad del proceso</h2>
  <div class="kpi-grid kpi-g4">
    <div class="kpi green">
      <div class="valor">{kpi1}%</div>
      <div class="label">KPI 1 — Homologación CUOC</div>
      <div class="meta">Registros con similitud ≥ 0.80 · Total: {metricas.get('kpi1_n_homologados','N/A'):,}</div>
    </div>
    <div class="kpi green">
      <div class="valor">{kpi2}%</div>
      <div class="label">KPI 2 — Reducción de categorías</div>
      <div class="meta">{k2_a} categorías → {k2_d} tras homologación</div>
    </div>
    <div class="kpi green">
      <div class="valor">{kpi3}%</div>
      <div class="label">KPI 3 — Municipios validados</div>
      <div class="meta">Contra catálogo oficial DIVIPOLA · {metricas.get('kpi3_n_validados','N/A'):,} registros</div>
    </div>
    <div class="kpi green">
      <div class="valor">{kpi4}%</div>
      <div class="label">KPI 4 — Nivel educativo normalizado</div>
      <div class="meta">Aplicando diccionario de equivalencias · {metricas.get('kpi4_n_normalizados','N/A'):,} registros</div>
    </div>
  </div>
</div>

<!-- KPIs de tiempo -->
<div class="bloque">
  <h2>KPIs de rendimiento del pipeline</h2>
  <div class="kpi-grid kpi-g3">
    <div class="kpi purple">
      <div class="valor">{t_total}s</div>
      <div class="label">Tiempo total de ejecución</div>
      <div class="meta">Extract: {t_ext}s &nbsp;|&nbsp; Transform: {t_tra}s &nbsp;|&nbsp; Load: {t_load}s</div>
    </div>
    <div class="kpi amber">
      <div class="valor">{t_cuoc}s</div>
      <div class="label">Tiempo imputación CUOC</div>
      <div class="meta">Sentence Embeddings multilingüe + similitud coseno</div>
    </div>
    <div class="kpi amber">
      <div class="valor">{t_div}s</div>
      <div class="label">Tiempo homologación DIVIPOLA</div>
      <div class="meta">Alias + coincidencia exacta + fuzzy matching</div>
    </div>
  </div>
</div>

<!-- Ocupaciones -->
{seccion("Ocupaciones — análisis descriptivo",
    card("Nube de ocupaciones","Tamaño proporcional a la frecuencia de cada ocupación CUOC","nube","full") +
    card("Top 15 ocupaciones más frecuentes","Agrupado por nombre de ocupación CUOC","top_ocu","full") +
    card("Top 20 ocupaciones — tabla de frecuencias","Cantidad y porcentaje sobre el total","tabla","full")
)}

<!-- Áreas de cualificación -->
{seccion("Áreas de cualificación CUOC",
    card("Distribución por área de cualificación","Según clasificación oficial CUOC 2025","areas","full")
)}

<!-- Preguntas estratégicas -->
{seccion("Preguntas estratégicas",
    card("P4 — Nivel educativo por ocupación (Top 10)",
         "¿Qué relación existe entre nivel educativo y grupo ocupacional homologado?","edu_ocu") +
    card("P6 — Heterogeneidad educativa por ocupación",
         "¿Qué ocupaciones presentan mayor diversidad en nivel educativo? (mín. 50 registros)","het") +
    card("P7 — Concentración municipio + ocupación",
         "¿Qué combinaciones de municipio y ocupación presentan mayor concentración?","conc","full")
)}

<!-- Escolaridad -->
{seccion("Escolaridad",
    card("Escolaridad por rango de edad",
         "P5 — ¿Cómo se distribuye el nivel educativo por grupo etario?","edu_edad") +
    card("Escolaridad por municipio (Top 10)",
         "P5 — ¿Qué nivel educativo predomina en cada municipio validado?","edu_mun")
)}

<footer>Proyecto Estandarización Buscadores de Empleo &middot; Pipeline ETL &middot; Generado automáticamente</footer>
</body>
</html>"""

    ruta = os.path.join(OUTPUT_DIR, "dashboard_eda.html")
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Dashboard guardado: {ruta}")
    return ruta


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df, df_c, metricas = cargar_datos()

    imgs = {}
    imgs["nube"]    = g_nube(df)
    imgs["top_ocu"] = g_top_ocupaciones(df)
    imgs["tabla"]   = g_tabla_ocupaciones(df)
    imgs["areas"]   = g_areas(df)
    imgs["edu_edad"]= g_edu_edad(df)
    imgs["edu_mun"] = g_edu_municipio(df)
    imgs["edu_ocu"] = g_edu_ocupacion(df)
    imgs["het"]     = g_heterogeneidad(df)
    imgs["conc"]    = g_concentracion(df)

    ruta = generar_dashboard(df, imgs, metricas)

    print(f"\n✅ EDA completado.")
    print(f"   Gráficos : {OUTPUT_DIR}")
    print(f"   Dashboard: {ruta}")
    print(f"\n   Abre con: start {ruta}")
