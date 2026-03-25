
import argparse
import os
import sys
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime

# ── Rutas del proyecto ────────────────────────────────────────
# __file__ apunta a pipeline.py; dirname sube a src/
# sys.path.insert permite importar los módulos locales (config, extract, etc.)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT_DIR)

from config import DATA_RAW, DATA_PROCESSED, SIMILARITY_THRESHOLD
from extract.catalogos import cargar_cuoc, cargar_divipola
from transform.imputar_ocupacion    import ImputadorOcupacion
from transform.homologar_municipio  import HomologadorMunicipio
from transform.normalizar_educacion import normalizar_nivel_educativo
from load.guardar_resultados import (
    guardar_excel,
    guardar_excel_analisis,
    guardar_reporte_calidad,
    guardar_resultados_finales,
)


# ─────────────────────────────────────────────────────────────
# Función: leer_fuente
# ─────────────────────────────────────────────────────────────
# Lee el archivo de datos crudos (xlsx o csv) y retorna un
# DataFrame de pandas. Usa dtype=str para que todos los campos
# se traten como texto y evitar que códigos como '05001'
# se conviertan en números (perdiendo el cero inicial).
# ─────────────────────────────────────────────────────────────
def leer_fuente(ruta: str) -> pd.DataFrame:
    ext = os.path.splitext(ruta)[1].lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        df = pd.read_excel(ruta, dtype=str)
    elif ext == ".csv":
        # sep=None con engine="python" detecta automáticamente el separador
        df = pd.read_csv(ruta, sep=None, engine="python", dtype=str)
    else:
        raise ValueError(f"Formato no soportado: {ext}")
    print(f"[Extract] Leídos {len(df)} registros desde: {ruta}")
    return df


# ─────────────────────────────────────────────────────────────
# Función: ejecutar_pipeline
# ─────────────────────────────────────────────────────────────
# Función principal que ejecuta el pipeline completo.
# Recibe parámetros configurables (nombres de columnas, umbral,
# formato de salida) con valores por defecto definidos en config.py
# ─────────────────────────────────────────────────────────────
def ejecutar_pipeline(
    ruta_input:    str   = "data/raw/Data_Proyecto.xlsx",
    col_ocupacion: str   = "ocupacion",        # columna de ocupación en el archivo fuente
    col_municipio: str   = "municipio",        # columna de municipio en el archivo fuente
    col_educacion: str   = "nivel_escolaridad",# columna de nivel educativo en el archivo fuente
    umbral:        float = SIMILARITY_THRESHOLD, # similitud mínima para aceptar imputación CUOC
    formato:       str   = "excel",
    nombre_salida: str   = "Resultados estandarizacion",
) -> pd.DataFrame:

    # Diccionario que acumula todas las métricas de calidad y tiempo
    metricas = {"fecha_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    t_total  = time.time()  # marca de tiempo de inicio total

    # ══════════════════════════════════════════════════════════
    # FASE 1 — EXTRACT
    # Objetivo: cargar todos los datos necesarios para el proceso
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("PROYECTO: Estandarización Buscadores de Empleo")
    print("FASE 1 — EXTRACT")
    print("=" * 60)

    t0 = time.time()

    # Paso 1.1 — Leer el archivo fuente con los buscadores de empleo
    df = leer_fuente(ruta_input)

    # Paso 1.2 — Cargar el catálogo CUOC 2025 del DANE
    # Solo se cargan las denominaciones (nivel más específico, código con punto)
    # Ejemplo: '03221.015' = Chef de cocina
    df_cuoc = cargar_cuoc()

    # Paso 1.3 — Cargar el catálogo DIVIPOLA (División Político-Administrativa)
    # Solo cabeceras municipales (tipo CM) para tener un registro único por municipio
    # Incluye longitud y latitud para análisis geográfico
    df_divipo = cargar_divipola()

    metricas["t_extract_seg"]            = round(time.time() - t0, 2)
    metricas["registros_entrada"]        = len(df)
    metricas["ocupaciones_unicas_antes"] = df[col_ocupacion].nunique()

    print(f"  CUOC cargada:     {len(df_cuoc)} denominaciones")
    print(f"  DIVIPOLA cargada: {len(df_divipo)} municipios")
    print(f"  Tiempo Extract:   {metricas['t_extract_seg']}s")

    # ══════════════════════════════════════════════════════════
    # FASE 2 — TRANSFORM
    # Objetivo: estandarizar las tres variables críticas
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("FASE 2 — TRANSFORM")
    print("=" * 60)

    # ── Paso 2a — Imputación de ocupación con CUOC ────────────
    # Técnica: Sentence Embeddings multilingüe + similitud coseno
    # El modelo convierte cada texto de ocupación en un vector numérico
    # y busca el vector más cercano en el catálogo CUOC.
    # Solo se acepta la imputación si la similitud supera el umbral definido.
    # La primera ejecución descarga el modelo (~470MB) y guarda embeddings
    # en caché para que las siguientes ejecuciones sean mucho más rápidas.
    t0 = time.time()
    if col_ocupacion in df.columns:
        print("\n[2a] Imputación CUOC — Sentence Embeddings + similitud coseno ...")
        imp = ImputadorOcupacion(df_cuoc)
        df  = imp.imputar(df, columna_ocupacion=col_ocupacion, umbral=umbral)
        metricas["t_cuoc_seg"] = round(time.time() - t0, 2)

        # KPI 1: % de registros con similitud >= umbral (0.80)
        mask_80 = df["cuoc_imputado"].astype(bool) & (df["cuoc_similitud"] >= 0.80)
        metricas["kpi1_pct_cuoc_homologado"]  = round(mask_80.sum() / len(df) * 100, 2)
        metricas["kpi1_n_homologados"]         = int(mask_80.sum())

        # KPI 2: reducción de categorías únicas de ocupación
        # Antes: N ocupaciones distintas en texto libre (ej. 'CHEF', 'Chef de cocina', 'COCINERO')
        # Después: N denominaciones CUOC únicas (ej. 'Chef de cocina')
        metricas["ocupaciones_unicas_despues"] = int(df["cuoc_nombre_ocupacion"].nunique())
        metricas["kpi2_pct_reduccion_categorias"] = round(
            (metricas["ocupaciones_unicas_antes"] - metricas["ocupaciones_unicas_despues"])
            / metricas["ocupaciones_unicas_antes"] * 100, 2
        )
        print(f"  Tiempo CUOC: {metricas['t_cuoc_seg']}s")
        print(f"  KPI 1 — Homologados CUOC >= 0.80: {metricas['kpi1_pct_cuoc_homologado']}%")
        print(f"  KPI 2 — Reducción categorías: {metricas['ocupaciones_unicas_antes']} → "
              f"{metricas['ocupaciones_unicas_despues']} ({metricas['kpi2_pct_reduccion_categorias']}%)")

    # ── Paso 2b — Homologación de municipio con DIVIPOLA ──────
    # Estrategia en 3 capas:
    #   1. Alias manual  → CALI = SANTIAGO DE CALI
    #   2. Exacto        → nombre normalizado coincide directamente
    #   3. Fuzzy         → rapidfuzz encuentra la coincidencia más cercana
    # Agrega longitud y latitud para análisis geográfico en el EDA
    t0 = time.time()
    if col_municipio in df.columns:
        print("\n[2b] Homologación DIVIPOLA — Alias + exacto + fuzzy matching ...")
        hom = HomologadorMunicipio(df_divipo)
        df  = hom.homologar(df, columna_municipio=col_municipio)
        metricas["t_divipola_seg"] = round(time.time() - t0, 2)

        # KPI 3: % de municipios validados (cualquier método que encuentre match)
        validados = df["divipola_metodo"].isin(["exacto", "alias", "fuzzy"]).sum()
        metricas["kpi3_pct_municipios_validados"] = round(validados / len(df) * 100, 2)
        metricas["kpi3_n_validados"]               = int(validados)
        print(f"  Tiempo DIVIPOLA: {metricas['t_divipola_seg']}s")
        print(f"  KPI 3 — Municipios validados: {metricas['kpi3_pct_municipios_validados']}%")

    # ── Paso 2c — Normalización de nivel de escolaridad ───────
    # Estrategia:
    #   1. Diccionario de equivalencias exactas
    #      (ej. 'Media(10-13)' → 'Bachiller')
    #   2. Coincidencia parcial (el texto contiene una clave del diccionario)
    #   3. Fuzzy matching si las anteriores no encuentran resultado
    # Los 11 niveles oficiales están definidos en config.py
    t0 = time.time()
    if col_educacion in df.columns:
        print("\n[2c] Normalización nivel escolaridad — Diccionario + fuzzy ...")
        df = normalizar_nivel_educativo(df, columna=col_educacion)
        metricas["t_educacion_seg"] = round(time.time() - t0, 2)

        # KPI 4: % de registros normalizados (cualquier método que encuentre resultado)
        normalizados = (df["nivel_educativo_metodo"] != "no_clasificado").sum()
        metricas["kpi4_pct_educacion_normalizada"] = round(normalizados / len(df) * 100, 2)
        metricas["kpi4_n_normalizados"]             = int(normalizados)
        print(f"  Tiempo Educación: {metricas['t_educacion_seg']}s")
        print(f"  KPI 4 — Nivel educativo normalizado: {metricas['kpi4_pct_educacion_normalizada']}%")

    # ── Paso 2d — Post-transformación ─────────────────────────
    # Reemplaza las columnas originales con los valores estandarizados
    # y agrega la columna de rango de edad para segmentación
    print("\n[2d] Post-transformación — Reemplazar columnas y calcular rango de edad ...")

    # Reemplazar 'ocupacion' por la denominación CUOC imputada
    # Solo si el registro fue imputado (supera el umbral de similitud)
    if "cuoc_nombre_denominacion" in df.columns and "cuoc_imputado" in df.columns:
        mask = df["cuoc_imputado"].astype(bool)
        df["ocupacion"] = np.where(mask, df["cuoc_nombre_denominacion"], df["ocupacion"])

    # Reemplazar 'nivel_escolaridad' por el nivel oficial normalizado
    if "nivel_educativo_norm" in df.columns:
        df["nivel_escolaridad"] = df["nivel_educativo_norm"]

    # Calcular rango de edad según los rangos institucionales definidos
    df["edad"] = pd.to_numeric(df["edad"], errors="coerce")
    def rango_edad(edad):
        if pd.isna(edad):  return None
        if edad <= 28:     return "18 a 28 años"
        elif edad <= 40:   return "29 a 40 años"
        elif edad <= 50:   return "41 a 50 años"
        else:              return "51 en adelante"
    df["rango_edad"] = df["edad"].apply(rango_edad)
    print(f"  Distribución rango edad: {df['rango_edad'].value_counts().to_dict()}")

    # Tiempo total de la fase transform
    metricas["t_transform_seg"] = round(
        metricas.get("t_cuoc_seg", 0) +
        metricas.get("t_divipola_seg", 0) +
        metricas.get("t_educacion_seg", 0), 2
    )

    # ══════════════════════════════════════════════════════════
    # FASE 3 — LOAD
    # Objetivo: persistir los resultados en dos versiones
    # ══════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("FASE 3 — LOAD")
    print("=" * 60)

    t0 = time.time()

    # Paso 3.1 — Guardar versión completa (todas las columnas, para auditoría)
    guardar_excel(df, nombre_archivo="Resultados estandarizacion")

    # Paso 3.2 — Guardar versión de análisis (solo columnas necesarias para el EDA)
    guardar_excel_analisis(df, nombre_archivo="Base de buscadores estandarizada")

    # Paso 3.3 — Guardar reporte de calidad con resúmenes por variable
    guardar_reporte_calidad(df)

    # Paso 3.4 — Guardar versión final en data/resultados/ (se versiona en Git)
    guardar_resultados_finales(df)

    metricas["t_load_seg"]       = round(time.time() - t0, 2)
    metricas["t_total_seg"]      = round(time.time() - t_total, 2)
    metricas["registros_salida"] = len(df)

    # Paso 3.4 — Guardar métricas en JSON para que el EDA las pueda leer
    os.makedirs(DATA_PROCESSED, exist_ok=True)
    ruta_m = os.path.join(DATA_PROCESSED, "metricas_pipeline.json")
    with open(ruta_m, "w", encoding="utf-8") as f:
        json.dump(metricas, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  Tiempo total    : {metricas['t_total_seg']}s")
    print(f"    Extract       : {metricas.get('t_extract_seg', 0)}s")
    print(f"    Transform     : {metricas.get('t_transform_seg', 0)}s")
    print(f"      CUOC        : {metricas.get('t_cuoc_seg', 0)}s")
    print(f"      DIVIPOLA    : {metricas.get('t_divipola_seg', 0)}s")
    print(f"      Educación   : {metricas.get('t_educacion_seg', 0)}s")
    print(f"    Load          : {metricas.get('t_load_seg', 0)}s")
    print(f"  Métricas guardadas: {ruta_m}")
    print("✅ Pipeline ETL finalizado correctamente.")
    return df


# ─────────────────────────────────────────────────────────────
# Bloque principal
# ─────────────────────────────────────────────────────────────
# argparse permite pasar parámetros desde la terminal.
# Si no se pasan argumentos, se usan los valores por defecto.
# Ejemplo: python src/pipeline.py --umbral 0.85 --formato csv
# ─────────────────────────────────────────────────────────────
def _parse_args():
    p = argparse.ArgumentParser(
        description="Estandarización Buscadores de Empleo — Pipeline ETL"
    )
    p.add_argument("--input",   default="data/raw/Data_Proyecto.xlsx",
                   help="Ruta al archivo de datos crudos")
    p.add_argument("--col-ocu", default="ocupacion",
                   help="Nombre de la columna de ocupación")
    p.add_argument("--col-mun", default="municipio",
                   help="Nombre de la columna de municipio")
    p.add_argument("--col-edu", default="nivel_escolaridad",
                   help="Nombre de la columna de nivel educativo")
    p.add_argument("--umbral",  default=SIMILARITY_THRESHOLD, type=float,
                   help="Umbral de similitud para imputación CUOC (0-1)")
    p.add_argument("--formato", default="excel",
                   choices=["excel","csv","parquet"],
                   help="Formato del archivo de salida")
    p.add_argument("--salida",  default="resultado_etl",
                   help="Nombre base del archivo de salida")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    ejecutar_pipeline(
        ruta_input    = args.input,
        col_ocupacion = args.col_ocu,
        col_municipio = args.col_mun,
        col_educacion = args.col_edu,
        umbral        = args.umbral,
        formato       = args.formato,
        nombre_salida = args.salida,
    )
