"""
load/guardar_resultados.py
───────────────────────────────────────────────────────────────
Proyecto : Estandarización Buscadores de Empleo
Archivo  : load/guardar_resultados.py
Propósito: Persiste los resultados del pipeline en dos versiones
           y genera un reporte de calidad del proceso.

Archivos generados en data/processed/:
  resultado_etl_FECHA.xlsx         → versión completa (auditoría)
  resultado_etl_analisis_FECHA.xlsx → versión reducida (EDA)
  reporte_calidad_FECHA.xlsx        → resúmenes por variable
"""

import os
import pandas as pd
from datetime import datetime
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_PROCESSED

# Columnas seleccionadas para el archivo de análisis
# Se excluyen columnas auxiliares del proceso (metodo, score, imputado)
# que son útiles para auditoría pero no para el análisis de negocio
COLUMNAS_ANALISIS = [
    "ID",
    "ocupacion",                    # denominación CUOC imputada
    "cuoc_codigo_ocupacion",        # código de 5 dígitos para cruce con otras fuentes
    "cuoc_nombre_ocupacion",        # agrupación por ocupación (nivel medio CUOC)
    "cuoc_area_cualificacion",      # área de cualificación principal
    "cuoc_similitud",               # score de similitud para validación
    "nivel_escolaridad",            # nivel normalizado a categorías oficiales
    "edad",
    "rango_edad",                   # segmentación etaria institucional
    "genero",
    "municipio",                    # municipio original tal como fue digitado
    "divipola_nombre_municipio",    # nombre oficial según DIVIPOLA
    "divipola_nombre_departamento", # departamento oficial
    "divipola_codigo_municipio",    # código de 5 dígitos para análisis geográfico
    "longitud",                     # coordenada geográfica para mapas
    "latitud",                      # coordenada geográfica para mapas
]


# ─────────────────────────────────────────────────────────────
# Función: guardar_excel (versión completa)
# ─────────────────────────────────────────────────────────────
# Guarda todas las columnas del DataFrame procesado.
# Útil para auditoría: incluye similitudes, métodos usados,
# columnas auxiliares del proceso ETL.
# ─────────────────────────────────────────────────────────────
def guardar_excel(
    df: pd.DataFrame,
    nombre_archivo: str = "Resultados estandarizacion",
    carpeta: str = DATA_PROCESSED,
    incluir_timestamp: bool = True,
) -> str:
    os.makedirs(carpeta, exist_ok=True)
    ts   = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if incluir_timestamp else ""
    ruta = os.path.join(carpeta, f"{nombre_archivo}{ts}.xlsx")

    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="datos_completos")

    print(f"[Load] Excel completo guardado: {ruta}  ({len(df)} filas × {len(df.columns)} cols)")
    return ruta


# ─────────────────────────────────────────────────────────────
# Función: guardar_excel_analisis (versión reducida)
# ─────────────────────────────────────────────────────────────
# Guarda solo las columnas necesarias para el EDA y análisis.
# Más liviano y fácil de manejar para analistas de negocio.
# ─────────────────────────────────────────────────────────────
def guardar_excel_analisis(
    df: pd.DataFrame,
    nombre_archivo: str = "Base de buscadores estandarizada",
    carpeta: str = DATA_PROCESSED,
    incluir_timestamp: bool = True,
) -> str:
    os.makedirs(carpeta, exist_ok=True)
    ts   = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if incluir_timestamp else ""
    ruta = os.path.join(carpeta, f"{nombre_archivo}{ts}.xlsx")

    # Solo incluir columnas que existan en el DataFrame
    # (por si alguna transformación no se ejecutó)
    cols       = [c for c in COLUMNAS_ANALISIS if c in df.columns]
    df_analisis = df[cols]

    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df_analisis.to_excel(writer, index=False, sheet_name="datos_analisis")

    print(f"[Load] Excel análisis guardado: {ruta}  ({len(df_analisis)} filas × {len(df_analisis.columns)} cols)")
    return ruta


# ─────────────────────────────────────────────────────────────
# Función: guardar_csv
# ─────────────────────────────────────────────────────────────
# Guarda en CSV con separador ';' y encoding utf-8-sig (BOM).
# El BOM (Byte Order Mark) permite que Excel en español abra
# el archivo correctamente sin problemas de caracteres especiales.
# ─────────────────────────────────────────────────────────────
def guardar_csv(
    df: pd.DataFrame,
    nombre_archivo: str = "Resultados estandarizacion",
    carpeta: str = DATA_PROCESSED,
    incluir_timestamp: bool = True,
) -> str:
    os.makedirs(carpeta, exist_ok=True)
    ts   = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if incluir_timestamp else ""
    ruta = os.path.join(carpeta, f"{nombre_archivo}{ts}.csv")
    df.to_csv(ruta, index=False, sep=";", encoding="utf-8-sig")
    print(f"[Load] CSV guardado: {ruta}")
    return ruta


# ─────────────────────────────────────────────────────────────
# Función: guardar_parquet
# ─────────────────────────────────────────────────────────────
# Parquet es un formato columnar eficiente para análisis de datos.
# Ocupa mucho menos espacio que Excel/CSV y es más rápido de leer.
# Ideal para integración con herramientas como Power BI o Spark.
# ─────────────────────────────────────────────────────────────
def guardar_parquet(
    df: pd.DataFrame,
    nombre_archivo: str = "Resultados estandarizacion",
    carpeta: str = DATA_PROCESSED,
    incluir_timestamp: bool = True,
) -> str:
    os.makedirs(carpeta, exist_ok=True)
    ts   = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if incluir_timestamp else ""
    ruta = os.path.join(carpeta, f"{nombre_archivo}{ts}.parquet")
    df.to_parquet(ruta, index=False)
    print(f"[Load] Parquet guardado: {ruta}")
    return ruta


# ─────────────────────────────────────────────────────────────
# Función: guardar_reporte_calidad
# ─────────────────────────────────────────────────────────────
# Genera un Excel con múltiples hojas de calidad:
#   resumen_ocupacion  → distribución de imputaciones CUOC
#   no_imputados       → registros que no superaron el umbral
#   resumen_municipio  → distribución de métodos DIVIPOLA
#   resumen_educacion  → distribución de niveles normalizados
# ─────────────────────────────────────────────────────────────
def guardar_reporte_calidad(df: pd.DataFrame, carpeta: str = DATA_PROCESSED) -> str:
    os.makedirs(carpeta, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta = os.path.join(carpeta, f"reporte_calidad_{ts}.xlsx")

    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:

        # Hoja 1: resumen de imputación CUOC
        if "cuoc_imputado" in df.columns:
            res_ocu = (
                df.groupby(["cuoc_imputado", "cuoc_nombre_ocupacion"])
                .size().reset_index(name="registros")
                .sort_values("registros", ascending=False)
            )
            res_ocu.to_excel(writer, index=False, sheet_name="resumen_ocupacion")

            # Hoja extra: registros NO imputados para revisión manual
            no_imp = df[~df["cuoc_imputado"].astype(bool)].copy()
            if len(no_imp):
                no_imp.to_excel(writer, index=False, sheet_name="no_imputados_ocupacion")

        # Hoja 2: resumen de homologación DIVIPOLA
        if "divipola_metodo" in df.columns:
            res_mun = (
                df.groupby(["divipola_metodo", "divipola_nombre_departamento"])
                .size().reset_index(name="registros")
                .sort_values("registros", ascending=False)
            )
            res_mun.to_excel(writer, index=False, sheet_name="resumen_municipio")

        # Hoja 3: resumen de normalización educativa
        if "nivel_educativo_metodo" in df.columns:
            res_edu = (
                df.groupby(["nivel_educativo_norm", "nivel_educativo_metodo"])
                .size().reset_index(name="registros")
                .sort_values("registros", ascending=False)
            )
            res_edu.to_excel(writer, index=False, sheet_name="resumen_educacion")

    print(f"[Load] Reporte de calidad guardado: {ruta}")
    return ruta


# ─────────────────────────────────────────────────────────────
# Función: guardar_resultados_finales
# ─────────────────────────────────────────────────────────────
# Guarda la versión final sin timestamp en data/resultados/.
# Esta carpeta SÍ se versiona en Git como entregable del proyecto.
# Se sobreescribe en cada ejecución para mantener siempre
# la versión más reciente como archivo fijo.
# ─────────────────────────────────────────────────────────────
def guardar_resultados_finales(
    df: pd.DataFrame,
    carpeta_base: str = None,
) -> str:
    if carpeta_base is None:
        carpeta_base = os.path.join(os.path.dirname(DATA_PROCESSED), "resultados")

    os.makedirs(carpeta_base, exist_ok=True)

    # Versión análisis — sin timestamp, se sobreescribe siempre
    cols = [c for c in COLUMNAS_ANALISIS if c in df.columns]
    df_analisis = df[cols]

    ruta = os.path.join(carpeta_base, "Base de buscadores estandarizada.xlsx")
    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df_analisis.to_excel(writer, index=False, sheet_name="datos_analisis")

    print(f"[Load] Resultado final guardado: {ruta}  ({len(df_analisis)} filas × {len(df_analisis.columns)} cols)")
    return ruta
