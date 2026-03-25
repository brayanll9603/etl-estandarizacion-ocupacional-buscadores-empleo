"""
extract/catalogos.py
───────────────────────────────────────────────────────────────
Proyecto : Estandarización Buscadores de Empleo
Archivo  : extract/catalogos.py
Propósito: Carga y limpieza inicial de los catálogos oficiales.
           Retorna DataFrames listos para ser usados en la fase
           de transformación.

Catálogos:
  - CUOC 2025  : Clasificación Única de Ocupaciones para Colombia (DANE)
  - DIVIPOLA   : División Político-Administrativa de Colombia (DANE)
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    CUOC_FILE, CUOC_SHEET, CUOC_HEADER_ROW,
    CUOC_COL_GRAN_GRP, CUOC_COL_OCC_CODE, CUOC_COL_OCC_NAME,
    CUOC_COL_DEN_CODE, CUOC_COL_DEN_NAME, CUOC_COL_AREA,
    DIVIPOLA_FILE, DIVIPOLA_SHEET,
    DIVIPOLA_COL_DEP_COD, DIVIPOLA_COL_DEP_NOM,
    DIVIPOLA_COL_MUN_COD, DIVIPOLA_COL_MUN_NOM,
)


# ─────────────────────────────────────────────────────────────
# Función: cargar_cuoc
# ─────────────────────────────────────────────────────────────
# El catálogo CUOC tiene 3 niveles jerárquicos:
#   Gran grupo  → código de 1 dígito  (ej. '0')
#   Ocupación   → código de 4 dígitos (ej. '1100')
#   Denominación→ código con punto    (ej. '01100.001')
#
# Para la imputación solo usamos las DENOMINACIONES porque son
# el nivel más específico y el que mejor describe el cargo.
# Se filtran usando str.contains(r"\.") que detecta el punto.
#
# dtype=str en toda la lectura evita que pandas convierta
# '01100' a 1100 (número), perdiendo los ceros iniciales.
# ─────────────────────────────────────────────────────────────
def cargar_cuoc() -> pd.DataFrame:
    # Leer el Excel completo como texto
    df = pd.read_excel(
        CUOC_FILE,
        sheet_name=CUOC_SHEET,
        header=CUOC_HEADER_ROW,  # la fila 1 (0-indexed) tiene los encabezados reales
        dtype=str,
    )

    # Renombrar columnas a nombres internos estandarizados
    df = df.rename(columns={
        CUOC_COL_GRAN_GRP: "gran_grupo",
        CUOC_COL_OCC_CODE: "codigo_ocupacion_raw",
        CUOC_COL_OCC_NAME: "nombre_ocupacion",
        CUOC_COL_DEN_CODE: "codigo_denominacion",
        CUOC_COL_DEN_NAME: "nombre_denominacion",
        CUOC_COL_AREA:     "area_cualificacion",
    })

    # Filtrar solo denominaciones (tienen punto en el código, ej. '01100.001')
    # Esto reduce de ~30.000 filas a 14.460 denominaciones
    df = df[df["codigo_denominacion"].str.contains(r"\.", na=False)].copy()

    # Extraer el código de ocupación (5 dígitos antes del punto)
    # Ejemplo: '01100.001' → '01100'
    df["codigo_ocupacion"] = df["codigo_denominacion"].str.extract(r"^(\d+)\.")[0]

    # Eliminar filas sin denominación (filas vacías o de encabezado)
    df = df.dropna(subset=["nombre_denominacion", "codigo_denominacion"])
    df = df.reset_index(drop=True)

    return df[[
        "gran_grupo", "codigo_ocupacion", "nombre_ocupacion",
        "codigo_denominacion", "nombre_denominacion", "area_cualificacion",
    ]]


# ─────────────────────────────────────────────────────────────
# Función: cargar_divipola
# ─────────────────────────────────────────────────────────────
# DIVIPOLA contiene 3 tipos de registros:
#   CM = Cabecera Municipal   → la ciudad principal del municipio
#   CP = Centro Poblado       → corregimientos y veredas
#   CU = Cabecera de Corregimiento
#
# Solo usamos CM para tener UN registro único por municipio
# y evitar duplicados. Sin este filtro, cada municipio aparece
# varias veces (una por cada centro poblado).
#
# El archivo Excel tiene los encabezados reales en la fila 10
# (índice 10, contando desde 0). Las primeras filas contienen
# títulos y encabezados de sección del archivo original del DANE.
#
# Longitud y Latitud se preservan como float para usarlas
# en visualizaciones geográficas en el EDA.
# ─────────────────────────────────────────────────────────────
def cargar_divipola() -> pd.DataFrame:
    # header=10 porque los encabezados reales están en la fila 11 del Excel
    # (fila 10 en índice 0-based de pandas)
    # No usamos dtype=str aquí para preservar longitud/latitud como float
    df = pd.read_excel(
        DIVIPOLA_FILE,
        sheet_name=DIVIPOLA_SHEET,
        header=10,
    )

    # Forzar a str solo las columnas de código y nombre
    for col in [DIVIPOLA_COL_DEP_COD, DIVIPOLA_COL_DEP_NOM,
                DIVIPOLA_COL_MUN_COD, DIVIPOLA_COL_MUN_NOM]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Construir mapa de renombre
    # La columna "Tipo" a veces viene con salto de línea ('Tipo\n')
    # según la versión de Excel, por eso buscamos cualquier columna
    # que CONTENGA la palabra "Tipo" en lugar de buscarla exacta
    rename_map = {
        DIVIPOLA_COL_DEP_COD: "codigo_departamento",
        DIVIPOLA_COL_DEP_NOM: "nombre_departamento",
        DIVIPOLA_COL_MUN_COD: "codigo_municipio",
        DIVIPOLA_COL_MUN_NOM: "nombre_municipio",
        "Longitud":           "longitud",
        "Latitud":            "latitud",
    }
    for col in df.columns:
        if "Tipo" in str(col):
            rename_map[col] = "tipo"

    df = df.rename(columns=rename_map)

    # Limpiar espacios en columnas de texto
    for col in ["codigo_municipio", "nombre_municipio",
                "codigo_departamento", "nombre_departamento", "tipo"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Filtrar solo cabeceras municipales (CM) → un registro por municipio
    df_mun = (
        df[df["tipo"] == "CM"]
        [["codigo_departamento", "nombre_departamento",
          "codigo_municipio",    "nombre_municipio",
          "longitud",            "latitud"]]
        .drop_duplicates(subset=["codigo_municipio"])
        .reset_index(drop=True)
    )

    # Asegurar que las coordenadas sean numéricas
    df_mun["longitud"] = pd.to_numeric(df_mun["longitud"], errors="coerce")
    df_mun["latitud"]  = pd.to_numeric(df_mun["latitud"],  errors="coerce")

    return df_mun
