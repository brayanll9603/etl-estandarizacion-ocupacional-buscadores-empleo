import re
import unicodedata
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import NIVELES_EDUCATIVOS, EDUCACION_EQUIVALENCIAS, COL_NIVEL_EDUC

try:
    from rapidfuzz import process as rf_process, fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False

FUZZY_THRESHOLD = 75


# ─────────────────────────────────────────────────────────────
# Función: _normalizar
# ─────────────────────────────────────────────────────────────
# Quita tildes y convierte a minúsculas para comparaciones
# insensibles a mayúsculas y tildes.
# ─────────────────────────────────────────────────────────────
def _normalizar(texto: str) -> str:
    if not isinstance(texto, str): return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.lower().strip()


# Preparar índices de búsqueda una sola vez al importar el módulo
# (más eficiente que reconstruirlos en cada llamada)
_EQUIV_NORM  = {_normalizar(k): v for k, v in EDUCACION_EQUIVALENCIAS.items()}
_NIVELES_NORM = [_normalizar(n) for n in NIVELES_EDUCATIVOS]
_NIVELES_MAP  = {_normalizar(n): n for n in NIVELES_EDUCATIVOS}


# ─────────────────────────────────────────────────────────────
# Función: _clasificar
# ─────────────────────────────────────────────────────────────
# Clasifica un valor de nivel educativo en texto libre.
# Retorna un dict con: nivel oficial, método y score.
# ─────────────────────────────────────────────────────────────
def _clasificar(texto: str) -> dict:
    vacio = {"nivel": None, "metodo": "no_clasificado", "score": 0.0}
    if not isinstance(texto, str) or not texto.strip():
        return vacio

    norm = _normalizar(texto)

    # Capa 1: Coincidencia exacta con el diccionario de equivalencias
    # Cubre los valores más frecuentes como 'media(10-13)', 'profesional', etc.
    if norm in _EQUIV_NORM:
        return {"nivel": _EQUIV_NORM[norm], "metodo": "exacto", "score": 100.0}

    # También verificar si el texto ya es un nivel oficial
    if norm in _NIVELES_MAP:
        return {"nivel": _NIVELES_MAP[norm], "metodo": "exacto", "score": 100.0}

    # Capa 2: Coincidencia parcial
    # El texto de entrada contiene alguna clave del diccionario
    # Ordenamos por longitud descendente para evitar matches cortos incorrectos
    for clave, nivel in sorted(_EQUIV_NORM.items(), key=lambda x: -len(x[0])):
        if clave and clave in norm:
            return {"nivel": nivel, "metodo": "parcial", "score": 90.0}

    # Capa 3: Fuzzy matching sobre los niveles oficiales
    if _HAS_RAPIDFUZZ:
        resultado = rf_process.extractOne(norm, _NIVELES_NORM, scorer=fuzz.WRatio)
        if resultado and resultado[1] >= FUZZY_THRESHOLD:
            return {
                "nivel":  _NIVELES_MAP[resultado[0]],
                "metodo": "fuzzy",
                "score":  float(resultado[1]),
            }

    return vacio


# ─────────────────────────────────────────────────────────────
# Función pública: normalizar_nivel_educativo
# ─────────────────────────────────────────────────────────────
# Aplica la clasificación a toda la columna del DataFrame y
# agrega 3 columnas nuevas con el resultado.
# ─────────────────────────────────────────────────────────────
def normalizar_nivel_educativo(
    df: pd.DataFrame,
    columna: str = COL_NIVEL_EDUC,   # 'nivel_escolaridad'
) -> pd.DataFrame:
    df_out = df.copy()

    # Aplicar la clasificación fila por fila
    resultados = df_out[columna].fillna("").apply(_clasificar)

    # Desempaquetar en tres columnas
    df_out["nivel_educativo_norm"]   = resultados.apply(lambda r: r["nivel"])
    df_out["nivel_educativo_metodo"] = resultados.apply(lambda r: r["metodo"])
    df_out["nivel_educativo_score"]  = resultados.apply(lambda r: r["score"])

    # Reporte de resultados
    print("[NivelesEducativos] Resultado:")
    for metodo, cnt in df_out["nivel_educativo_metodo"].value_counts().items():
        print(f"  {metodo}: {cnt} ({cnt/len(df_out)*100:.1f}%)")

    return df_out


def reporte_calidad_educacion(df: pd.DataFrame) -> pd.DataFrame:
    """Tabla cruzada de niveles normalizados vs método usado."""
    return (
        df.groupby(["nivel_educativo_norm", "nivel_educativo_metodo"])
        .size().reset_index(name="registros")
        .sort_values("registros", ascending=False)
    )
