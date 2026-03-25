import re
import unicodedata
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import COL_MUNICIPIO

try:
    from rapidfuzz import process as rf_process, fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False
    print("[AVISO] rapidfuzz no instalado. Solo se usará alias + exacto.")


# ─────────────────────────────────────────────────────────────
# Función: _normalizar
# ─────────────────────────────────────────────────────────────
# Quita tildes, convierte a mayúsculas y elimina espacios extra.
# Se aplica tanto al nombre en la data como a los nombres del
# catálogo, garantizando que la comparación sea consistente.
# ─────────────────────────────────────────────────────────────
def _normalizar(texto: str) -> str:
    if not isinstance(texto, str): return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.upper().strip()


class HomologadorMunicipio:

    # Diccionario de aliases: nombre en la data → nombre oficial en DIVIPOLA
    # Necesario porque algunos municipios tienen nombres oficiales distintos
    # a como se conocen popularmente.
    ALIASES = {
        "CALI":              "SANTIAGO DE CALI",    # nombre oficial completo
        "BOGOTA":            "BOGOTA, D.C.",
        "BOGOTA D.C.":       "BOGOTA, D.C.",
        "BOGOTÁ":            "BOGOTA, D.C.",
        "BOGOTÁ D.C.":       "BOGOTA, D.C.",
        "CARTAGENA":         "CARTAGENA DE INDIAS",
        "SANTA MARTA":       "SANTA MARTA (DIST. ESPEC.)",
        "BARRANQUILLA":      "BARRANQUILLA",
    }

    FUZZY_THRESHOLD = 80  # similitud mínima para aceptar match fuzzy (0-100)

    def __init__(self, df_divipola: pd.DataFrame):
        self.df_div = df_divipola.copy()

        # Crear columna normalizada para búsqueda eficiente
        self.df_div["_nombre_norm"] = self.df_div["nombre_municipio"].apply(_normalizar)

        # Índice de búsqueda O(1) para coincidencia exacta
        self._lookup = self.df_div.set_index("_nombre_norm")

        # Lista de nombres para búsqueda fuzzy
        self._nombres_norm = self.df_div["_nombre_norm"].tolist()

        print(f"[HomologadorMunicipio] Catálogo DIVIPOLA cargado: {len(self.df_div)} municipios.")

    def homologar(self, df: pd.DataFrame, columna_municipio: str = COL_MUNICIPIO) -> pd.DataFrame:
        """
        Aplica la homologación a cada fila del DataFrame.
        Agrega 8 columnas nuevas con los datos oficiales de DIVIPOLA.
        """
        df_out = df.copy()

        # Aplicar la búsqueda a cada municipio de forma vectorizada
        resultados = df_out[columna_municipio].fillna("").apply(self._buscar_municipio)

        # Desempaquetar los resultados en columnas separadas
        df_out["divipola_codigo_municipio"]    = resultados.apply(lambda r: r["codigo_municipio"])
        df_out["divipola_nombre_municipio"]    = resultados.apply(lambda r: r["nombre_municipio"])
        df_out["divipola_codigo_departamento"] = resultados.apply(lambda r: r["codigo_departamento"])
        df_out["divipola_nombre_departamento"] = resultados.apply(lambda r: r["nombre_departamento"])
        df_out["longitud"]                     = resultados.apply(lambda r: r["longitud"])
        df_out["latitud"]                      = resultados.apply(lambda r: r["latitud"])
        df_out["divipola_metodo"]              = resultados.apply(lambda r: r["metodo"])
        df_out["divipola_score"]               = resultados.apply(lambda r: r["score"])

        # Reporte por método usado
        print("[HomologadorMunicipio] Resultado:")
        for metodo, cnt in df_out["divipola_metodo"].value_counts().items():
            print(f"  {metodo}: {cnt} ({cnt/len(df_out)*100:.1f}%)")

        return df_out

    def _buscar_municipio(self, texto: str) -> dict:
        """
        Busca un municipio aplicando las 3 capas en orden:
        alias → exacto → fuzzy
        Retorna un dict con todos los campos DIVIPOLA.
        """
        # Resultado vacío para cuando no se encuentra match
        vacio = {
            "codigo_municipio": None, "nombre_municipio": None,
            "codigo_departamento": None, "nombre_departamento": None,
            "longitud": None, "latitud": None,
            "metodo": "no_encontrado", "score": 0.0,
        }
        if not texto.strip():
            return vacio

        # Normalizar el texto de entrada
        norm_original = _normalizar(texto)

        # Capa 1: Alias manual
        # Busca si el nombre normalizado tiene un alias definido
        norm = self.ALIASES.get(norm_original, norm_original)
        metodo = "alias" if norm != norm_original else "exacto"

        # Capa 2: Coincidencia exacta
        # Busca en el índice del catálogo (operación O(1))
        if norm in self._lookup.index:
            fila = self._lookup.loc[norm]
            if isinstance(fila, pd.DataFrame):
                fila = fila.iloc[0]  # si hay duplicados, tomar el primero
            return {
                "codigo_municipio":    fila["codigo_municipio"],
                "nombre_municipio":    fila["nombre_municipio"],
                "codigo_departamento": fila["codigo_departamento"],
                "nombre_departamento": fila["nombre_departamento"],
                "longitud":            fila.get("longitud"),
                "latitud":             fila.get("latitud"),
                "metodo":              metodo,
                "score":               100.0,
            }

        # Capa 3: Fuzzy matching
        # rapidfuzz compara el texto con todos los nombres del catálogo
        # y retorna el más similar si supera el umbral definido
        if _HAS_RAPIDFUZZ:
            resultado = rf_process.extractOne(
                norm, self._nombres_norm, scorer=fuzz.WRatio
            )
            if resultado and resultado[1] >= self.FUZZY_THRESHOLD:
                fila = self.df_div[self.df_div["_nombre_norm"] == resultado[0]].iloc[0]
                return {
                    "codigo_municipio":    fila["codigo_municipio"],
                    "nombre_municipio":    fila["nombre_municipio"],
                    "codigo_departamento": fila["codigo_departamento"],
                    "nombre_departamento": fila["nombre_departamento"],
                    "longitud":            fila.get("longitud"),
                    "latitud":             fila.get("latitud"),
                    "metodo":              "fuzzy",
                    "score":               float(resultado[1]),
                }

        return vacio

    def reporte_calidad(self, df_homologado: pd.DataFrame) -> pd.DataFrame:
        conteo = df_homologado["divipola_metodo"].value_counts().reset_index()
        conteo.columns = ["metodo", "registros"]
        conteo["porcentaje"] = (conteo["registros"] / len(df_homologado) * 100).round(2)
        return conteo
