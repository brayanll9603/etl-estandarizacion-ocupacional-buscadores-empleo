"""
¿Cómo funciona?
  1. El modelo convierte cada texto de ocupación en un vector
     numérico de 384 dimensiones (embedding).
  2. Se calcula la similitud coseno entre el vector del cargo
     de la base de buscadores y los vectores de las 14.460 denominaciones CUOC.
  3. Se asigna la denominación con mayor similitud, siempre que
     supere el umbral definido (por defecto 0.80).

Modelo: paraphrase-multilingual-MiniLM-L12-v2
  - Entiende semántica en español e inglés
  - Maneja siglas, abreviaciones y cargos compuestos
  - Primera ejecución descarga el modelo (~470MB)

Caché de embeddings CUOC:
  - Primera vez  → calcula y guarda cuoc_embeddings.npy
  - Siguientes   → carga desde archivo (menos de 1 segundo)
  - Auto-invalida si el catálogo CUOC cambia
"""

import re
import unicodedata
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    EMBEDDING_MODEL,
    SIMILARITY_THRESHOLD,
    COL_OCUPACION,
    DATA_CATALOGOS,
)

# Rutas de los archivos de caché
# .npy es el formato de numpy para guardar arrays numéricos de forma eficiente
CACHE_EMBEDDINGS = os.path.join(DATA_CATALOGOS, "cuoc_embeddings.npy")
CACHE_TEXTOS     = os.path.join(DATA_CATALOGOS, "cuoc_textos.npy")


# ─────────────────────────────────────────────────────────────
# Función: _limpiar_texto
# ─────────────────────────────────────────────────────────────
# Normaliza el texto antes de generar el embedding para mejorar
# la precisión de la similitud. Pasos:
#   1. Quita tildes (normalización Unicode NFD)
#   2. Convierte a minúsculas
#   3. Elimina espacios no estándar
#   4. Elimina espacios múltiples
# ─────────────────────────────────────────────────────────────
def _limpiar_texto(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    # NFD separa caracteres base de sus marcas diacríticas
    # luego filtramos las marcas (categoría 'Mn')
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = texto.lower().strip()
    texto = re.sub(r"[^\S\n]", " ", texto)  # espacios no estándar → espacio normal
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


# ─────────────────────────────────────────────────────────────
# Clase: ImputadorOcupacion
# ─────────────────────────────────────────────────────────────
class ImputadorOcupacion:

    def __init__(self, df_cuoc: pd.DataFrame, modelo: str = EMBEDDING_MODEL):
        self.df_cuoc = df_cuoc.copy()

        # Limpiar textos del catálogo CUOC para mejorar la similitud
        self.df_cuoc["_texto_limpio"] = self.df_cuoc["nombre_denominacion"].apply(
            _limpiar_texto
        )

        print(f"[ImputadorOcupacion] Cargando modelo: {modelo}")
        self.modelo = SentenceTransformer(modelo)

        # Cargar o calcular embeddings CUOC
        self._cargar_o_calcular_embeddings()

    # ── Caché de embeddings ───────────────────────────────────
    def _cargar_o_calcular_embeddings(self):
        """
        Gestión inteligente de caché:
        - Si existe el archivo .npy y los textos son iguales → carga directo
        - Si no existe o el catálogo cambió → recalcula y guarda
        Esto evita recalcular 14.460 embeddings en cada ejecución.
        """
        textos_actuales = self.df_cuoc["_texto_limpio"].tolist()

        if os.path.exists(CACHE_EMBEDDINGS) and os.path.exists(CACHE_TEXTOS):
            textos_cache = np.load(CACHE_TEXTOS, allow_pickle=True).tolist()
            if textos_cache == textos_actuales:
                print("[ImputadorOcupacion] Caché encontrada — cargando embeddings CUOC...")
                self._emb_cuoc = np.load(CACHE_EMBEDDINGS)
                print(f"[ImputadorOcupacion] {len(self._emb_cuoc)} embeddings cargados.")
                return
            else:
                print("[ImputadorOcupacion] Catálogo CUOC cambió — recalculando...")
        else:
            print("[ImputadorOcupacion] Primera ejecución — calculando embeddings CUOC...")

        # Calcular embeddings: el modelo procesa los textos en batches
        print(f"[ImputadorOcupacion] Codificando {len(textos_actuales)} denominaciones...")
        self._emb_cuoc = self.modelo.encode(textos_actuales, show_progress_bar=True)

        # Guardar caché para próximas ejecuciones
        os.makedirs(DATA_CATALOGOS, exist_ok=True)
        np.save(CACHE_EMBEDDINGS, self._emb_cuoc)
        np.save(CACHE_TEXTOS, np.array(textos_actuales, dtype=object))
        print(f"[ImputadorOcupacion] Caché guardada: {CACHE_EMBEDDINGS}")

    # ── Imputación ────────────────────────────────────────────
    def imputar(
        self,
        df: pd.DataFrame,
        columna_ocupacion: str = COL_OCUPACION,
        umbral: float = SIMILARITY_THRESHOLD,
    ) -> pd.DataFrame:
        """
        Proceso:
          1. Limpia los textos de ocupación del archivo fuente
          2. Genera embeddings para cada ocupación
          3. Calcula similitud coseno contra los 14.460 embeddings CUOC
          4. Asigna la denominación con mayor similitud
          5. Marca como imputado solo si supera el umbral
        """
        df_out = df.copy()

        # Limpiar textos de la fuente (mismo proceso que aplicamos al catálogo)
        textos_src = (
            df_out[columna_ocupacion]
            .fillna("")
            .apply(_limpiar_texto)
            .tolist()
        )

        # Generar embeddings para los registros de entrada
        print(f"[ImputadorOcupacion] Codificando {len(textos_src)} registros fuente...")
        emb_src = self.modelo.encode(textos_src, show_progress_bar=True)

        # Matriz de similitud coseno: (n_registros × n_denominaciones_cuoc)
        # Cada celda [i,j] = similitud entre registro i y denominación j
        sim_matrix = cosine_similarity(emb_src, self._emb_cuoc)

        # Para cada registro, obtener el índice y valor de la máxima similitud
        idx_max = sim_matrix.argmax(axis=1)   # índice de la mejor denominación
        val_max = sim_matrix.max(axis=1)       # valor de similitud (0-1)

        # Obtener las filas del catálogo CUOC correspondientes a los mejores matches
        mejores = self.df_cuoc.iloc[idx_max].reset_index(drop=True)
        mask    = val_max >= umbral  # True = supera umbral, False = no se imputa

        # Agregar columnas al DataFrame resultado
        df_out["cuoc_similitud"]           = val_max.round(4)
        df_out["cuoc_imputado"]            = mask
        df_out["cuoc_codigo_denominacion"] = np.where(mask, mejores["codigo_denominacion"].values, None)
        df_out["cuoc_nombre_denominacion"] = np.where(mask, mejores["nombre_denominacion"].values, None)
        df_out["cuoc_codigo_ocupacion"]    = np.where(mask, mejores["codigo_ocupacion"].values, None)
        df_out["cuoc_nombre_ocupacion"]    = np.where(mask, mejores["nombre_ocupacion"].values, None)
        df_out["cuoc_area_cualificacion"]  = np.where(mask, mejores["area_cualificacion"].values, None)

        n_ok = int(mask.sum())
        print(
            f"[ImputadorOcupacion] Imputados: {n_ok}/{len(df_out)} "
            f"({n_ok/len(df_out)*100:.1f}%) con umbral >= {umbral}"
        )
        return df_out

    def reporte_calidad(self, df_imputado: pd.DataFrame) -> pd.DataFrame:
        """Resumen estadístico de la distribución de similitud."""
        sim = df_imputado["cuoc_similitud"]
        return pd.DataFrame([{
            "total_registros":   len(df_imputado),
            "imputados":         int(df_imputado["cuoc_imputado"].sum()),
            "no_imputados":      int((~df_imputado["cuoc_imputado"]).sum()),
            "similitud_media":   round(float(sim.mean()), 4),
            "similitud_mediana": round(float(sim.median()), 4),
            "similitud_min":     round(float(sim.min()), 4),
            "similitud_max":     round(float(sim.max()), 4),
        }])
