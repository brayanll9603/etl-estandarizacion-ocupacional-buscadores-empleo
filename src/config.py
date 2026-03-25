import os

# ── Rutas base ──────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_RAW       = os.path.join(ROOT_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(ROOT_DIR, "data", "processed")
DATA_CATALOGOS = os.path.join(ROOT_DIR, "data", "catalogos")

# ── Catálogos ───────────────────────────────────────────────
CUOC_FILE     = os.path.join(DATA_CATALOGOS, "PerfilesOcupacionales-Excel-CUOC-2025.xlsx")
DIVIPOLA_FILE = os.path.join(DATA_CATALOGOS, "DIVIPOLA_CentrosPoblados.xlsx")

# ── CUOC ────────────────────────────────────────────────────
CUOC_SHEET        = "Denominaciones"
CUOC_HEADER_ROW   = 1          # fila 0-indexed donde están los encabezados
CUOC_COL_GRAN_GRP = "Código del Gran Grupo"
CUOC_COL_OCC_CODE = "Código de la Ocupación"
CUOC_COL_OCC_NAME = "Nombre de la Ocupación"
CUOC_COL_DEN_CODE = "Código de la Denominación"
CUOC_COL_DEN_NAME = "Nombre de la Denominación"
CUOC_COL_AREA     = "Área de Cualificación Principal"

# ── DIVIPOLA ─────────────────────────────────────────────────
DIVIPOLA_SHEET      = "Cabeceras - Centros Poblados"
DIVIPOLA_HEADER_ROW = 10       # fila 1-indexed (openpyxl)
# Columnas resultantes después de leer con header en fila 10
DIVIPOLA_COL_DEP_COD  = "Código"          # código departamento (2 dígitos)
DIVIPOLA_COL_DEP_NOM  = "Nombre"          # nombre departamento
DIVIPOLA_COL_MUN_COD  = " Código "        # código municipio (5 dígitos)
DIVIPOLA_COL_MUN_NOM  = " Nombre "        # nombre municipio
DIVIPOLA_COL_CP_COD   = "  Código  "      # código centro poblado (8 dígitos)
DIVIPOLA_COL_CP_NOM   = "  Nombre  "

# ── Modelo de embeddings ─────────────────────────────────────
EMBEDDING_MODEL    = "paraphrase-multilingual-MiniLM-L12-v2"
SIMILARITY_THRESHOLD = 0.60    # umbral mínimo para aceptar imputación

# ── Niveles educativos oficiales ──────────────────────────────
NIVELES_EDUCATIVOS = [
    "Ninguna",
    "Básica primaria",
    "Básica secundaria",
    "Bachiller",
    "Técnico laboral",
    "Técnico profesional",
    "Tecnólogo",
    "Universitario",
    "Especialización",
    "Magíster",
    "Doctorado",
]

# Diccionario de equivalencias (variantes → nivel oficial)
EDUCACION_EQUIVALENCIAS = {
    # ── Valores reales encontrados en Data_Proyecto.xlsx ──────
    "media(10-13)":              "Bachiller",
    "tecnica laboral":           "Técnico laboral",
    "profesional":               "Universitario",
    "tecnologica":               "Tecnólogo",
    "especializacion":           "Especialización",
    "basica secundaria(6-9)":    "Básica secundaria",
    "basica primaria(1-5)":      "Básica primaria",
    "magister":                  "Magíster",
    "no identificado":           "Ninguna",

    # ── Variantes adicionales ─────────────────────────────────
    "sin estudio":               "Ninguna",
    "ninguno":                   "Ninguna",
    "no tiene":                  "Ninguna",
    "analfabeta":                "Ninguna",
    "primaria":                  "Básica primaria",
    "basica primaria":           "Básica primaria",
    "secundaria":                "Básica secundaria",
    "basica secundaria":         "Básica secundaria",
    "bachillerato":              "Bachiller",
    "bachiller":                 "Bachiller",
    "media vocacional":          "Bachiller",
    "tecnico laboral":           "Técnico laboral",
    "tecnico":                   "Técnico laboral",
    "tecnico profesional":       "Técnico profesional",
    "tecnologo":                 "Tecnólogo",
    "tecnologia":                "Tecnólogo",
    "universitario":             "Universitario",
    "pregrado":                  "Universitario",
    "licenciatura":              "Universitario",
    "universidad":               "Universitario",
    "especialista":              "Especialización",
    "posgrado":                  "Especialización",
    "maestria":                  "Magíster",
    "msc":                       "Magíster",
    "doctorado":                 "Doctorado",
    "phd":                       "Doctorado",
    "doctor":                    "Doctorado",
}

# ── Columna de entrada en la fuente principal ─────────────────
COL_OCUPACION      = "ocupacion"          # nombre de columna en data raw
COL_NIVEL_EDUC     = "nivel_escolaridad"  # nombre real en el archivo fuente
COL_MUNICIPIO      = "municipio"
