# Estandarización Buscadores de Empleo 

Pipeline ETL orientado a la estandarización estructural de variables críticas (ocupación, nivel educativo, municipio, entre otras), mediante procesos de validación contra catálogos oficiales, reglas de negocio y técnicas de procesamiento de texto, con el fin de consolidar una base de datos analítica robusta y gobernada.

## Transformaciones

| Variable          | Técnica                                | Catálogo              |
|-------------------|----------------------------------------|-----------------------|
| Ocupación         | Sentence Embeddings + coseno           | CUOC 2025 (DANE)      |
| Municipio         | Alias + exacto + fuzzy                 | DIVIPOLA sep-2025     |
| Nivel escolaridad | Diccionario de equivalencias + fuzzy   | Catálogo institucional|

## Estructura del proyecto

```
etl_project/
├── data/
│   ├── raw/            ← datos anonimizados de buscadores de empleo
│   ├── catalogos/      ← catálogos oficiales DANE (CUOC, DIVIPOLA)
│   ├── processed/      ← resultados con timestamp (no se versiona)
│   └── resultados/     ← entregable final del pipeline (sí se versiona)
├── src/
│   ├── config.py                        ← rutas, columnas, umbrales
│   ├── pipeline.py                      ← orquestador ETL
│   ├── eda_visualizaciones.py           ← dashboard EDA
│   ├── extract/catalogos.py             ← carga CUOC y DIVIPOLA
│   ├── transform/imputar_ocupacion.py   ← Sentence Embeddings → CUOC
│   ├── transform/homologar_municipio.py ← alias + exacto + fuzzy → DIVIPOLA
│   ├── transform/normalizar_educacion.py← diccionario → niveles oficiales
│   └── load/guardar_resultados.py       ← guarda Excel + reporte calidad
├── README.md
├── requirements.txt
└── .gitignore
```

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

```bash
# 1. Correr pipeline ETL
python src/pipeline.py

# 2. Generar dashboard EDA
python src/eda_visualizaciones.py

# 3. Abrir dashboard en el navegador
start data\processed\graficos\dashboard_eda.html
```

## Archivos generados

| Archivo | Ubicación | Descripción |
|---|---|---|
| `Base de buscadores estandarizada.xlsx` | `data/resultados/` | Entregable final (en Git) |
| `Resultados estandarizacion_FECHA.xlsx` | `data/processed/` | Completo con auditoría |
| `reporte_calidad_FECHA.xlsx` | `data/processed/` | Resumen del proceso |
| `dashboard_eda.html` | `data/processed/graficos/` | Dashboard interactivo |

## KPIs del pipeline

- KPI 1: % ocupaciones homologadas CUOC con similitud ≥ 0.80
- KPI 2: % reducción de categorías únicas de ocupación
- KPI 3: % municipios validados contra DIVIPOLA
- KPI 4: % nivel educativo normalizado
- Tiempos de ejecución por fase (Extract / Transform / Load)

## Niveles educativos oficiales

Ninguna · Básica primaria · Básica secundaria · Bachiller ·
Técnico laboral · Técnico profesional · Tecnólogo · Universitario ·
Especialización · Magíster · Doctorado
