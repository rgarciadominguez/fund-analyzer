---
name: extract-pdfs-cowork
description: Extrae contenido cualitativo y estructurado de PDFs descargados por la prep determinista (CNMV anexos semestrales, Annual Reports INT, KIIDs, prospectus, factsheets). Reemplaza las llamadas a Gemini Pro/Flash en `cnmv_agent.py` (cualitativo), `cnmv_enrichment.py` (sectores/RV/RF) e `intl_extractor_v2.py` (concept-first 2-stage). Úsala SIEMPRE que Rafa diga "extract pdfs cowork", "extrae cualitativo de X", "skill extract pdfs X", o cualquier variante sobre extraer texto/datos cualitativos de PDFs descargados localmente. Espera que la prep haya dejado un manifiesto `pending_extraction.json` listando los PDFs y los esquemas de extracción esperados.
---

# extract-pdfs-cowork v1.0

Sustituto de las llamadas Gemini Pro/Flash a `gemini_wrapper.py` para extracción de PDFs locales. Diseñada para correr bajo Claude Max sin coste API.

**Reemplaza**:
- `cnmv_agent._extract_cualitativo_from_pdf()` (Gemini Pro)
- `cnmv_enrichment._extract_sectors_from_pdf()` (Gemini Flash)
- `intl_extractor_v2._mapper_pro()` + `_extractor_flash()` (concept-first 2-stage)

## Pre-requisito obligatorio

La prep determinista debe haber dejado:
- PDFs descargados en `data/funds/{ISIN}/raw/reports/` y/o `raw/discovery/`
- `data/funds/{ISIN}/pending_extraction.json` con la lista de tareas

Estructura esperada de `pending_extraction.json`:

```json
{
  "isin": "ES0146309002",
  "tipo": "ES",
  "tasks": [
    {
      "id": "cnmv_cualitativo_2024_H2",
      "agent": "cnmv_agent",
      "pdf_path": "data/funds/{ISIN}/raw/reports/CNMV_ES0146309002_2024_H2.pdf",
      "schema": {
        "seccion_9_texto_completo": "string",
        "seccion_10_perspectivas_texto": "string",
        "hechos_relevantes": "list[{periodo, epigrafe, detalle}]"
      },
      "context": "Anexo CNMV semestral del fondo. Extrae literal las secciones 9 y 10. Si hay hechos relevantes en sección 10 o 11, lístalos."
    },
    {
      "id": "intl_extractor_AR_2024",
      "agent": "intl_extractor_v2",
      "pdf_path": "data/funds/{ISIN}/raw/discovery/AR_2024.pdf",
      "schema": {
        "kpis": {"aum_actual_meur": "float", "ter_pct": "float"},
        "posiciones": "list[{nombre, peso_pct, sector, pais, asset_type}]",
        "cualitativo": {"estrategia": "string", "filosofia_inversion": "string"}
      },
      "context": "Annual Report del sub-fondo (Class O EUR). NO sumes AUM del SICAV paraguas — solo del sub-fondo target.",
      "two_stage": true
    }
  ]
}
```

Si el archivo no existe → la prep no terminó correctamente. ABORTA y pide a Rafa que ejecute la prep antes.

## Schema EXACTO del output

Por cada task del manifiesto, escribir el resultado en `data/funds/{ISIN}/extracted/{task_id}.json`:

```json
{
  "task_id": "cnmv_cualitativo_2024_H2",
  "agent": "cnmv_agent",
  "pdf_path": "...",
  "extracted_at": "ISO timestamp",
  "model_used": "claude-opus-4-8 (cowork)",
  "data": {
    "seccion_9_texto_completo": "...",
    "seccion_10_perspectivas_texto": "...",
    "hechos_relevantes": [...]
  },
  "anti_invencion_notes": []
}
```

Y al terminar TODOS los tasks, escribir `data/funds/{ISIN}/extraction_complete.json`:

```json
{
  "isin": "...",
  "completed_at": "ISO",
  "n_tasks": 5,
  "n_succeeded": 5,
  "n_failed": 0,
  "task_outputs": ["data/funds/{ISIN}/extracted/{id}.json"]
}
```

## Workflow paso a paso

### 1. Validación pre-requisitos (1 turn)

Bash:
```
ISIN={ISIN}
ls data/funds/$ISIN/pending_extraction.json
ls data/funds/$ISIN/raw/
```

Si falta el manifest o no hay PDFs → aborta.

### 2. Lectura del manifest (1 turn)

Read `data/funds/{ISIN}/pending_extraction.json`. Lista todas las tasks.

### 3. Lectura del PDF — IMAGEN, no texto (crítico para fidelidad de cifras)

Lee cada página como **imagen**, no como texto plano. El texto de `pdfplumber`/`pypdf` en informes CNMV/AR **pierde dígitos** y trae **prosa con fuentes CID** ilegibles — copiar cifras de ahí es la causa nº1 de datos malos. La imagen de la página es inmune a eso.

1. **Primera opción — `Read` del PDF** (el tool lo rasteriza con `pdftoppm`/poppler y te da la página como imagen). Usa el parámetro `pages` (máx ~20 págs/llamada; obligatorio en PDFs >10 págs).
2. **Si `Read` falla con `pdftoppm not found`** (típico en Windows sin poppler), renderiza con **PyMuPDF (`fitz`, ya instalado)** a PNG 200 DPI y `Read` el PNG:
   ```bash
   python -c "import fitz; d=fitz.open(r'{pdf_path}'); [d[p].get_pixmap(dpi=200).save(rf'data/funds/{ISIN}/raw/_pg{p}.png') for p in range(START,END)]"
   ```
   Luego `Read data/funds/{ISIN}/raw/_pg{N}.png` por cada página. Borra los `_pg*.png` al terminar la task.
3. **`pdfplumber`/`pypdf` texto plano SOLO para localizar** (TOC, en qué página está una sección) — **nunca para copiar cifras**.

### 3b. Procesamiento por task (1-2 turns por task)

Para CADA task (leyendo las páginas como imagen según §3):

**Tipo A — Extracción simple (CNMV cualitativo, KIID, factsheet)**:
1. Leer la(s) página(s) del PDF como imagen (§3)
2. Aplicar el schema de extracción al contenido leído
3. Devolver JSON con los campos pedidos
4. Escribir `data/funds/{ISIN}/extracted/{task_id}.json`

**Tipo B — Concept-first 2-stage (Annual Reports INT >30 páginas)**:
1. **Stage 1 (mapper)**: localiza la TOC (texto plano vale aquí) y mapea qué páginas contienen qué (estrategia, posiciones, KPIs, gestores). Output intermedio: `{section_name: page_range}`. NO intentes `Read` de un PDF de 500 págs entero — pagina por rangos de la TOC.
2. **Stage 2 (extractor)**: Re-leer las páginas específicas identificadas en stage 1, extraer datos estructurados al schema.
3. Devolver JSON estructurado completo.

Para tasks con `two_stage: true` → siempre Tipo B.

### 4. Reglas de extracción (no negociables)

- **Citas literales**: copia EXACTAMENTE como aparece en el PDF. NO parafrasees secciones cualitativas. NO traduzcas.
- **Cifras literales**: AUM, TER, %, fechas, partícipes — copia desde el PDF, no inventes ni redondees.
- **Anti-invención**: si un campo del schema NO está en el PDF, devuelve `null` o `""` (no hagas suposiciones).
- **Sub-fondos vs umbrella SICAV**: si el PDF es de un SICAV-paraguas con sub-fondos, extrae datos del sub-fondo target indicado en `context`. NO sumes AUM agregados del paraguas.
- **Un AR por AÑO = extrae TODO de CADA año (crítico para el histórico)**: si hay varios Annual Reports (annual_report_2020.pdf, ..._2024.pdf), procesa CADA uno por separado y transcribe SUS posiciones, breakdowns (geo/sector/asset), statistics y performance. **NO omitas las posiciones de un AR "antiguo" pensando que el AR reciente ya trae la cartera actual** — el análisis histórico de consistencia y cambios de cartera NECESITA la cartera de CADA año. Cada `extracted/{...}_annual_report_YYYY.json` debe llevar las `posiciones` de ESE año (no vacías). Sí es más trabajo: el primer análisis multi-año tarda más, es lo esperado.
- **Cartera COMPLETA (crítico en renta fija)**: `posiciones` = la sección `Securities Portfolio`/`Schedule of Investments` ENTERA, no el `Top Ten Holdings`. En un fondo de bonos son 100-300+ líneas en 5-15 páginas: lee TODAS esas páginas (como imagen si el texto es CID/ilegible) y extrae CADA línea. Si terminas con ≤15 posiciones en un fondo de deuda, NO leíste la sección completa → vuelve y pagina por todo el portfolio. El `Top Ten` va SOLO en `top_10`. Para cada bono captura `emisor` y, si el AR trae columna de rating crediticio (Moody's/S&P/Fitch), el `rating` literal; si no hay columna de rating → `rating: null` (no lo inventes).
- **Fechas**: formato ISO `YYYY-MM-DD` siempre que sea posible.
- **Idiomas**: respeta el idioma original del PDF en cualitativo. NO traduzcas inglés→español ni viceversa.

### 5. Anti-invención por task

Después de cada extracción, registra en `anti_invencion_notes` qué campos del schema NO encontraste en el PDF:

```json
"anti_invencion_notes": [
  "kpis.num_participes: no aparece en el AR (solo en cnmv_data)",
  "posiciones[*].sector: no se menciona, dejado null"
]
```

### 6. Manifest de cierre (1 turn al final)

Tras procesar todos los tasks, escribir `data/funds/{ISIN}/extraction_complete.json` con el resumen.

### 7. Mensaje final a Rafa

Confirma:
- N tasks procesadas exitosamente / fallidas
- Path de los outputs
- Comando siguiente:
  ```
  python -m agents.orchestrator --isin {ISIN} --consume-extracted
  ```

NO ejecutes el consume automáticamente.

## Modelo recomendado

- **Opus 4.8** — el bat lo fuerza vía `claude -p --model claude-opus-4-8` (var `MODEL_EXTRACT`). Calidad-primero: la extracción ya corría en Opus (evidencia en `extracted/*.json`) y rinde bien con `anti_invencion_notes`; bajar a Sonnet sería ahorro de coste, no mejora.
- **El cuello de botella de calidad NO es el modelo sino el input.** Ver «Lectura del PDF» abajo: leer la página como **imagen** (no texto de pdfplumber) es lo que evita los fallos de cifras/CID. Eso rinde más que cualquier cambio de tier.

## Coste y rate limit

- Por fondo ES (1-3 PDFs CNMV semestrales): ~5-8 turns total
- Por fondo INT (1-2 Annual Reports + KIID + factsheet): ~10-15 turns total
- Bajo Max: 0€ marginal
- Volumen: 8-12 fondos por ventana 5h

## Errores comunes a evitar

1. **Inventar campos faltantes**: si el PDF no tiene num_participes, devuelve null. NUNCA "estimes".
2. **Traducir cualitativo**: respeta idioma original.
3. **Sumar AUM de paraguas SICAV**: solo del sub-fondo target.
4. **Saltar anti_invencion_notes**: registra siempre qué campos no encontraste.
5. **Ejecutar el consume automáticamente**: NO. Devuelve el comando al usuario.
6. **Procesar PDFs corruptos sin avisar**: si un PDF no se puede parsear, escribe el task como `failed` con el motivo, no inventes data.
