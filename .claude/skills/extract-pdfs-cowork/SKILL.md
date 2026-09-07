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

### 3. Lectura del PDF — texto donde es fiable, IMAGEN donde no (ahorro sin perder cifras)

**Regla coste/calidad (#3): lee TEXTO plano por defecto, y usa la página como IMAGEN solo cuando el texto NO es fiable o son cifras críticas.** Leer TODA página como imagen es caro (cada página ≈ miles de tokens) y no hace falta cuando el texto sale limpio. La calidad se mantiene porque las cifras críticas siguen yendo por imagen.

**Paso 1 — extrae el texto de la(s) página(s) target** con PyMuPDF:
```bash
python -c "import fitz; d=fitz.open(r'{pdf_path}'); print(d[P].get_text())"
```
**Paso 2 — decide POR PÁGINA:**
- **Texto LIMPIO → úsalo directamente, SIN imagen.** Limpio = prosa legible, palabras bien formadas, números con todos sus dígitos. Aplica a la **prosa cualitativa** (Directors' Report, estrategia, comentario del gestor) y a la **TOC**.
- **Texto SUCIO/CID → lee la página como IMAGEN.** Señales de sucio: espacios como `\x03`, secuencias tipo `7KH\x03\\HDU` (fuente CID con offset), cajas/carácter de reemplazo `�`, o números a los que les faltan dígitos.
- **CIFRAS CRÍTICAS pequeñas → por imagen** (pocas cifras en 1-2 págs, imagen barata y segura): `Statistics` (NAV/nº acciones), `Performance`, breakdowns geo/sector, Top Ten y el AUM. Un dígito mal ahí contamina todo.
- **Tablas GRANDES (`Securities Portfolio`/`Schedule of Investments`, 5-15 págs de RF) → texto si sale LIMPIO**, imagen solo en las páginas con CID/dígitos faltantes. Verifica 2-3 valores contra la imagen de UNA página para confirmar que el texto es fiable; si lo es, extrae el resto por texto (no rasterices 15 páginas en balde). Si viene CID → imagen, como siempre.

**Cómo leer como imagen (solo cuando aplique):**
1. `Read` del PDF (rasteriza con poppler) usando `pages` (máx ~20 págs/llamada).
2. **Si `Read` falla con `pdftoppm not found`** (Windows sin poppler), renderiza con **PyMuPDF (`fitz`)** a PNG 200 DPI y `Read` el PNG:
   ```bash
   python -c "import fitz; d=fitz.open(r'{pdf_path}'); [d[p].get_pixmap(dpi=200).save(rf'data/funds/{ISIN}/raw/_pg{p}.png') for p in range(START,END)]"
   ```
   Borra los `_pg*.png` al terminar la task.

Resumen: **prosa cualitativa y TOC → texto; tablas de cifras y páginas con CID → imagen.** Nunca copies una cifra de un texto que se ve sucio.

### 3b. Procesamiento por task (1-2 turns por task)

Para CADA task (leyendo cada página por texto o imagen según la regla de §3):

**Tipo A — Extracción simple (CNMV cualitativo, KIID, factsheet)**:
1. Leer la(s) página(s) del PDF (texto si limpio, imagen si cifras/CID — §3)
2. Aplicar el schema de extracción al contenido leído
3. Devolver JSON con los campos pedidos
4. Escribir `data/funds/{ISIN}/extracted/{task_id}.json`

**Tipo B — Concept-first 2-stage (Annual Reports INT >30 páginas)**:
1. **Stage 1 (mapper)**: localiza la TOC (texto plano) y calcula el offset TOC→PDF. Mapea SOLO las páginas del **sub-fondo TARGET**: su Directors' Report/estrategia, sus Statistics, su Securities Portfolio, sus breakdowns y Top Ten. Output intermedio: `{section_name: page_range}` **del target**.
2. **Stage 2 (extractor) — lee ÚNICAMENTE ese rango del target (#2, gran ahorro):**
   - **NUNCA leas el PDF entero** (un paraguas son 500 págs con ~25 sub-fondos): es el mayor derroche de tokens y arriesga contaminar con datos de OTRO sub-fondo.
   - **NO leas las páginas de los sub-fondos vecinos.** El target ocupa un bloque contiguo (p.ej. sus Financial Statements van desde su primera página hasta la del SIGUIENTE sub-fondo en la TOC); limita el rango a ese bloque.
   - Solo amplías si una sección del target se sale de lo mapeado (p.ej. un `Securities Portfolio` de RF que ocupa más páginas de las previstas → sigue paginando hasta acabar ESA sección del target).
3. Devolver JSON estructurado completo.

Para tasks con `two_stage: true` → siempre Tipo B.

### 4. Reglas de extracción (no negociables)

- **Citas literales**: copia EXACTAMENTE como aparece en el PDF. NO parafrasees secciones cualitativas. NO traduzcas.
- **Cifras literales**: AUM, TER, %, fechas, partícipes — copia desde el PDF, no inventes ni redondees.
- **Anti-invención**: si un campo del schema NO está en el PDF, devuelve `null` o `""` (no hagas suposiciones).
- **Sub-fondos vs umbrella SICAV**: si el PDF es de un SICAV-paraguas con sub-fondos, extrae datos del sub-fondo target indicado en `context`. NO sumes AUM agregados del paraguas.
- **Identificar el sub-fondo por NOMBRE (paraguas con muchos sub-fondos)**: el ISIN a menudo NO se imprime en el AR. Si el `fund_name`/`context` del target coincide de forma clara con UNO de los sub-fondos listados en el AR (match por nombre, aunque falte el ISIN — p.ej. target "Baillie Gifford WW Long Term Global Growth" → sub-fondo "Long Term Global Growth"), **EXTRAE ESE sub-fondo**. NO te detengas por no ver el ISIN. Detente y no escribas nada SOLO si hay ambigüedad REAL: varios sub-fondos podrían ser el target, o ninguno coincide. Ante un match claro por nombre, extraer es lo correcto (no extraer nada es peor que extraer el sub-fondo bien identificado).
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
- **El cuello de botella de calidad NO es el modelo sino el input.** Ver «Lectura del PDF» (§3): las **cifras** salen de la **imagen** de la página (evita fallos CID/dígitos), y la **prosa cualitativa** se lee por **texto** (fiable y barato). Elegir bien texto-vs-imagen por página rinde más que cualquier cambio de tier — y ahorra tokens sin perder precisión.

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
