---
name: analyst-cowork
description: Genera el bloque `analyst_synthesis.*` (8 secciones narrativas + estructuradas) de un fondo del proyecto fund-analyzer usando la cuota de Claude Max. Reemplaza al `agents/analyst_agent.py` legacy. Úsala SIEMPRE que Rafa diga "analyst cowork", "analiza fondo X con cowork", "regenera síntesis de X via skill", "skill analyst X", "consume preview de X", "monta el analyst de X aquí", o cualquier variante sobre ejecutar la síntesis del analyst del fund-analyzer dentro de Cowork. NO la uses para ejecutar el pipeline de descarga (CNMV, PDFs, scraping) — eso sigue en Python. NO la uses para fondos que no han pasado antes por la prep determinista (`python -m agents.orchestrator --isin X --prep-only`).
---

# analyst-cowork v2.3

Sustituto del `agents/analyst_agent.py` del proyecto fund-analyzer. Genera el bloque `analyst_synthesis.*` con 8 secciones siguiendo el **schema EXACTO** que espera el dashboard renderer (`dashboard/generate_dashboard.py`). Diseñada para correr bajo Claude Max y eliminar el coste API de Anthropic.

**v2 (2026-05-04)**: corrige 3 schema mismatches críticos descubiertos en smoke test Avantage. Ahora usa exactamente los nombres de campo que el dashboard renderiza. Audit pass obligatorio.

**v2.2 (2026-05-04)**: añade campos que el quality_loop v1 chequea — `estrategia.fortalezas/riesgos/perfil_riesgo` y `cartera.top_posiciones`.

**v2.3 (2026-05-04)**: reglas estrictas de formato en `texto` para evitar headers disruptivos y listas inline apelmazadas (feedback visual usuario).

## REGLAS DE FORMATO en `texto` fields (críticas — v2.4)

Las siguientes pautas aplican a TODOS los campos `texto` y `trayectoria`/`filosofia` largos. **Asume que el dashboard renderiza `**Header**\n\n` como sub-header sutil** (font-size 15px, bold, sin border horizontal, sin caps). Si tu instalación del dashboard aún tiene el render antiguo con border + caps, deja headers en línea con `—` como fallback (versión v2.3).

### Regla 1 (v2.4): SÍ usa `**Header**\n\n` para sub-secciones — el renderer los pinta sutil.

Para dividir secciones largas (>2K chars) en bloques temáticos, usa headers standalone. El dashboard renderiza tipo "subtitle" sin border ni text-transform.

✅ BIEN:
```
... párrafo previo.

**Origen y fundación de Troy (2000)**

Troy Asset Management fue fundada en Londres en el año 2000...

**Lanzamiento del sub-fondo Irlandés (febrero 2012)**

La fecha del lanzamiento es notable...
```

Pautas:
- Headers descriptivos pero no demasiado largos (3-12 palabras).
- Sin "(YYYY)" final salvo cuando aporta contexto temporal claro.
- 1ª letra en mayúscula, resto del header en minúsculas (no all-caps).
- Máximo 3-5 sub-headers por sección. Más es exceso de fragmentación.

### Regla 2: Listas con periodos temporales — cada periodo en su propio párrafo, NO bullets.

Los periodos son párrafos narrativos con cuerpo amplio, no items de lista corta. Mantenlos como párrafos con `**YYYY**:` como label inline.

✅ BIEN — cada periodo es su propio párrafo con label en bold:
```
A lo largo de su historia el fondo ha enfrentado:

**2013-2015**: periodo de discusión sobre tapering de la Fed...

**2022**: año de fuertes correcciones en RV+RF por subidas de tipos...

**2023**: año del rendimiento del 4,8% del bono del Tesoro a 10 años...
```

❌ MAL — todo apelmazado:
```
A lo largo de su historia el fondo ha enfrentado: - 2013-2015: ... - 2022: ...
```

❌ TAMPOCO — bullets para periodos largos (los items quedan como tarjetas extrañas):
```
- **2013-2015**: largo párrafo descriptivo...
- **2022**: largo párrafo descriptivo...
```

### Regla 3 (v2.4): Enumeraciones cortas con label — SÍ usa bullets markdown explícitos.

Cuando enumeras 3-6 criterios/principios/escenarios con cuerpo corto (≤500 chars cada uno), usa lista markdown explícita. El dashboard renderiza como `<ul>` con `<li>` correctos.

✅ BIEN:
```
El proceso combina criterios sistemáticos:

- **Adición contracíclica**: añadir a posiciones existentes y abrir nuevas cuando los mercados caen.
- **Salida disciplinada**: salir de posiciones por incertidumbre sobre el crecimiento futuro de beneficios.
- **Gestión activa de oro**: ajustar la ponderación de metales preciosos según ciclo y valoración relativa.
- **Disciplina de duración**: reducir la duración media ponderada de los bonos en escenarios de tipos en máximos.
```

Diferencia con la Regla 2: aquí cada item es **corto** (1-3 sentences). Si los items son párrafos largos, usa Regla 2 (párrafos con label inline) en vez de bullets.

### Regla 4: `**bold**` solo para énfasis dentro de un sentence o como label de párrafo, NUNCA para envolver párrafos enteros.

❌ MAL: `**Todo este párrafo en bold porque parece importante.**`

✅ BIEN: `Texto normal con **palabras clave** o **conceptos importantes** en bold.`

### Regla 5: Separación de párrafos siempre con `\n\n`.

`\n` simple no separa párrafos en el render del dashboard. Cualquier salto de párrafo debe ser doble newline.

### Regla 6 (v2.4): No abuses de sub-headers. Máximo 3-5 por sección.

Si una sección tiene más de 5 sub-headers, probablemente cabe reorganizar agrupando temas. Más de 5 fragmenta la lectura. Menos de 2 hace que la sección lea como un bloque único difícil de escanear.

## Pre-requisito obligatorio

El usuario debe haber ejecutado primero la prep determinista:
```
cd /ruta/a/fund-analyzer
python -m agents.orchestrator --isin {ISIN} --prep-only
```

Esto deja en `data/funds/{ISIN}/bundle/`:
- `fund_data.json` (cnmv_data.json o intl_data.json normalizado)
- `manager_profile.json`
- `letters_data.json`
- `readings.json`
- `sources.json`
- `bundle_manifest.json`
- `human_feedback.json` **(opcional, T3.7)** — feedback humano APPLIED del usuario sobre el análisis anterior

Si falta cualquiera de los 5 inputs **obligatorios**, ABORTA y pide a Rafa que ejecute la prep antes. `human_feedback.json` es opcional — solo aparece si el usuario ha guardado feedback con `📝 Mejorar este análisis` y lanzado un re-run con `♺` o `--apply-feedback`.

## Human feedback (T3.7, 2026-05-28) — INSTRUCCIÓN PRIORITARIA

Si existe `bundle/human_feedback.json`, **léelo antes de generar cada sección**. Estructura:

```json
{
  "isin": "...",
  "n_relevant_items": 3,
  "items": [
    {
      "feedback_id": "fb_xxx",
      "item_idx": 0,
      "raw_text_hint": "El resumen no menciona la estrategia value...",
      "target_path": null,
      "target_section": "resumen",
      "action": "revisar",
      "value": null,
      "confidence": "high",
      "source_urls": ["https://..."],
      "rationale": "usuario dice que falta tratamiento de value en el resumen"
    }
  ]
}
```

Reglas:
1. **Por cada item con `target_section`**, al generar ESA sección, considera el `rationale` y `raw_text_hint` como instrucción del usuario que PRIORIZA sobre la generación automática previa. Tienes que reflejar el feedback.
2. **Por cada item con `source_urls`** que apliquen a una sección, intenta incorporar la información de esas URLs en `analyst_synthesis.fuentes_externas.texto` o como referencia en la sección target.
3. **Items con `action=revisar` y sin `target_section`** (global): aplica el feedback al contexto general de TODAS las secciones (suele ser feedback de calidad/tono).
4. **Confianza humana > automática**: si el usuario contradice algo que el analyst anterior dijo, el usuario gana.

### Regeneración selectiva (2026-06-06) — IMPORTANTE

Cuando **TODOS** los items de `human_feedback.json` tienen `target_section` (es decir, el feedback apunta a secciones concretas, no es global):

- **Regenera SOLO esas secciones** y emítelas en `analyst_synthesis`. NO regeneres las demás: el consumidor (`orchestrator._consume_cowork_analyst`) preservará verbatim las secciones que NO incluyas, copiándolas del análisis anterior. Esto evita empeorar pestañas que el usuario no pidió tocar (caso real: feedback sobre `cartera`/`evolucion` no debe alterar `gestores`) y ahorra tokens.
- Si **algún** item es global (sin `target_section`), regenera TODAS las secciones como siempre.

### Veredicto por item (2026-06-06) — OBLIGATORIO: `_meta.feedback_outcomes`

Por **cada** item de `human_feedback.json` que proceses, añade una entrada a `_meta.feedback_outcomes` declarando HONESTAMENTE si conseguiste lo que el usuario pidió:

```json
"feedback_outcomes": [
  {
    "feedback_id": "fb_xxx",
    "item_idx": 2,
    "resolved": false,
    "reason": "La cartera real del fondo solo reporta 8 posiciones en la última fuente disponible (informe Dic-2024), no 10. No puedo inventar 2 posiciones más. El número '10' que aparecía era un error de plantilla, ya corregido a 8 en la narrativa."
  },
  {
    "feedback_id": "fb_xxx",
    "item_idx": 0,
    "resolved": true,
    "reason": "Añadida la evolución completa de AUM por periodo en evolucion.datos_graficos y narrada en evolucion.texto."
  }
]
```

Reglas del veredicto:
- `resolved: true` SOLO si de verdad corregiste/mejoraste lo que el usuario señaló. `reason` = qué cambiaste.
- `resolved: false` si NO pudiste (faltan datos en el bundle, el dato pedido no existe, la fuente contradice lo pedido). `reason` = por qué no, con concreción (qué fuente, qué cifra real). **Es preferible un `false` honesto que un `true` falso**: el `false` se muestra al usuario en ámbar con tu razón, para que sepa que ese punto sigue pendiente y por qué.
- `feedback_id` y `item_idx` deben coincidir EXACTAMENTE con los del item en `human_feedback.json` (la verificación los emparea por esos dos campos).
- No inventes datos para poder marcar `resolved: true`. La honestidad del veredicto es el objetivo de este campo.

## Inputs cualitativos del bundle (CRÍTICOS para narrativa)

`fund_data.json` (renombrado de cnmv_data.json para fondos ES, intl_data.json para INT) contiene un campo `cualitativo` con texto pre-extraído de los PDFs semestrales CNMV o annual reports INT. **DEBES leer y USAR estos campos** para enriquecer las secciones narrativas. Si no los usas, las secciones saldrán pobres y genéricas.

Campos planos (texto del último periodo disponible):

- `fund_data.cualitativo.contexto_mercado` (string ~150-250 palabras): visión de la gestora sobre el entorno macro y mercado durante el último periodo. **Usar en**: `resumen.texto`, `evolucion.texto`, `estrategia.texto` para anclar el análisis al contexto real.
- `fund_data.cualitativo.decisiones_tomadas` (string ~100-300 palabras): decisiones de inversión del último periodo (compras, ventas, ajustes), con nombres de activos. **Usar en**: `cartera.texto` (narrativa de movimientos recientes), `estrategia.texto` (cómo se ejecuta la tesis).
- `fund_data.cualitativo.tesis_gestora` (string ~100-200 palabras, opcional): tesis o filosofía expresada en este periodo. **Usar en**: `resumen.filosofia_inversion`, `estrategia.texto`.
- `fund_data.cualitativo.perspectivas` (string ~100-200 palabras, opcional): outlook expresado para el próximo periodo. **Usar en**: `evolucion.texto` (cierre prospectivo), `estrategia.texto` (visión a futuro).

Campo histórico (todos los periodos):

- `fund_data.cualitativo._historico` (dict por periodo: `{"2024_H2": {contexto_mercado: ..., decisiones_tomadas: ..., ...}, "2023_H2": {...}, ...}`). **Usar en**:
  - `historia.texto`: narrar la evolución del fondo periodo a periodo, citando contexto y decisiones de cada año (esto es lo que da riqueza a la cronología).
  - `historia.hitos[]`: cada periodo con cambios significativos puede ser un hito (`{anio, titulo, evento, tipo: "contexto_mercado"}` o `tipo: "cambio_estrategia"`).
  - `evolucion.texto`: hilar la evolución de AUM/posiciones con el contexto de cada periodo.

### Reglas de uso

1. **Cita del periodo**: cuando uses datos del histórico, indica el periodo entre paréntesis: `"En 2024 H2 la gestora destacó (...)" `.
2. **No inventar**: si un campo es null, NO inventes. Si todo el histórico está vacío, di explícitamente que no hay datos cualitativos disponibles.
3. **Prioridad reciente**: para `resumen` y `estrategia` (visión actual), usa los campos planos (último periodo). Para `historia` y `evolucion` usa `_historico` (todos).
4. **Diferenciación ES vs INT**:
   - ES: estos campos vienen de PDFs semestrales CNMV (sección 9, sección 10 perspectivas).
   - INT: estos campos pueden venir de annual reports / factsheets / commentaries del sub-fondo. Mismo schema, distinta procedencia.

## Schema EXACTO del output (no inventes nombres de campos)

Producir un único fichero JSON: `data/funds/{ISIN}/analyst_synthesis_cowork.json`

```json
{
  "_meta": {
    "isin": "...",
    "generated": "ISO timestamp",
    "generator": "skill:analyst-cowork",
    "skill_version": "2.0.0",
    "main_model": "opus",
    "audit_model": "sonnet",
    "sections_generated": [...],
    "audit_pass_done": true,
    "audit_iterations": 1,
    "anti_invencion_flagged": [],
    "input_files_hash": {...},
    "feedback_outcomes": []
  },
  "analyst_synthesis": {
    "resumen": {...},
    "historia": {...},
    "gestores": {...},
    "evolucion": {...},
    "estrategia": {...},
    "cartera": {...},
    "fuentes_externas": {...},
    "documentos": {...}
  }
}
```

### resumen (campos obligatorios — usa EXACTAMENTE estos nombres)

```json
{
  "texto": "1500-2000 chars de narrativa principal. Cita fuentes entre paréntesis. Densa.",
  "filosofia_inversion": "1000-1400 chars. Filosofía detallada del gestor.",
  "criterios_inversion": [
    {"titulo": "...", "descripcion": "..."},
    ...3-5 criterios
  ],
  "fortalezas": ["frase 1", "frase 2", ...4-6 strings cada uno 100-300 chars],
  "riesgos": ["frase 1", "frase 2", ...3-5 strings cada uno 100-300 chars],
  "para_quien_es": "300-500 chars. Perfil del inversor adecuado.",
  "compromiso_gestor": "200-400 chars. Skin in the game, alineación, propiedad.",
  "signal": "POSITIVO | NEUTRAL | NEGATIVO",
  "signal_rationale": "200-300 chars. Por qué esa señal."
}
```

### historia

```json
{
  "texto": "4000-6000 chars. Narrativa cronológica del fondo desde inicio hasta hoy.",
  "hitos": [
    {"anio": "2014", "titulo": "...", "evento": "...", "tipo": "lanzamiento|premio|cambio_gestor|cambio_estrategia|hecho_relevante"},
    ...8-15 hitos
  ]
}
```

`tipo` opciones: `lanzamiento, premio, cambio_gestor, cambio_estrategia, cambio_owner, hecho_relevante, contexto_mercado, otro`.

### gestores (CRÍTICO — schema exacto, no usar `biografia`)

```json
{
  "texto": "10000-18000 chars. Narrativa profunda del equipo gestor.",
  "perfiles": [
    {
      "nombre": "Juan Gómez Bada",
      "cargo": "CEO y Director de Inversiones",
      "trayectoria": "Texto largo (1500-3000 chars) en MARKDOWN con **bold** para énfasis. Cuenta su carrera, formación, experiencia, reconocimientos.",
      "filosofia": "Texto (500-1000 chars) sobre su filosofía de inversión personal. NO duplicar con resumen.filosofia_inversion (este es del GESTOR, aquel es del FONDO).",
      "cv_bullets": [
        "CEO y Director de Inversiones de Avantage Capital",
        "Rating AA Citywire",
        "Fondo Avantage Fund con 5 estrellas Morningstar",
        "+15 años de experiencia",
        ...4-8 bullets cortos
      ],
      "decisiones_clave": [
        "Decisión 1 con contexto: qué hizo y por qué (200-400 chars)",
        ...3-5 decisiones
      ],
      "rasgos_diferenciales": "300-500 chars. Qué le distingue de otros gestores.",
      "fuente": "manager_profiler"
    },
    ...incluir TODOS los miembros del equipo (lead + co-managers + IR si aplica)
  ]
}
```

**REGLAS GESTORES**:
- `nombre` y `cargo` SON OBLIGATORIOS para todos los perfiles.
- `trayectoria` es OBLIGATORIO para los 1-2 perfiles lead. Para miembros secundarios puede ser más breve (300-600 chars).
- `cv_bullets` es OBLIGATORIO para lead. Opcional para secundarios.
- NO inventes empresas previas o años no presentes en `manager_profile.json`. Si no sabes dónde trabajó antes, no lo digas.
- `fuente` debe ser uno de: `manager_profiler, manager_deep_agent, google_snippet, sibling_auto, manual_verificado, analyst_llm`.

### evolucion

```json
{
  "texto": "5000-9000 chars. Narrativa de la evolución del fondo: AUM, partícipes, rentabilidad, posiciones, cambios estratégicos en el tiempo.",
  "datos_graficos": {
    "concentracion_historica": [
      {"periodo": "2024", "top5_pct": 23.5, "top10_pct": 38.2, "top15_pct": 49.8},
      ...
    ],
    "drawdown": [{"periodo": "...", "drawdown_pct": ...}],
    "exposicion_geografica": [{"periodo": "...", "espana_pct": ..., "internacional_pct": ...}],
    "num_posiciones_por_anio": [{"anio": "...", "num": ...}],
    "rentabilidades_anuales": [{"anio": "2024", "fondo_pct": ..., "ibex_pct": ..., "sp500_eur_pct": ...}]
  }
}
```

`datos_graficos` debe poblarse con datos REALES extraídos de `fund_data.cuantitativo` y `letters_data.cartas[*]`. Si no tienes el dato, omite la entry. NO inventes números.

### estrategia

```json
{
  "texto": "6000-10000 chars. Narrativa profunda de la estrategia. Sub-secciones con **bold** headers.",
  "estrategia_actual_resumen": "200-400 chars. Resumen de la estrategia hoy.",
  "fortalezas": [
    "frase 1 (100-300 chars)",
    "frase 2",
    ...4-6 strings — DUPLICAN los de resumen.fortalezas (el dashboard renderiza ambos sitios)
  ],
  "riesgos": [
    "frase 1 (100-300 chars)",
    ...3-5 strings — DUPLICAN los de resumen.riesgos
  ],
  "perfil_riesgo": {
    "tipo_activo_principal": "Mixto Flexible Global / Renta Variable Global / Renta Fija / etc",
    "riesgos_especificos": ["riesgo 1", "riesgo 2", "riesgo 3"],
    "desglose_exposicion": [
      {"dimension": "geografía", "detalle": "60% internacional, 20% España, 16% Argentina, 4% otros"},
      {"dimension": "sectores", "detalle": "..."}
    ]
  },
  "hitos_estrategia": [
    {
      "periodo": "2025-S2",
      "contexto_mercado": "300-500 chars. Qué pasaba en el mercado.",
      "decisiones": "300-600 chars. Qué decidió el gestor.",
      "resultado": "+X.XX% — driver explicativo (200-400 chars)"
    },
    ...4-8 hitos cubriendo años con cartas K15
  ],
  "quotes": [
    {
      "texto": "cita literal entre 30-200 chars",
      "autor": "Juan Gómez Bada",
      "contexto": "Carta Semestral julio 2025"
    },
    ...3-6 quotes
  ]
}
```

**REGLAS estrategia** (v2.2):
- Los `quotes` deben ser CITAS LITERALES extraídas de `letters_data.cartas[*].texto_completo`. Si no encuentras citas reales, devuelve lista vacía (no inventes).
- `fortalezas` y `riesgos` se DUPLICAN aquí en estrategia (también en resumen). El v1 dashboard renderiza ambos sitios. NO es redundancia inocua — el quality_loop chequea los dos.
- `hitos_estrategia[].resultado` debe seguir el formato `"+X.XX% — driver"` (cifra + por qué). Sin driver explicativo, el quality_loop lo flaggea.
- `perfil_riesgo` es OBLIGATORIO con los 3 sub-campos. Sin esto, el quality_loop reporta "Perfil de riesgo de la estrategia incompleto".

### cartera

```json
{
  "texto": "5000-8000 chars. Narrativa COMPOSICIONAL de la cartera: tipo de activos, sectores, geografía, racional general. NO enumerar las posiciones individuales — la tabla del dashboard ya las muestra. Foco en composición + racional + riesgos.",
  "top_posiciones": [
    {"nombre": "...", "peso_pct": 6.05, "categoria": "Real estate / Tech / Banca / etc"},
    ...10 entries — DUPLICAN top10 de posiciones.actuales[] del top-level del schema
  ],
  "concentracion": {
    "top5_pct": 23.5,
    "top10_pct": 38.2,
    "top15_pct": 49.8
  },
  "concentracion_historica": [
    {"periodo": "2024", "top5_pct": ..., "top10_pct": ..., "top15_pct": ..., "fuente": "fund_data"},
    ...8-15 entries
  ],
  "distribucion_tipo": {
    "rv_internacional_pct": 60.0,
    "rv_españa_pct": 20.3,
    "rf_pct": 18.7,
    "liquidez_pct": 1.0
  }
}
```

**REGLAS cartera** (v2.2):
- `top_posiciones` es OBLIGATORIO con 10 entries duplicando los top10 de `posiciones.actuales` (datos top-level). El quality_loop chequea esto. NO es redundancia con el dashboard — es input para el quality_check, no para visualización (el dashboard sigue leyendo de `posiciones.actuales`).

**REGLAS cartera.texto** (críticas — feedback v2.1):
- **NO ENUMERAR posiciones individuales con sus pesos en el texto**. La tabla de `posiciones.actuales` (top-level del schema, top-level del dashboard) ya las muestra. Repetirlo es redundancia visual molesta para el lector.
- El texto debe ser **un párrafo resumen sobre composición**: tipo de activo (RV vs RF vs liquidez), distribución geográfica (zonas y tesis por zona), distribución sectorial general, concentración relativa (top-N como % del patrimonio sin nombrar valores), racional global de la cartera y riesgos estructurales actuales.
- Cuando menciones una posición concreta porque ilustra un punto del racional (p.ej. "exposición a Argentina vía empresas como X e Y"), hazlo con moderación y SIN poner el peso. El peso lo da la tabla.
- Cifras de `concentracion` y `distribucion_tipo` SOLO de `fund_data.posiciones.actuales` y `fund_data.cuantitativo.mix_activos_historico`. NO inventes.

### fuentes_externas (CRÍTICO — usa `opiniones_clave`, no `lecturas_destacadas`)

```json
{
  "texto": "5000-9000 chars. Síntesis cualitativa de qué dicen las fuentes externas sobre el fondo.",
  "opiniones_clave": [
    {
      "fuente": "Substack Salud Financiera",
      "titulo": "Análisis de Avantage Fund - Salud Financiera",
      "url": "https://saludfinanciera.substack.com/p/analisis-avantage-fund",
      "tipo": "analisis | review | interview | rating | comunidad | podcast | video",
      "opinion": "200-400 chars. Síntesis de lo que dice esta fuente.",
      "fecha": "2025"
    },
    ...8-15 opiniones
  ]
}
```

**REGLAS fuentes_externas**:
- Lee `readings.readings[]` del bundle.
- Filtra ruido (videos de música irrelevantes, posts genéricos sin contenido).
- Para cada reading legítimo, escribe su `opinion` sintetizada (no copies, sintetiza).
- `tipo` debe ser uno de: `analisis, review, interview, rating, comunidad, podcast, video, articulo`.

### documentos (CRÍTICO — schema URL-based, no descriptivo)

```json
{
  "informes_pdf": [
    {"archivo": "CNMV_ES0112231008_2024_H2.pdf"},
    ...todos los pdfs en fund_data.fuentes.informes_descargados
  ],
  "xmls_cnmv": [
    {"archivo": "Abril_FONDMENS_202504.xml"},
    ...todos los xmls en fund_data.fuentes.xmls_cnmv
  ],
  "cartas_urls": [
    "https://www.avantagecapital.com/carta-semestral-a-los-inversores-enero-2025/",
    ...todas las URL únicas en letters_data.cartas[].url_fuente
  ],
  "fuentes_externas_urls": [
    "https://saludfinanciera.substack.com/p/analisis-avantage-fund",
    ...todas las URL únicas en readings.readings[].url
  ],
  "urls_consultadas": [
    "https://www.cnmv.es/portal/Consultas/IIC/Fondo.aspx?isin=ES0112231008",
    ...URLs de fund_data.fuentes.urls_consultadas (CNMV portal, regulator portals)
  ],
  "total_fuentes": 127
}
```

**REGLAS documentos**:
- `documentos` es 100% AGREGACIÓN MECÁNICA, no narrativa. NO incluyas `texto` ni descripciones.
- `informes_pdf` y `xmls_cnmv`: extrae directo de `fund_data.fuentes.{informes_descargados, xmls_cnmv}` (cada item es `{archivo: "..."}`).
- `cartas_urls`: deduplica `letters_data.cartas[].url_fuente`, ordena.
- `fuentes_externas_urls`: deduplica `readings.readings[].url`, ordena. Filtra URLs claramente ruidosas (videos no relacionados, etc.).
- `urls_consultadas`: típicamente 1-2 URLs del portal del regulador.
- `total_fuentes` = suma de longitudes de las 5 listas anteriores.

## Workflow paso a paso

### 1. Validación de pre-requisitos (1 turn)

Bash:
```
ISIN={ISIN}
cd /ruta/a/fund-analyzer
ls -la data/funds/$ISIN/bundle/
```

Si falta el bundle o cualquiera de los 5 inputs → aborta y pide ejecutar prep. Si OK → continúa.

### 2. Lectura del schema (1 turn)

Lee `docs/cowork_handoff/CLAUDE.md` secc