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

Si falta cualquiera de los 5 inputs, ABORTA y pide a Rafa que ejecute la prep antes.

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
    "input_files_hash": {...}
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

Lee `docs/cowork_handoff/CLAUDE.md` secciones 1-2 para refrescar el schema canónico de `output.json`. NO inventes paths.

### 3. Lectura paralela de inputs (1 turn)

Lee los 5 ficheros del bundle en paralelo. Si alguno >150K tokens, usa offset+limit para leer las partes clave (KPIs+posiciones+cualitativo de fund_data, perfiles+articulos de manager_profile, cartas más recientes de letters_data).

Para `letters_data.json` (que puede ser muy grande), prioriza:
- Las 4-6 cartas semestrales más recientes con texto completo
- Cualquier carta con K15 estructurado (`tesis_gestora, decisiones_tomadas, citas_textuales`)
- Lista de todas las cartas (periodos + URLs) para el sección documentos

### 4. Generación de las 8 secciones (1 turn por sección, secuencial)

Por cada sección en este orden:

1. `resumen` — narrativa principal + filosofía + criterios + fortalezas + riesgos + para_quien_es + compromiso_gestor + signal/rationale
2. `historia` — texto cronológico + hitos[] (incluir hechos relevantes CNMV de informacion_cnmv.hechos_relevantes)
3. `gestores` — texto + perfiles[] con trayectoria/filosofia/cv_bullets/decisiones_clave/rasgos_diferenciales por gestor
4. `evolucion` — texto + datos_graficos[] estructurados desde fund_data.cuantitativo
5. `estrategia` — texto + estrategia_actual_resumen + hitos_estrategia[] desde cartas K15 + quotes literales
6. `cartera` — texto + concentracion + distribucion_tipo + concentracion_historica desde posiciones
7. `fuentes_externas` — texto + opiniones_clave[] (no lecturas_destacadas)
8. `documentos` — agregación mecánica de URLs (no LLM, solo agregación)

**Reglas duras NO negociables**:
- Atribución entre paréntesis: "(Carta Semestral enero 2026)", "(CNMV)", "(manager_profiler)".
- Anti-invención: si una entidad/cifra/cita NO aparece en ningún input → NO la incluyas. Especialmente cuidado con:
  - Empresas donde trabajó previamente un gestor (NO inventes "Banco Popular 2004-2014" si manager_profile no lo dice).
  - CEOs de empresas en cartera (NO inventes "CEO Gianluca Garbi" para IRSA si no está en inputs).
  - Cifras (% de cualquier tipo) — copia literal de los inputs.
- Citas literales (entre comillas) DEBEN aparecer literal en `letters_data.cartas[].texto_completo` o `readings.readings[].resumen|opinion`.
- Markdown bold se renderiza: usa `**texto**` para énfasis en `texto`/`trayectoria`/`filosofia`. NO uses `**` para envolver párrafos enteros.
- **Longitud mínima por sección**: cubre el rango orientativo del schema. Las secciones cortas se ven pobres en el dashboard. Si te quedas corto, expande con datos reales del bundle (no rellenes con genericidades).

### 5. Pase de auditoría con Sonnet en subagente (OBLIGATORIO — 1 turn)

NO ES OPCIONAL. Si saltas este paso, marca `audit_pass_done: false` y avisa al usuario explícitamente al final que la skill quedó incompleta.

Lanza el subagente:

```
Agent({
  description: "Audit anti-invención analyst",
  subagent_type: "general-purpose",
  model: "sonnet",
  prompt: `Eres auditor independiente. Tu trabajo es validar que el draft del analyst NO contiene entidades, cifras o citas inventadas.

INPUTS QUE PUEDES VERIFICAR (lee solo estos):
- data/funds/{ISIN}/bundle/fund_data.json
- data/funds/{ISIN}/bundle/manager_profile.json
- data/funds/{ISIN}/bundle/letters_data.json
- data/funds/{ISIN}/bundle/readings.json
- data/funds/{ISIN}/bundle/sources.json

DRAFT A AUDITAR:
{pega aquí el JSON con las 8 secciones}

PARA CADA SECCIÓN, lista:
1. Entidades nombradas (gestores, empresas previas, instituciones, CEOs) que NO aparecen en los inputs.
2. Cifras (%, €, fechas, edades, años) que no se pueden verificar contra los inputs.
3. Citas entre comillas que no aparecen literal en letters_data o readings.
4. Atribuciones a fuentes (ej. "según Morningstar") que no respaldan ningún input.

CASOS HISTÓRICOS A VIGILAR ESPECIALMENTE (lecciones de smoke tests previos):
- Empresas previas de gestores no respaldadas
- CEOs de empresas en cartera inventados (verificar con searches en inputs)

Output formato JSON estricto:
{
  "issues": [
    {"section": "gestores", "type": "entidad_inventada", "snippet": "...", "evidence": "no aparece en manager_profile.json"},
    ...
  ],
  "verdict": "clean" | "minor_issues" | "major_issues"
}

Si verdict=clean, devuelve {"issues": [], "verdict": "clean"}. Sé estricto: ante la duda, marca como issue.`
})
```

Espera el resultado.

### 6. Aplicar correcciones (1 turn por sección con issues, máximo 3)

Si `verdict == "clean"` → pasa al paso 7.

Si hay issues:
- Por cada sección con issues, regenera SOLO esa sección eliminando o corrigiendo los snippets flagged. Re-lee el input correspondiente para confirmar qué SÍ está respaldado.
- Máximo 3 secciones revisables. Si hay >3 issues → registra todos en `_meta.anti_invencion_flagged`.
- Una sola iteración. NO bucle.

### 7. Ensamblado y escritura del JSON final (1 turn)

Construye el dict final. Calcula `input_files_hash` (sha256 de cada uno de los 5 inputs). Escribe a `data/funds/{ISIN}/analyst_synthesis_cowork.json`.

`_meta.audit_pass_done` debe ser `true` si ejecutaste paso 5. `false` solo si falló el subagente (y entonces deja `anti_invencion_flagged: ["audit_failed"]`).

### 8. Mensaje final a Rafa

Confirma:
- Path del JSON.
- Resumen: 8 secciones generadas, audit verdict, N issues residuales.
- Comando consume:
  ```
  python -m agents.orchestrator --isin {ISIN} --consume-cowork
  ```

NO ejecutes consume automáticamente.

## Modelo recomendado

- Sesión principal: Opus.
- Subagente audit: Sonnet (forzado vía `model: "sonnet"`).

## Coste y rate limit

- Por fondo: 300-500K tokens (output schema más rico que v1).
- Bajo Max: 0€ marginal, ~12-15 mensajes de cuota 5h.
- Volumen: 5-12 fondos por ventana.

## Errores comunes a evitar

1. **Usar `biografia` en gestores.perfiles**: el campo correcto es `trayectoria` (texto largo MD).
2. **Usar `lecturas_destacadas` en fuentes_externas**: el campo correcto es `opiniones_clave`.
3. **Usar `lista` descriptiva en documentos**: el campo correcto es `informes_pdf`/`cartas_urls`/etc. con URLs.
4. **Saltar audit pass**: NO opcional. Si no lo ejecutas, marca audit_pass_done=false y avisa.
5. **Rellenar longitud con genericidades**: si te quedas corto, expande con DATOS REALES del bundle, no con texto vacío.
6. **Inventar empresas previas de gestores**: si el manager_profile dice "más de 15 años" sin más, NO inventes Banco Popular ni nada.
7. **Inventar CEOs**: nunca atribuyas un nombre de CEO a una empresa en cartera salvo que aparezca explícito en algún input.
8. **Olvidar atribuir fuentes**: cada cifra y cada cita debe tener (Carta X), (CNMV), (Morningstar), etc.
