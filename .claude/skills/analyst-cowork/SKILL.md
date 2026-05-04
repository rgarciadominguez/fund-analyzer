---
name: analyst-cowork
description: Genera el bloque `analyst_synthesis.*` (8 secciones narrativas) de un fondo del proyecto fund-analyzer usando la cuota de Claude Max en lugar de la API Anthropic. Reemplaza al `agents/analyst_agent.py` de 5197 LOC. Úsala SIEMPRE que Rafa diga "analyst cowork", "analiza fondo X con cowork", "regenera síntesis de X via skill", "skill analyst X", "consume preview de X", "monta el analyst de X aquí", o cualquier variante sobre ejecutar la síntesis del analyst del fund-analyzer dentro de Cowork. NO la uses para ejecutar el pipeline de descarga (CNMV, PDFs, scraping) — eso sigue en Python. NO la uses para fondos que no han pasado antes por la prep determinista (`python -m agents.orchestrator --isin X --prep-only`).
---

# analyst-cowork

Sustituto del `agents/analyst_agent.py` del proyecto fund-analyzer. Genera las 8 secciones de `analyst_synthesis.*` consumiendo los JSONs intermedios producidos por la prep determinista en Python. Diseñada para correr bajo Claude Max (Cowork o Claude Code) y eliminar el coste API de Anthropic.

## Pre-requisito obligatorio

El usuario debe haber ejecutado primero la prep determinista del pipeline:
```bash
cd /ruta/a/fund-analyzer
python -m agents.orchestrator --isin {ISIN} --prep-only
```
(Ver `INSTALL.md` para añadir el flag `--prep-only` al orchestrator.)

Esto deja en `data/funds/{ISIN}/`:
- `cnmv_data.json` (ES) **o** `intl_data.json` (INT)
- `manager_profile.json` (con `articulos_completos`)
- `letters_data.json` (cartas K15 estructuradas)
- `readings.json` (análisis externos sintetizados)
- `sources.json` (fuentes pro identificadas)
- `output.json` parcial (top-level + meta, sin `analyst_synthesis`)

Si falta cualquiera de los cuatro JSONs centrales, ABORTA y pide a Rafa que ejecute la prep antes.

## Inputs que la skill DEBE consumir

| Path | Contenido | Uso |
|---|---|---|
| `data/funds/{ISIN}/cnmv_data.json` o `intl_data.json` | KPIs, posiciones, cualitativo CNMV | Base de `resumen`, `evolucion`, `estrategia`, `cartera`, KPIs en `historia` |
| `data/funds/{ISIN}/manager_profile.json` | Equipo, perfiles, `articulos_completos`, `equipo_roles` con lead/co | `gestores.perfiles` y `gestores.texto` |
| `data/funds/{ISIN}/letters_data.json` | Cartas trimestrales con K15 (tesis, decisiones, contexto, citas) | `evolucion`, `estrategia`, citas en `cartera` |
| `data/funds/{ISIN}/readings.json` | Análisis Morningstar/Citywire/blogs pro estructurados | `fuentes_externas`, contraste en `gestores` |
| `data/funds/{ISIN}/sources.json` | Catálogo de fuentes consideradas válidas | `documentos`, atribuciones |
| `docs/cowork_handoff/CLAUDE.md` | Schema canónico + convenciones | LEER PRIMERO. No improvisar el schema. |

## Output que la skill DEBE producir

Un único fichero JSON: `data/funds/{ISIN}/analyst_synthesis_cowork.json`

Estructura exacta (ver `output_schema.example.json` adjunto):
```json
{
  "_meta": {
    "isin": "...",
    "generated": "2026-05-04T...",
    "generator": "skill:analyst-cowork",
    "main_model": "opus",
    "audit_model": "sonnet",
    "sections_generated": ["resumen", "historia", ...],
    "audit_pass_done": true,
    "anti_invencion_flagged": [],
    "input_files_hash": {...}
  },
  "analyst_synthesis": {
    "resumen": {"texto": "...", "filosofia_inversion": "...", "criterios_inversion": [...]},
    "historia": {"texto": "...", "hitos": [...]},
    "gestores": {"perfiles": [...], "texto": "..."},
    "evolucion": {"texto": "...", "puntos_clave": [...]},
    "estrategia": {"texto": "...", "fortalezas": [...], "riesgos": [...]},
    "cartera": {"texto": "...", "top_posiciones": [...], "citas": [...]},
    "fuentes_externas": {"texto": "...", "lecturas_destacadas": [...]},
    "documentos": {"texto": "...", "lista": [...]}
  }
}
```

Después de escribir el fichero, indica a Rafa que ejecute:
```bash
python -m agents.orchestrator --isin {ISIN} --consume-cowork
```
Esto integra el analyst en `output.json`, corre validators + quality rules + genera el dashboard HTML.

## Workflow paso a paso

### 1. Validación de pre-requisitos (1 turn)

Ejecuta en Bash:
```bash
ISIN={ISIN}
cd /ruta/a/fund-analyzer
ls -la data/funds/$ISIN/{cnmv_data.json,intl_data.json,manager_profile.json,letters_data.json,readings.json,sources.json} 2>/dev/null
```

Si falta algo crítico (cnmv O intl, los demás obligatorios) → aborta y pide ejecutar prep. Si OK → continúa.

### 2. Lectura de schema canónico (1 turn)

Lee `docs/cowork_handoff/CLAUDE.md` sección 1 (schema `output.json`) y sección 11 (manager_profile.json schema). Esto te recuerda los paths exactos esperados. NO inventes campos.

### 3. Lectura paralela de inputs (1 turn, llamadas Read en paralelo)

Lee los 5 JSONs de inputs en una sola tanda. Si alguno >50K tokens, léelo entero igualmente — Opus tiene 200K de contexto. Si tras leer los 5 superas 150K, usa subagentes (paso 4 alternativo).

### 4. Generación de las 8 secciones (1 turn por sección, secuencial)

Por cada sección en este orden — DETERMINISTA, no cambies:
1. `resumen` (texto + filosofia_inversion + criterios_inversion[])
2. `historia` (texto + hitos[] cronológicos)
3. `gestores` (perfiles[] + texto narrativo)
4. `evolucion` (texto + puntos_clave[])
5. `estrategia` (texto + fortalezas[] + riesgos[])
6. `cartera` (texto + top_posiciones[] + citas[] de cartas)
7. `fuentes_externas` (texto + lecturas_destacadas[])
8. `documentos` (texto + lista[])

Reglas de cada sección (no negociables):
- **Atribuir fuente** entre comillas o con paréntesis: "(Carta Q4 2025)", "(Annual Report 2024 p.47)", "(Morningstar)". No inventar fuentes.
- **Anti-invención**: si una entidad (gestor, posición, cifra) NO aparece en ninguno de los 5 JSONs leídos → no la incluyas. No completes huecos con texto genérico tipo "se considera un fondo equilibrado".
- **Cifras literales**: copia AUM, TER, %, fechas exactamente como aparecen en las fuentes. Si hay inconsistencia entre fuentes, marca con paréntesis "(según CNMV)" / "(según AR 2024)".
- **Markdown bold se renderiza** en el dashboard: usa `**texto**` para énfasis, no para llenar.
- **Largo orientativo por sección** (sin pasarse): resumen 600-1200 palabras, historia 800-1500, gestores 500-1000 + perfiles, evolucion 400-800, estrategia 500-900 con bullets, cartera 400-700 + tabla, fuentes_externas 200-400, documentos 100-300 + lista.

Tras cada sección, guárdala en una variable de trabajo (no escribas el JSON final aún).

### 5. Pase de auditoría con Sonnet en subagente (1 turn)

Una vez tienes las 8 secciones en draft, lanza UN subagente Sonnet con el Agent tool:

```
Agent({
  description: "Audit anti-invención analyst",
  subagent_type: "general-purpose",
  model: "sonnet",
  prompt: `Eres auditor independiente. Tienes que validar que el draft del analyst NO contiene entidades, cifras o citas inventadas.

INPUTS QUE PUEDES VERIFICAR (lee solo estos):
- data/funds/{ISIN}/cnmv_data.json o intl_data.json
- data/funds/{ISIN}/manager_profile.json
- data/funds/{ISIN}/letters_data.json
- data/funds/{ISIN}/readings.json

DRAFT A AUDITAR:
{pega aquí el JSON con las 8 secciones}

Para cada sección, lista:
1. Entidades nombradas (gestores, empresas, instituciones) que NO aparecen en los inputs.
2. Cifras (%, €, fechas) que no se pueden verificar contra los inputs.
3. Citas entre comillas que no aparecen literal en letters_data.json o readings.json.
4. Atribuciones a fuentes (ej. "según Morningstar") que no respaldan ningún input.

Output formato JSON estricto:
{
  "issues": [
    {"section": "gestores", "type": "entidad_inventada", "snippet": "...", "evidence": "no aparece en manager_profile.equipo ni en articulos_completos"},
    {"section": "cartera", "type": "cita_no_verificable", "snippet": "...", "evidence": "no encontrada en letters_data K15.citas_textuales"}
  ],
  "verdict": "clean" | "minor_issues" | "major_issues"
}

Si verdict=clean, devuelve {"issues": [], "verdict": "clean"}. Sé estricto: ante la duda, marca como issue.`
})
```

Espera el resultado del subagente.

### 6. Aplicar correcciones (1 turn por sección con issues, máximo 3)

Si `verdict == "clean"` → pasa al paso 7.

Si hay issues:
- Por cada sección con issues, regenera SOLO esa sección eliminando o corrigiendo los snippets flagged. Re-lee el input correspondiente para confirmar qué SÍ está respaldado.
- Máximo 3 secciones revisables en este pase. Si hay >3 issues → algo va mal en los inputs, escribe el output igual pero registra todos los issues en `_meta.anti_invencion_flagged` para que Rafa los vea.
- NO repitas el pase de audit: una sola iteración. Si quedan issues residuales, anótalos en `_meta` y avisa a Rafa.

### 7. Ensamblado y escritura del JSON final (1 turn)

Construye el dict final con `_meta` + `analyst_synthesis` y escríbelo a:
```
data/funds/{ISIN}/analyst_synthesis_cowork.json
```

`_meta.input_files_hash` debe contener un hash sha256 de cada input file leído (Bash: `sha256sum`). Esto permite a la prep saber si los inputs cambiaron entre la skill y el consume.

`_meta.sections_generated` lista qué secciones se generaron. Si por algún motivo una se omitió (ej. no había datos para `fuentes_externas` porque `readings.json` estaba vacío), márcalo aquí en lugar de escribir relleno.

### 8. Mensaje final a Rafa

Confirma:
- Path del JSON escrito.
- Resumen de qué contiene (8 secciones generadas con N issues residuales).
- Comando exacto para integrar:
  ```bash
  python -m agents.orchestrator --isin {ISIN} --consume-cowork
  ```
- Si hay anti_invencion_flagged residuales, lístalos para que él decida.

NO ejecutes el `--consume-cowork` automáticamente. Es un side effect sobre `output.json` y debe ser explícito.

## Modelo recomendado

- **Sesión principal**: Opus (configurable en Cowork settings o forzando con `/model opus` en Claude Code).
- **Subagente de audit**: Sonnet (forzado vía `model: "sonnet"` en la llamada Agent).
- **No usar Haiku para nada** dentro de esta skill — la calidad de validación que necesitamos no la garantiza.

Justificación de no usar Opus también para audit: el valor del audit es la *segunda perspectiva*. Si Opus audita Opus, hay sesgo de confirmación. Sonnet como modelo distinto cumple el papel del `_opus_audit_per_section` original (que en la versión Python usaba Opus auditando Sonnet — mismo patrón al revés).

## Coste y rate limit (orientación honesta)

- Por fondo nuevo: 250-400K tokens totales (input+output combinado a través de las ~10-12 turns).
- Equivalente API Sonnet: ~$1.10-$1.75. Equivalente API Opus: ~$3-5. Bajo Max: 0€ marginales pero ~10-15 mensajes de tu cuota de 5h.
- Volumen práctico: 5-15 fondos por ventana de 5h. Para >20 fondos en ráfaga, fragmenta entre ventanas.
- Re-runs: si los inputs no han cambiado (`input_files_hash` igual al run anterior), no tiene sentido regenerar — la skill puede leer el `analyst_synthesis_cowork.json` previo y validar que sigue válido en 1-2 turns en lugar de regenerar 8 secciones.

## Errores comunes y cómo evitarlos

1. **Generar antes de validar pre-requisitos** → siempre paso 1 primero.
2. **Inventar paths de campos** ("kpis.aum_eur", "kpis.aum_total", "aum") → el path canónico es `kpis.aum_actual_meur`. Lee CLAUDE.md sección 2 si dudas.
3. **Auditar con Opus en lugar de Sonnet** → rompe el patrón de segunda mirada. Subagente con `model: "sonnet"` explícito.
4. **Ejecutar `--consume-cowork` automáticamente** → no hacerlo. Devolver el comando al usuario.
5. **Asumir que un fondo INT tiene `cualitativo` top-level** → solo ES tiene eso. INT tiene `_int_cualitativo` (cache). Para INT, lee el cualitativo desde `intl_data.json`.
6. **Olvidar el `articulos_completos` del manager_profile** → es la mina de oro para `gestores.perfiles` (CV con bios reales, no inventos). Léelo entero.
7. **Markdown sin renderizar**: usa `**bold**` literal en los textos. El dashboard lo convierte a `<strong>`. NO uses HTML directo.

## Cierre obligatorio

Cuando termines (escrito el JSON + dado el comando consume al usuario), no invoques otras skills. Esta tarea acaba con el handoff a Python.

Si Rafa pide después "regenera la sección X" → es una invocación nueva de esta skill con un argumento adicional. Carga el `analyst_synthesis_cowork.json` previo, regenera SOLO la sección pedida, vuelve a hacer audit pass solo sobre esa sección, y reescribe el fichero.
