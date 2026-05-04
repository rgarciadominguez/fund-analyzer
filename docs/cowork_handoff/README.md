# Fund Analyzer

Sistema multi-agente para análisis de fondos de inversión españoles e internacionales a partir de su ISIN. Genera un `output.json` consolidado y un dashboard HTML por fondo.

**Estado**: producción para fondos ES (CNMV) e INT (LU/IE/FR/DE vía regulator router). Fase K (2026-04-29) consolidó el sistema con auditoría sistémica.

---

## Quickstart

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar API keys
cp .env.example .env  # editar con ANTHROPIC_API_KEY, GOOGLE_API_KEY, SERPER_API_KEY

# 3. Analizar un fondo (~10-25 min, ~$0.40 LLM)
python -m agents.orchestrator --isin ES0112231008 --auto

# 4. Generar dashboard HTML
python dashboard/generate_dashboard.py ES0112231008

# 5. Abrir en navegador
start dashboard/fund-ES0112231008.html  # Windows
open dashboard/fund-ES0112231008.html    # macOS
xdg-open dashboard/fund-ES0112231008.html  # Linux
```

---

## Arquitectura

```
ISIN → orchestrator.py (coordinador secuencial)
  ├── Paso 0:  regulator_router (solo INT)         → cssf/cbi/amf/bundesanzeiger_data.json
  ├── Paso 1b: cnmv_agent (ES) | intl_extractor_v2 → cnmv_data.json | intl_data.json
  ├── Paso 2:  sources_agent                       → sources.json
  ├── Paso 3:  paralelo letters + readings + manager_profiler
  ├── Paso 3a: manager_deep_agent (enrich K1)      → merge en manager_profile.json
  ├── Paso 3b: letters_deep_agent
  ├── Paso 4:  analyst_agent (8 secciones LLM)     → output.json
  ├── Paso 5-6: validation + meta agents
  ├── Paso 7:  quality_loop (max 2 iter, anti-regresión K3)
  └── Paso 8:  publication_calendar + _meta block
```

**Pipeline ES** (CNMV): `cnmv_agent → analyst_agent`. Lee XMLs bulkdata + parsea PDFs semestrales.

**Pipeline INT**: `regulator_router → discovery_v2 → intl_extractor_v2 → analyst_agent`. Routing por prefijo ISIN (LU→CSSF, IE→CBI, FR→AMF, DE→Bundesanzeiger, otros→discovery puro).

---

## Output

Cada fondo genera `data/funds/{ISIN}/output.json` con:
- **Top-level RAW**: `nombre`, `gestora`, `kpis` (AUM, TER, partícipes…), `posiciones`, `cuantitativo` (series temporales)
- **`analyst_synthesis`**: `resumen`, `historia`, `gestores` (perfiles), `evolucion`, `estrategia`, `cartera`, `fuentes_externas`, `documentos`
- **`publication_calendar`**: cadencia detectada (annual/semiannual/quarterly_letters) + `next_expected_date`
- **`_meta`**: pipeline_version, sources_attempted, anti_invencion_warnings_count

Schema canónico documentado en [CLAUDE.md](CLAUDE.md) sección 1.

---

## Archivos clave

| Path | Rol |
|---|---|
| `agents/orchestrator.py` | Coordinador del pipeline |
| `agents/cnmv_agent.py` | Datos cuantitativos + cualitativos CNMV (ES) |
| `agents/intl_extractor_v2.py` | Extractor concept-first 2-stage (INT) |
| `agents/regulator_router.py` | Dispatch INT por prefijo ISIN |
| `agents/letters_collector.py` + `letters_deep_agent.py` | Cartas trimestrales del gestor |
| `agents/readings_collector.py` | Análisis externos (Morningstar, Citywire, blogs pro) |
| `agents/manager_profiler.py` | Identifica equipo gestor + Opus lead/co (Fase J) |
| `agents/manager_deep_agent.py` | Enriquece perfiles con full-text articles |
| `agents/analyst_agent.py` | Síntesis 8 secciones (Sonnet/Gemini) |
| `agents/dashboard_quality_agent.py` | 106 reglas declarativas |
| `tools/output_accessor.py` | Único punto de lectura del schema |
| `tools/output_merger.py` | Preserva `_manual_edits` entre runs |
| `tools/process_lock.py` | Locks por fondo (multi-proceso) |
| `tools/publication_calendar.py` | Detecta cadencia de publicaciones |
| `data/quality_rules.json` | 106 reglas de quality (Fases F+G+I+K) |
| `data/trusted_sources.json` | Pro sources con tag region (ES/INT) |
| `dashboard/generate_dashboard.py` | Generador HTML |
| `chat_server.py` | FastAPI + Gemini para Q&A sobre los docs del fondo |

---

## Tests

```bash
# No-regresión ES + INT (4 fondos baseline cada uno)
python tests/test_es_no_regression.py
python tests/test_int_no_regression.py

# Tests Fase K (fixes auditoría)
python tests/test_fase_k_fixes.py

# Tests integración tools
python tests/test_integration_tools.py
python tests/test_output_accessor.py
```

---

## Variables de entorno

Crear `.env` en la raíz:

```
ANTHROPIC_API_KEY=sk-ant-...   # Claude Opus/Sonnet
GOOGLE_API_KEY=AIza...         # Gemini
SERPER_API_KEY=...             # Serper.dev (Google search)
```

---

## Filosofía del sistema

- **Determinismo**: llamadas LLM de extracción usan `temperature=0` (Fase K). Mismo input → mismo output.
- **Anti-invención**: validador post-LLM (`_validate_perfiles_against_sources`) descarta entidades no presentes en fuentes.
- **Anti-regresión**: quality loop hace backup pre-iter y restaura si empeora (Fase K).
- **Schema único**: `tools/output_accessor.py` es punto canónico de lectura. Convención obligatoria.
- **Sin overfitting**: heurísticas reemplazadas por Opus con `confidence` cuando disponible. Fallback a peers (no lead/co arbitrario).

---

## Optimización de coste (Fase Cost-Opt, 2026-05-02)

Sistema optimizado para coste sostenible — **€8-15/mes proyectado** vs €100 antes.

### Variables de entorno opcionales

```bash
# Cache LLM local (default: activo, TTL 24h)
LLM_CACHE_DISABLED=1            # Forzar refresh (no usar cache)

# Skip secciones ya completas en analyst
SKIP_EXISTING_SECTIONS=1        # Solo regenerar secciones borradas/forzadas

# Quality loop iteraciones (default: 1)
QUALITY_LOOP_MAX_ITER=2         # Permitir iter 2 para fondos problemáticos

# Fallback Anthropic cuando Gemini falla (default: activo)
GEMINI_FALLBACK_ANTHROPIC=0     # Desactivar fallback (no recomendado)

# Sonnet como T1 default (calidad alta en críticas, default: ON)
USE_SONNET=false                # Usar solo Gemini Flash (más barato, calidad menor)
```

### Telemetría de coste

```bash
python -m tools.cost_monitor              # resumen sesión + 7d + 30d
python -m tools.cost_monitor --today      # solo hoy
python -m tools.cost_monitor --month      # mes actual
python -m tools.cost_monitor --by-agent 30
python -m tools.cost_monitor --by-model 30
python -m tools.cost_monitor --top-funds 10
```

### Coste real por operación

| Operación | Coste |
|---|---|
| Pipeline ES nuevo (cache vacío) | €0.18-0.37 |
| Pipeline INT nuevo | €0.30-0.50 |
| Re-chequear fondo existente (sin nuevos docs) | €0-0.02 (cache+skip) |
| Regenerar 1 sección modificada | €0.01-0.02 |

---

## Licencia

MIT (ver `LICENSE`).
