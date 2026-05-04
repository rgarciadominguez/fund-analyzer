# Fund Analyzer — Arquitectura

## Vista general

```
                        ┌────────────┐
ISIN del fondo  ───────▶│orchestrator│  (1 proceso, async)
                        └─────┬──────┘
                              │
                              ▼
              ┌──────── Routing por prefijo ISIN ───────┐
              │                                          │
              ▼                                          ▼
       Pipeline ES                             Pipeline INT
   (CNMV + PDFs ES)                       (LU/IE/FR/DE/GB...)
       │                                          │
       │                                          ▼
       │                              ┌─────────────────────┐
       │                              │ regulator_router    │
       │                              │  ├ CSSF   (LU)      │
       │                              │  ├ CBI    (IE)      │
       │                              │  ├ AMF    (FR)      │
       │                              │  └ Bundes (DE)      │
       │                              └─────────┬───────────┘
       │                                        │
       ▼                                        ▼
  cnmv_agent                          discovery_v2 + intl_extractor_v2
  (XMLs CNMV bulkdata                 (web gestora + Wayback +
   + PDFs semestrales)                 Google + Opus hints +
                                      concept-first 2-stage Gemini)
       │                                        │
       └─────────────────┬──────────────────────┘
                         │
                         ▼
        ┌─── Pasos compartidos (ES + INT) ───┐
        │                                     │
        │ Paso 2:  sources_agent              │
        │ Paso 3:  letters + readings +       │
        │          manager_profiler (parallel)│
        │ Paso 3a: manager_deep_agent (merge) │
        │ Paso 3b: letters_deep_agent         │
        │ Paso 4:  analyst_agent (8 sec LLM)  │
        │ Paso 5-6: validation + meta         │
        │ Paso 7:  quality_loop (max 1 iter)  │
        │ Paso 8:  publication_calendar       │
        │                                     │
        └────────────┬────────────────────────┘
                     │
                     ▼
           data/funds/{ISIN}/output.json
                     │
                     ▼
           dashboard/generate_dashboard.py
                     │
                     ▼
           dashboard/fund-{ISIN}.html  (autocontenido, sin servidor)
```

---

## Agentes (resumen 1 línea)

### Pipeline ES (`agents/cnmv_*`)
| Agente | Rol |
|---|---|
| `cnmv_agent.py` | Descarga XMLs CNMV bulkdata (mensual + trimestral) + PDFs semestrales. Extrae cuanti + cuali ES |
| `cnmv_enrichment.py` | Módulo aditivo: enriquece sectores, rentabilidad, mix cuando cnmv_agent no aporta |

### Pipeline INT (`agents/intl_*` + reguladores)
| Agente | Rol |
|---|---|
| `regulator_router.py` | Dispatch por prefijo ISIN. Devuelve identity card (nombre, gestora, depositario) |
| `cssf_agent.py` | Identity card LU (CSV maestro CSSF) |
| `cbi_agent.py` | Identity card IE (Central Bank of Ireland) |
| `amf_agent.py` | Identity card FR (portal AMF) |
| `bundesanzeiger_agent.py` | Identity + docs DE (raro: regulador con AR descargables) |
| `intl_discovery_agent.py` | Wrapper que serializa discovery_v2 a JSON |
| `discovery_v2.py` | Localiza docs (web gestora + Wayback Machine + Google + Opus hints) |
| `intl_extractor_v2.py` | Extrae estructurado: AUM, holdings, gestores, cualitativo. 2-stage Gemini Pro mapper + Flash extractor |

### Compartidos (ES + INT)
| Agente | Rol |
|---|---|
| `sources_agent.py` | Identifica fuentes pro válidas (Morningstar, Citywire, Funds Society…) |
| `letters_collector.py` | Cartas trimestrales del gestor (web gestora + cazadores legacy) |
| `letters_deep_agent.py` | Extrae texto completo de cartas + estructura K15 (tesis, decisiones, contexto) |
| `readings_collector.py` | Análisis externos del fondo (24 sources globales + nichos por región) |
| `manager_profiler.py` | Identifica equipo gestor + Opus lead/co + bio (Fase J) |
| `manager_deep_agent.py` | Enriquece perfiles con full-text articles (Fase H+J) |
| `analyst_agent.py` | Síntesis 8 secciones (resumen, historia, gestores, evolución, estrategia, cartera, fuentes, documentos). Sonnet T1 + Gemini fallback |
| `dashboard_quality_agent.py` | Aplica 106 reglas declarativas. Detecta fallos por severidad y agente responsable |
| `validation_agent.py` | Validaciones cross-fund (anti-invención, AUM ranges, duplicados gestores) |
| `meta_agent.py` | Genera `_meta` block (pipeline_version, sources_attempted, anti_invencion_warnings) |

### Orchestration
| Agente | Rol |
|---|---|
| `orchestrator.py` | Coordinador secuencial. Paso 0-8. Quality loop max 1 iter. Anti-regresión K3 |

---

## Tools compartidas (`tools/`)

| Tool | Rol |
|---|---|
| `output_accessor.py` | **Único punto de lectura del schema.** ~50 getters. NUNCA acceso directo data["X"] |
| `output_merger.py` | Preserva `_manual_edits` entre runs. Patches sobreviven regeneraciones |
| `process_lock.py` | Lock por ISIN. Aborta si otro proceso está activo en el mismo fondo |
| `regression_guard.py` | No acepta sección con menos contenido que la anterior (Fase C) |
| `cost_tracker.py` | Telemetría in-session (suma tokens, calcula coste por modelo) |
| `cost_monitor.py` | Persistencia jsonl + CLI (`--by-agent`, `--by-model`, `--top-funds`) |
| `llm_cache.py` | Cache local hash(model+prompt+context). TTL 24h. Default ON |
| `llm_logger.py` | Helper genérico: extrae tokens de Anthropic/Gemini response y delega a cost_tracker |
| `claude_extractor.py` | Wrapper Claude para extracción JSON estructurada |
| `gemini_wrapper.py` | Wrapper Gemini con fallback automático a Sonnet si 403/PERMISSION_DENIED |
| `pdf_extractor.py` | pdfplumber + heurísticas: parse_toc, extract_pages_by_keyword, extract_text_section |
| `xml_parser.py` | Parsea XMLs CNMV (FONDMENS mensual, FONDTRIM trimestral) |
| `http_client.py` | httpx async + retry + User-Agent realista. Cookies para CNMV |
| `publication_calendar.py` | Detecta cadencia (annual/semiannual/quarterly_letters) + next_expected_date |

---

## Schema de `output.json` (resumen)

Documentación completa: ver `CLAUDE.md` sección 1.

### Zona A — TOP-LEVEL = datos RAW (escrito por agentes fuente)
```json
{
  "isin": "...",
  "nombre": "...",
  "gestora": "...",
  "tipo": "ES" | "INT",
  "kpis": {
    "aum_actual_meur", "num_participes", "ter_pct", "coste_gestion_pct",
    "anio_creacion", "clasificacion", "perfil_riesgo", "depositario", ...
  },
  "cuantitativo": {
    "serie_aum": [...], "serie_participes": [...], "serie_ter": [...],
    "serie_rentabilidad": [...], "mix_activos_historico": [...]
  },
  "posiciones": {"actuales": [...], "historicas": [...]},
  "gestores": {"equipo": [...]},
  "cualitativo": {...},          // solo ES
  "_int_*": {...}                 // solo INT, cache interno
}
```

### Zona B — `analyst_synthesis.*` = datos PROCESADOS por LLM
```json
{
  "analyst_synthesis": {
    "resumen": {"texto", "filosofia_inversion", "criterios_inversion"},
    "historia": {"texto", "hitos"},
    "gestores": {"perfiles": [{cv, trayectoria, filosofia}], "texto"},
    "evolucion": {...},
    "estrategia": {...},
    "cartera": {...},
    "fuentes_externas": {...},
    "documentos": {...}
  }
}
```

### Zona D — Metadata interna
```json
{
  "_manual_edits": ["nombre", "kpis.aum_actual_meur"],
  "_merge_log": [...],
  "_meta": {
    "pipeline_version", "sources_attempted",
    "anti_invencion_warnings_count"
  }
}
```

---

## Flujo end-to-end (ejemplo ES, fondo nuevo)

1. Usuario ejecuta `python -m agents.orchestrator --isin ES0112231008 --auto`
2. Orchestrator detecta prefijo ES → pipeline ES
3. **Paso 0**: omitido (solo INT)
4. **Paso 1b**: `cnmv_agent.run()` descarga XMLs (~30 archivos, 11 meses) + PDFs semestrales (~2 archivos). Extrae cuanti + cuali. Guarda `cnmv_data.json`. Tiempo: 3-5 min, $0.05 LLM
5. **Paso 2**: `sources_agent.run()` identifica fuentes pro válidas para el fondo. Tiempo: 30s, $0.01
6. **Paso 3** (paralelo):
   - `letters_collector.run()` busca cartas en gestora.com + Wayback + Google. Tiempo: 2-3 min, $0.02
   - `readings_collector.run()` busca análisis externos. Tiempo: 1-2 min, $0.03
   - `manager_profiler.run()` identifica equipo + Opus lead/co. Tiempo: 1-2 min, $0.10 (Opus, en Fase M baja a Haiku $0.02)
7. **Paso 3a**: `manager_deep_agent.run()` enriquece perfiles con full-text articles. Merge con manager_profile.json (no sobrescribe). Tiempo: 1 min, $0.02
8. **Paso 3b**: `letters_deep_agent.run()` extrae texto completo + estructura K15. Tiempo: 1 min, $0.03
9. **Paso 4**: `analyst_agent.run()` sintetiza 8 secciones con Sonnet (T1) + Gemini fallback. Tiempo: 3-5 min, $0.15
10. **Paso 5-6**: validation + meta (sin coste LLM)
11. **Paso 7**: quality_loop. `dashboard_quality_agent` evalúa 106 reglas. Si fallos críticos, re-ejecuta agentes responsables (max 1 iter). Tiempo: 1-2 min, $0.05
12. **Paso 8**: publication_calendar + persist `_meta` block

Total: 12-20 min, $0.30-0.50 LLM. Output: `data/funds/ES0112231008/output.json` (15-30 KB JSON).

---

## Convenciones críticas (resumen)

1. **Schema único**: usa `tools/output_accessor.py` (`get_nombre`, `get_perfiles`, `get_kpi_aum`...). NUNCA `data["X"]` directo.
2. **Anti-invención**: validador post-LLM rechaza entidades no presentes en fuentes (`_validate_perfiles_against_sources`).
3. **Determinismo**: extracciones LLM con `temperature=0` (Fase K). Mismo input → mismo output.
4. **Anti-regresión**: quality loop hace backup pre-iter. Si iter N empeora vs N-1 → restaura (Fase K3).
5. **Manual edits sobreviven**: marca path con `mark_manual_edit(data, "kpis.aum_actual_meur")`. Analyst no lo sobrescribirá.
6. **Process locks**: lock por ISIN evita doble ejecución concurrente sobre el mismo fondo.
7. **Pipeline ES LOCKED**: extender ES de forma aditiva (ej. `cnmv_enrichment.py`), NO modificar `cnmv_agent.py` salvo bug crítico.

---

## Stack técnico

| Lenguaje | Python 3.11+ |
|---|---|
| Async | asyncio + httpx (NUNCA requests síncrono) |
| HTML/XML | BeautifulSoup + lxml |
| PDF | pdfplumber (gratis, local) |
| LLM | anthropic SDK (Claude), google-genai (Gemini) |
| Logging | rich.console + rich.progress (NUNCA print desnudo) |
| Tests | pytest + pytest-asyncio |
| Dashboard | HTML autocontenido (sin servidor) generado desde `dashboard/generate_dashboard.py` |
| Chat opcional | FastAPI + Gemini (`chat_server.py`) |

10 deps: `anthropic`, `httpx`, `beautifulsoup4`, `pdfplumber`, `lxml`, `python-dotenv`, `rich`, `google-genai`, `pytest`, `pytest-asyncio`.
