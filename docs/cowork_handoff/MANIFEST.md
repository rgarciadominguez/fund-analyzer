# Fund Analyzer — Manifest del paquete cowork

**Generado**: 2026-05-04
**Versión**: post-Fase M (downgrade Opus + skip-fresh re-runs)

---

## Documentación incluida (en este directorio `docs/cowork_handoff/`)

| Archivo | Tamaño aprox | Contenido |
|---|---|---|
| `README_HANDOFF.md` | 5 KB | Entrada principal: qué hace, por qué, quickstart, drop-in replacements gratuitos |
| `ARCHITECTURE.md` | 9 KB | Diagrama agentes ES/INT, flujo orchestrator, schema output.json, convenciones |
| `MEMORY_SUMMARY.md` | 11 KB | 35+ aprendizajes destilados (Discovery, Quality, Pipeline, Cost-opt, etc.) |
| `KNOWN_GAPS.md` | 4 KB | 6 gaps de documentación operativa con prioridad y tiempo de fix |
| `MANIFEST.md` | este archivo | Listado de qué entregar |
| `sample_outputs/` | ~13 MB | **7 fondos validados**: output.json + dashboard HTML autocontenidos. Ver `sample_outputs/README.md` |
| `README.md` | 6.6 KB | Copia del README raíz (quickstart técnico) |
| `CLAUDE.md` | 24 KB | Convenciones canónicas (schema, accessors, pipeline INT, fases) |
| `.env.example` | 384 B | Template variables entorno |
| `requirements.txt` | 177 B | 10 deps Python |
| `LICENSE` | 1 KB | MIT |

**Total docs**: ~62 KB. Lectura completa: ~30 min.

---

## Código a entregar (3 ZIPs)

### 1. `code_essential.zip` — Mínimo viable para ejecutar pipeline

```
agents/
  ├── orchestrator.py            # Coordinador
  ├── analyst_agent.py           # Síntesis 8 secciones
  ├── cnmv_agent.py              # Pipeline ES (CNMV)
  ├── cnmv_enrichment.py         # Aditivo ES (sectores, mix)
  ├── intl_extractor_v2.py       # Pipeline INT
  ├── regulator_router.py        # Dispatch INT
  ├── cssf_agent.py              # LU
  ├── cbi_agent.py               # IE
  ├── amf_agent.py               # FR
  ├── bundesanzeiger_agent.py    # DE
  ├── intl_discovery_agent.py    # Wrapper INT discovery
  ├── discovery_v2.py            # Localización docs
  ├── manager_profiler.py        # Equipo gestor
  ├── manager_deep_agent.py      # Enriquecimiento
  ├── letters_collector.py       # Cartas trimestrales
  ├── letters_deep_agent.py      # Texto completo cartas
  ├── readings_collector.py      # Análisis externos
  ├── sources_agent.py           # Fuentes pro
  ├── dashboard_quality_agent.py # 106 reglas
  ├── validation_agent.py        # Anti-invención
  └── meta_agent.py              # _meta block

tools/
  ├── output_accessor.py         # Único punto lectura schema
  ├── output_merger.py           # _manual_edits preservation
  ├── process_lock.py            # Lock por ISIN
  ├── regression_guard.py        # No aceptar peor
  ├── cost_tracker.py            # Telemetría in-session
  ├── cost_monitor.py            # Persistencia jsonl + CLI
  ├── llm_cache.py               # Cache local TTL 24h
  ├── llm_logger.py              # Helper genérico tracking
  ├── llm_models.py              # Constantes modelos (Fase M)
  ├── claude_extractor.py        # Wrapper Claude
  ├── gemini_wrapper.py          # Wrapper Gemini + fallback
  ├── pdf_extractor.py           # pdfplumber heurísticas
  ├── xml_parser.py              # XMLs CNMV
  ├── http_client.py             # httpx async
  └── publication_calendar.py    # Cadencia detection

dashboard/
  ├── generate_dashboard.py      # Generador HTML
  └── package.json               # Si tiene deps Node (ej. para charts)
```

**Comando para empaquetar**:
```bash
cd /path/to/fund-analyzer
zip -r code_essential.zip agents/ tools/ dashboard/generate_dashboard.py dashboard/package.json
```

### 2. `data_essential.zip` — Configuración + knowledge bases

```
data/
  ├── quality_rules.json          # 106 reglas declarativas (921 líneas)
  ├── trusted_sources.json        # Pro sources con tag region
  ├── regulators_knowledge.json   # CSSF/CBI/AMF/Bundesanzeiger configs
  ├── gestoras_registry.json      # Registry gestoras conocidas
  └── extraction_knowledge.json   # Patrones URL/dominios exitosos

schemas/
  └── fund_output.json            # Schema canónico
```

**NO INCLUIR**: `data/funds/{ISIN}/*` (outputs de fondos específicos analizados — no son necesarios para replicar).

**Comando para empaquetar**:
```bash
cd /path/to/fund-analyzer
zip data_essential.zip \
  data/quality_rules.json \
  data/trusted_sources.json \
  data/regulators_knowledge.json \
  data/gestoras_registry.json \
  data/extraction_knowledge.json \
  schemas/fund_output.json
```

### 3. `tests_baseline.zip` — Tests no-regresión

```
tests/
  ├── conftest.py                       # Fixtures pytest
  ├── test_es_no_regression.py          # 4 fondos ES baseline
  ├── test_int_no_regression.py         # 4 fondos INT baseline
  ├── test_fase_k_fixes.py              # Anti-regresión Fase K
  ├── test_fase_l_fixes.py              # Schema fixes Fase L
  ├── test_cost_opt.py                  # Cache + skip sections
  ├── test_integration_tools.py         # output_accessor/merger/lock
  ├── test_output_accessor.py           # Acceso schema obligatorio
  ├── baseline_es_v6.json               # Snapshot ES esperada
  └── baseline_int_v1.json              # Snapshot INT esperada
```

**Comando para empaquetar**:
```bash
cd /path/to/fund-analyzer
zip -r tests_baseline.zip tests/
```

---

## Comando único para regenerar los 3 ZIPs

```bash
#!/bin/bash
# Ejecutar desde la raíz del repo fund-analyzer
set -e

DATE=$(date +%Y%m%d)
OUT_DIR="docs/cowork_handoff/zips_$DATE"
mkdir -p "$OUT_DIR"

echo "Generando code_essential.zip..."
zip -r "$OUT_DIR/code_essential.zip" \
  agents/ tools/ \
  dashboard/generate_dashboard.py dashboard/package.json \
  -x "*.pyc" "**/__pycache__/*"

echo "Generando data_essential.zip..."
zip "$OUT_DIR/data_essential.zip" \
  data/quality_rules.json \
  data/trusted_sources.json \
  data/regulators_knowledge.json \
  data/gestoras_registry.json \
  data/extraction_knowledge.json \
  schemas/fund_output.json

echo "Generando tests_baseline.zip..."
zip -r "$OUT_DIR/tests_baseline.zip" tests/ \
  -x "*.pyc" "**/__pycache__/*"

echo "Hecho. Zips en: $OUT_DIR"
ls -lh "$OUT_DIR"
```

Guardar como `docs/cowork_handoff/regenerate_zips.sh` y dar permisos `chmod +x`.

---

## Lo que NO se incluye (intencional)

- ❌ `data/funds/{ISIN}/*`: outputs de fondos analizados. No son necesarios para replicar el sistema, solo son ejemplos de output.
- ❌ `data/.llm_cache/`: cache local LLM. Específico del entorno del autor.
- ❌ `data/cost_log.jsonl`: telemetría histórica de coste. Información sensible.
- ❌ `progress.log`, `pipeline_*.log`: logs runtime. Específicos del entorno.
- ❌ `.env`: contiene API keys reales. Solo se incluye `.env.example`.
- ❌ `feedback`, `backups/`, `dist/`, `__pycache__/`: directorios runtime/temporales.
- ❌ Memoria local del autor (`~/.claude/projects/.../memory/feedback_*.md`): contiene aprendizajes ya destilados en `MEMORY_SUMMARY.md`.

---

## Checklist de verificación post-handoff

Para validar que el paquete es funcional, el receptor debe poder:

- [ ] Leer `README_HANDOFF.md` y entender qué hace el sistema en <5 min
- [ ] Leer `ARCHITECTURE.md` y entender el flujo end-to-end en <10 min
- [ ] Instalar dependencias: `pip install -r requirements.txt` PASS
- [ ] Configurar `.env` con sus propias API keys
- [ ] Ejecutar `python -m agents.orchestrator --isin ES0112231008 --auto` y obtener `output.json` válido
- [ ] Ejecutar `pytest tests/test_es_no_regression.py` PASS (al menos 4 fondos baseline)
- [ ] Ejecutar `python -m tools.cost_monitor` y ver telemetría real
- [ ] Generar dashboard: `python dashboard/generate_dashboard.py ES0112231008`

Si los 8 checks pasan, el paquete es autosuficiente para replicación.
