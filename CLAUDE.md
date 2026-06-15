# CLAUDE.md — Convenciones del proyecto fund-analyzer

Documento canónico del proyecto. Léelo antes de tocar código.

---

## 1. Arquitectura de `output.json`

Cada fondo tiene un `data/funds/{ISIN}/output.json` con **dos zonas semánticas distintas** (no son duplicados — son niveles de procesamiento diferentes):

### Zona A — TOP-LEVEL = datos RAW
```json
{
  "nombre": "...",
  "isin": "...",
  "gestora": "...",
  "tipo": "ES" | "INT",
  "ultima_actualizacion": "...",
  "kpis": { ... },
  "cuantitativo": { "serie_aum": [...], ... },
  "posiciones": { "actuales": [...], "historicas": [...] },
  "gestores": { "equipo": [...], "perfiles": [...] },   ← RAW del manager_profiler
  "cualitativo": { ... },                                ← (solo ES)
  "hechos_relevantes": [...],
  "fuentes": { ... },
  "_manual_edits": ["nombre", "kpis.aum_actual_meur", ...],
  "_int_*": { ... }                                      ← (solo INT, cache interno)
}
```

**Quién escribe aquí**: agentes fuente (`cnmv_agent`, `manager_profiler`, `manager_deep_agent`, `intl_extractor_v2`).

**Características**: datos crudos extraídos de las fuentes (PDFs, XMLs, web scraping). Pueden estar incompletos, vacíos, o tener inconsistencias.

### Zona B — `analyst_synthesis.*` = datos PROCESADOS por LLM
```json
{
  "analyst_synthesis": {
    "resumen": { "texto": "...", "filosofia_inversion": "...", "criterios_inversion": [...] },
    "historia": { "texto": "...", "hitos": [...] },
    "gestores": { "perfiles": [...], "texto": "..." },   ← RICO con CV, trayectoria
    "evolucion": { ... },
    "estrategia": { ... },
    "cartera": { ... },
    "fuentes_externas": { ... },
    "documentos": { ... }
  }
}
```

**Quién escribe aquí**: `analyst_agent.py` (Capa 3 con Sonnet/Gemini multi-tier).

**Características**: síntesis cruzando múltiples fuentes (CNMV PDFs + cartas + web + extractor INT). Texto narrativo + estructuras enriquecidas.

### Zona C — Cache interno (`_int_*`, solo INT)
```json
{
  "_int_clases": [...],
  "_int_gestores": [...],
  "_int_cualitativo": { ... }
}
```

Cache temporal del extractor INT v3 entre passes del analyst. **No es API pública** — no leer directamente desde código fuera del analyst.

### Zona D — Metadata interna
```json
{
  "_manual_edits": ["nombre", "analyst_synthesis.gestores.perfiles", ...],
  "_merge_log": [...],
  "_anti_invencion_guard": "no_data_in_sources"
}
```

`_manual_edits` lista paths protegidos por `tools/output_merger`: cuando analyst regenera, esos paths NO se sobrescriben. Permite patches manuales (como `patch_all_nombres.py`) que sobreviven futuros runs.

---

## 2. Cómo leer del schema (REGLA OBLIGATORIA)

**Toda lectura del schema pasa por `tools/output_accessor.py`. NUNCA acceso directo `data["X"]` o `data.get("X", {}).get("Y")` en código fuera del propio accessor.**

### ❌ NO hacer
```python
nombre = data.get("nombre", "")                                    # PROHIBIDO
perfiles = data.get("analyst_synthesis", {}).get("gestores", {}).get("perfiles", [])   # PROHIBIDO
aum = data.get("kpis", {}).get("aum_actual_meur")                  # PROHIBIDO
```

### ✅ Sí hacer
```python
from tools.output_accessor import get_nombre, get_perfiles, get_kpi_aum
nombre = get_nombre(data)
perfiles = get_perfiles(data)
aum = get_kpi_aum(data)
```

**Por qué**: el accessor es el único punto de verdad sobre dónde leer cada campo. Si el schema cambia (raro, pero posible), solo hay que actualizar el accessor.

**Excepción**: si necesitas el dict raw completo (ej. `data.get("cuantitativo", {})` para iterar todas las series), está bien. La regla aplica a campos individuales nominados en el schema.

### Tabla canónica de getters

| Path en JSON | Getter | Quién escribe |
|---|---|---|
| `nombre` | `get_nombre(data)` | `cnmv_agent`, `intl_extractor_v2`, patch scripts |
| `gestora` | `get_gestora(data)` | `cnmv_agent`, patch scripts |
| `isin` | `get_isin(data)` | todos los agentes |
| `tipo` | `get_tipo(data)` | `cnmv_agent` (ES), `intl_extractor` (INT) |
| `kpis.aum_actual_meur` | `get_kpi_aum(data)` | `cnmv_agent` |
| `kpis.num_participes` | `get_kpi_participes(data)` | `cnmv_agent` |
| `kpis.ter_pct` | `get_kpi_ter(data)` | `cnmv_agent` |
| `kpis.ter_efectivo_pct` | `get_kpi_ter_efectivo(data)` | `cnmv_agent` |
| `kpis.coste_gestion_pct` | `get_kpi_coste_gestion(data)` | `cnmv_agent` |
| `kpis.volatilidad_pct` | `get_kpi_volatilidad(data)` | `cnmv_agent` |
| `kpis.clasificacion` | `get_kpi_clasificacion(data)` | `cnmv_agent` |
| `kpis.perfil_riesgo` | `get_kpi_perfil_riesgo(data)` | `cnmv_agent` |
| `kpis.depositario` | `get_kpi_depositario(data)` | `cnmv_agent` |
| `kpis.divisa` | `get_kpi_divisa(data)` | `cnmv_agent` |
| `kpis.fecha_registro` | `get_kpi_fecha_registro(data)` | `cnmv_agent` |
| `kpis.max_drawdown_pct` | `get_kpi_max_drawdown(data)` | `cnmv_agent` |
| `kpis.rotacion_cartera_pct` | `get_kpi_rotacion(data)` | `cnmv_agent` |
| `kpis.rating_morningstar` | `get_kpi_rating_morningstar(data)` | discovery + analyst |
| `kpis.srri` o `kpis.perfil_riesgo` | `get_kpi_srri(data)` | `cnmv_agent` |
| `posiciones.actuales` | `get_posiciones_actuales(data)` | `cnmv_agent` |
| `posiciones.historicas` | `get_posiciones_historicas(data)` | `cnmv_agent` |
| `cuantitativo.serie_aum` | `get_serie_aum(data)` | `cnmv_agent` |
| `cuantitativo.serie_ter` | `get_serie_ter(data)` | `cnmv_agent` |
| `cuantitativo.serie_rentabilidad` | `get_serie_rentabilidad(data)` | `cnmv_agent` + `cnmv_enrichment` |
| `cuantitativo.serie_vl_base100` | `get_serie_vl_base100(data)` | `cnmv_agent` |
| `cuantitativo.serie_participes` | `get_serie_participes(data)` | `cnmv_agent` |
| `cuantitativo.serie_ter_por_clase` | `get_serie_ter_por_clase(data)` | `cnmv_agent` |
| `cuantitativo.serie_comisiones_por_clase` | `get_serie_comisiones_por_clase(data)` | `cnmv_agent` |
| `cuantitativo.serie_rotacion` | `get_serie_rotacion(data)` | `cnmv_agent` |
| `cuantitativo.mix_activos_historico` | `get_mix_activos(data)` | `cnmv_agent` + `cnmv_enrichment` |
| `cuantitativo.serie_clases_info` | `get_clases_info(data)` | `cnmv_agent` |
| `analyst_synthesis.gestores.perfiles` | `get_perfiles(data)` | `analyst_agent` |
| `analyst_synthesis.gestores.texto` | `get_gestores_texto(data)` | `analyst_agent` |
| `analyst_synthesis.resumen` | `get_section_resumen(data)` | `analyst_agent` |
| `analyst_synthesis.resumen.texto` | `get_resumen_texto(data)` | `analyst_agent` |
| `analyst_synthesis.historia` | `get_section_historia(data)` | `analyst_agent` |
| `analyst_synthesis.historia.texto` | `get_historia_texto(data)` | `analyst_agent` |
| `analyst_synthesis.estrategia` | `get_section_estrategia(data)` | `analyst_agent` |
| `analyst_synthesis.cartera` | `get_section_cartera(data)` | `analyst_agent` |
| `analyst_synthesis.evolucion` | `get_section_evolucion(data)` | `analyst_agent` |
| `analyst_synthesis.fuentes_externas` | `get_section_fuentes_externas(data)` | `analyst_agent` |
| `analyst_synthesis.documentos` | `get_documentos(data)` | `analyst_agent` |
| `cualitativo` | `get_cualitativo(data)` | `cnmv_agent` (solo ES) |
| `hechos_relevantes` | `get_hechos_relevantes(data)` | `cnmv_agent`, `analyst_agent` |
| `lecturas_externas` | `get_lecturas_externas(data)` | `readings_collector` |
| `analisis_consistencia` | `get_analisis_consistencia(data)` | `analyst_agent` |
| `comision_exito` | `get_comision_exito(data)` | `cnmv_agent` |
| `kpis.anio_creacion` | `get_anio_creacion(data)` | `cnmv_agent` |
| `_int_clases` (INT) | `get_int_clases(data)` | `intl_extractor_v2` |
| `_int_gestores` (INT) | `get_int_gestores(data)` | `intl_extractor_v2` |
| `_int_cualitativo` (INT) | `get_int_cualitativo(data)` | `intl_extractor_v2` |
| `economia_fondo` (INT) | `get_economia_fondo(data)` | `intl_extractor_v2` |
| `clases` (INT) | `get_clases(data)` | `intl_extractor_v2` |

### Modo strict (para tests)

```python
from tools.output_accessor import get_perfiles_strict, CanonicalPathEmpty

try:
    perfiles = get_perfiles_strict(data)
except CanonicalPathEmpty:
    pytest.fail("Fondo sin perfiles canónicos")
```

### Inspección con CLI

```bash
python -m tools.output_accessor ES0112231008             # info básica
python -m tools.output_accessor --audit ES0112231008     # JSON completo (audit_output)
python -m tools.output_accessor --audit-all              # tabla resumen + drifts en todos los fondos
```

---

## 3. Cómo añadir un campo nuevo

1. **Decidir la zona**:
   - ¿Lo escribe un agente fuente (cnmv, manager, intl)? → top-level
   - ¿Lo procesa el analyst combinando fuentes? → `analyst_synthesis.*`
2. **Escribir desde el agente** en su path elegido.
3. **Añadir un getter** en `tools/output_accessor.py`.
4. **Documentar** en la tabla de arriba.
5. **Si es crítico** (afecta dashboard o decisiones), añadir al test `tests/test_output_accessor.py`.

---

## 4. Pipeline INT (5 etapas + routing por país)

El pipeline INT (no-ES) ejecuta 4 etapas tras detectar el prefijo del ISIN:

```
ISIN → regulator_router → identity card → intl_discovery_agent → docs locales
                                                               ↓
                          intl_extractor_v2 → intl_data.json → analyst_agent → output.json
```

### Routing por prefijo ISIN

| Prefijo | Regulador | Pipeline |
|---|---|---|
| **ES** | CNMV | path separado: `cnmv_agent` → `analyst_agent` |
| **LU** | CSSF | `regulator_router(CSSFAgent)` → discovery → extractor → analyst |
| **IE** | CBI (Central Bank of Ireland) | `regulator_router(CBIAgent)` → discovery → extractor → analyst |
| **FR** | AMF | `regulator_router(AMFAgent)` → discovery → extractor → analyst |
| **DE** | Bundesanzeiger | `regulator_router(BundesanzeigerAgent)` → discovery → extractor → analyst |
| **GB / BE / NL / CH / AT / IT** | None | `regulator_router(None)` → discovery hace identity + docs → extractor → analyst |

El `agents/orchestrator.py` invoca `regulator_router.run_regulator(isin, config)` para cualquier prefijo no-ES. El router devuelve `RegulatorOutput.to_dict()` con identity card y, si aplica, lista de documentos detectados en el regulador. Ese dict se persiste en `data/funds/{ISIN}/{regulador}_data.json` (`cssf_data.json`, `cbi_data.json`, `amf_data.json`, `bundesanzeiger_data.json`).

### Tabla de agentes INT

| Agente | LOC | Rol | Output | Quién lo invoca |
|---|---|---|---|---|
| `agents/regulator_router.py` | 202 | Dispatch + gap analysis | dict `RegulatorOutput` | `orchestrator.py` |
| `agents/cssf_agent.py` | 171 | Identity card LU (CSV CSSF) | `cssf_data.json` | `regulator_router` |
| `agents/cbi_agent.py` | — | Identity card IE | `cbi_data.json` | `regulator_router` |
| `agents/amf_agent.py` | — | Identity card FR (portal AMF) | `amf_data.json` | `regulator_router` |
| `agents/bundesanzeiger_agent.py` | — | Identity + docs DE | `bundesanzeiger_data.json` | `regulator_router` |
| `agents/discovery_v2.py` | 989 | Localización docs (live + Wayback + Google + Opus hints) | `SharedState` | `intl_discovery_agent` |
| `agents/intl_discovery_agent.py` | 188 | Wrapper que serializa discovery + KB per-fund | `intl_discovery_data.json` | `orchestrator.py` |
| `agents/intl_extractor_v2.py` | 917 | Extracción concept-first 2-stage Gemini (Pro mapper + Flash extractor) | `intl_data.json` | `orchestrator.py` |
| `agents/analyst_agent.py` | 3.7K | Síntesis final (compartido ES+INT) | `output.json` | `orchestrator.py` |

### Convenciones específicas INT

**Sub-fondos vs umbrella SICAV (caso DNCA INVEST)**:
- DNCA INVEST es un SICAV-paraguas con ~25 sub-fondos. ALPHA BONDS, FLEX INFLATION, etc.
- El AUM correcto para `kpis.aum_actual_meur` es el del **sub-fondo target**, NO la suma del SICAV completo.
- Bug histórico (Fase E, 2026-04-27): `_merge_share_classes` en `intl_extractor_v2.py` línea 285 usaba `max(serie, key=lambda e: str(e.get("periodo","")))`. Si una entry tenía `periodo="None"` (string), `"None" > "2025"` lexicográficamente y ganaba la suma agregada del SICAV (€41B en lugar de €14.6B real del sub-fondo).
- **Fix**: filtrar entries con `periodo` que match `^\d{4}$` antes del `max`. Y filtrar entries inválidas antes de añadirlas a `serie_aum`.

**Identity card != Annual Report**:
- Los reguladores europeos NO suelen publicar Annual Reports descargables (excepto Bundesanzeiger).
- CSSF (LU) solo publica identity card en CSV maestro. AMF (FR) tiene portal pero docs no son automáticos. CBI (IE) similar.
- Los AR vienen del **discovery** (web gestora + Wayback + Google).

**Cache interno `_int_*`**:
- `_int_clases`, `_int_gestores`, `_int_cualitativo` son scratchpad del extractor entre passes.
- NO son schema público. Excluidos de getters principales del accessor (existen `get_int_*` separados).

### Status implementación (2026-04-27)

| Etapa roadmap | Status |
|---|---|
| 1. Reguladores | ✅ Completo: LU (CSSF), IE (CBI), FR (AMF), DE (Bundesanzeiger). GB sin regulador. |
| 2. Discovery v2 | ✅ Producción (3 fondos validados: Trojan, Storm, DNCA) |
| 3. Extractor v3 | ✅ Producción (hold-out aprobado: GAM IE, R-Co FR, Trojan IE, DNCA LU) |
| 4. Letters quarterly INT | ❌ Pendiente (no hay agente dedicado para INT) |
| 5. Analyst INT | ✅ Compartido con ES (analyst_agent.py) |

### Tests baseline INT

`tests/test_int_no_regression.py` cubre actualmente 2 fondos: Trojan (IE00B6T42S66) + DNCA (LU1694789378). Validan métricas estructurales (perfiles, posiciones, chars de texto). Tras Fase E también validan rangos AUM razonables (descarta valores >€50B típicos de bug umbrella SICAV).

---

## 5. Diferencias ES vs INT

| Campo | ES | INT |
|---|---|---|
| `nombre`, `gestora`, `isin`, `tipo` | top-level | top-level |
| `kpis.*` | top-level | top-level |
| `posiciones.actuales` | top-level | top-level |
| `cualitativo.*` | top-level | NO (solo `_int_cualitativo` cache) |
| `gestores.equipo` (raw) | top-level | top-level |
| `analyst_synthesis.gestores.perfiles` | sí | sí |
| `economia_fondo` | NO | top-level (INT-específico) |
| `clases`, `_int_clases` | NO | top-level (INT-específico) |
| `cuantitativo.serie_aum` | sí | sí (en M€) |

El accessor unifica: el mismo getter funciona para ES e INT, devolviendo lista vacía/None cuando el campo no aplica.

---

## 6. Convención de patches manuales

Cuando un script externo modifica `output.json` (caso `patch_all_nombres.py`, `clean_invented_gestores.py`, etc.):

1. Lee con accessor.
2. Modifica el path.
3. **Marca el path como manual** con `tools.output_merger.mark_manual_edit(data, "path.dotted")`.
4. Guarda con `tools.output_merger.save_output(isin, data)`.

El analyst respetará el `_manual_edits` en futuras regeneraciones (no sobrescribirá esos paths).

---

## 7. Detección de drift

`tools.output_accessor.detect_drift(data)` valida:
1. Metadata (nombre/gestora/isin) coherente entre top-level y analyst_synthesis.
2. KPI AUM coherente (drift > 5% → alerta).
3. Perfiles top-level vs analyst_synthesis no contradictorios (caso del 2026-04-27).
4. Cache `_int_*` poblado pero analyst sin procesar.

Integrado en `analyst_agent._save()` — drifts se registran en `meta_report.json`.

CLI: `python -m tools.output_accessor --audit-all` muestra todos los drifts.

---

## 8. Pendientes / TODO

- Refactor `dashboard/app.py` (Streamlit) — diferido por requerir verificación visual manual.
- Pre-commit hook para detectar accesos directos al schema (planeado).
- Generación automática de la tabla canónica de esta página desde `output_accessor.py` (`tools/gen_schema_doc.py`).

---

## 9. Histórico de cambios

| Fase | Fecha | Cambio | Breaking? |
|---|---|---|---|
| C | 2026-04-27 | Refactor lectura, `output_accessor.py` ~50 getters | No |
| E | 2026-04-27 | Pipeline INT consolidado (CSSF/CBI/AMF/Bundes), `intl_agent.py` eliminado, bug DNCA AUM €41B arreglado | Sí (eliminó intl_agent) |
| F | 2026-04-27 | 7 fixes Magallanes: clases comerciales (cnmv_agent v7), trusted_sources.json, file:// PDFs, AUM extractor wins, comisiones line chart, chat doc-only, publication_calendar | No (aditivo) |
| G | 2026-04-28 | 33 reglas declarativas + region tag trusted_sources + cross-fund check inicial | No |
| H | 2026-04-28 | Bug párrafos cartera, badge recency 2 líneas, gestores iterativo `_deep_search_per_gestor` (Tier 1-4) | No |
| I | 2026-04-28 | Schema unificado equipo/equipo_gestor, `_filter_gestores` copia `articulos_completos`, dedup acentual + cross-fund manager_profiler, anti-invención v2 | Sí (equipo schema dual) |
| J | 2026-04-28 | Opus identifica lead/co/confidence en `_compile_profiles`. Eliminado `_rank_lead_first` heurístico | Sí (manager_profile.json schema) |
| J+ | 2026-04-28 | Guard CNMV abort (ISIN inválido), `_gestora_domain` genérico, `_explore_gestora_team_pages` |  No |
| **K** | **2026-04-29** | **Auditoría sistémica (14 fallos identificados, 11 fixes aplicados)**: K1 merge manager_deep ↔ profile, K2 guard analyst relajado, K3 anti-regresión quality loop, K4 sin fallback heurístico cuando Opus desconoce, K5 temperature=0 en 5 calls Opus extracción, K6 readings validation con anchor gestora si fund_short genérico, K9 pre-filter cross-fund eliminado (Opus es el filter), K10 metadata zombie condicional, K11 readings_agent.py movido a deprecated, K12 tests E2E con fixtures | Sí (manager_profile.json schema, behavior cambios) |
| K22 | 2026-04-29 | INT readings adaptación multi-idioma: 8 regiones (FR/DE/GB/IT/CH/BE/NL/EN_GLOBAL) con press regional + niche blogs + keywords idioma local (`entretien`/`Interview`/`intervista`...). Detección por prefijo ISIN o gestora (LU/IE multidomicilio) | No (aditivo) |
| **N** | **2026-06-08** | **Modelos cowork explícitos + calidad de extracción (visión) + gestores track-record**: (N1) el bat `analizar_fondo.bat` fija `claude -p --model claude-opus-4-8` en las 4 skills (`MODEL_EXTRACT/LETTERS/MANAGER/ANALYST`). Antes corrían SIN `--model` con el default (que ya era Opus 4.8, evidencia en `extracted/*.json`); la auditoría DESCARTÓ bajar extracción a Sonnet (sería ahorro, no calidad — ya iba en Opus y extrae bien con `anti_invencion_notes`). Fija el modelo para estabilidad + metadata honesta + poder cambiar analyst→Fable. `analyst._meta.main_model`=`claude-opus-4-8`. (N2) **Lectura visual de PDFs** en `extract-pdfs`: el `Read` rasteriza con `pdftoppm` (poppler) que NO está en Windows → caía a `pdfplumber` texto, que pierde dígitos/prosa CID (causa nº1 de cifras malas). Fallback: render con **PyMuPDF `fitz`** (ya instalado) a PNG 200 DPI → `Read` la imagen. Texto plano solo para localizar, nunca para copiar cifras. (N3) **Gestores → track-record/CV**: `analyst-cowork` reorienta `trayectoria`/`cv_bullets`/reglas hacia recorrido profesional (incorporación a la gestora, empresas/cargos previos con fechas, formación, experiencia) por encima de estilo/estrategia, con anti-invención reforzada; `manager-deep` prioriza conservar esos datos de carrera en `texto_completo`. | No (config + prompts) |
| **M** | **2026-06-06** | **Lazo de feedback humano que de verdad acciona (4 fixes)**: M1 (Fix A) regeneración SELECTIVA — cuando el feedback apunta a secciones concretas (todos los items con `target_section`), `orchestrator._consume_cowork_analyst` solo reemplaza esas secciones y preserva el resto del `analyst_synthesis` previo verbatim (gate `FUND_APPLY_FEEDBACK=1`; helper `_feedback_targeted_sections`). Evita empeorar pestañas no tocadas (caso gestores LU0168736675). `_feedback_revisar` se resetea por-run en `apply_pending_feedback` y se limpia tras consumir. M2 (Fix B) verificación honesta — el analyst-cowork emite `_meta.feedback_outcomes[]` (`{feedback_id, item_idx, resolved, reason}`) por item; `verify_resolved_after_run` lo respeta sobre la heurística ciega `len(texto)>100`. Un `resolved:false` honesto («la fuente solo tiene 8 posiciones, no 10») se muestra en ámbar en el dashboard. M3 (Fix C) `bundle_exporter.build_bundle_feedback` incluye feedback `pending` (no solo `applied`): en el re-run ♺ el feedback aún no está applied cuando se exporta el bundle (paso 4.5 antes del apply en paso 6) → sin esto la skill leía un bundle vacío (`n_relevant_items:0`) y nunca accionaba nada. M4 (Fix D) `analizar_fondo.bat` NO salta la skill analyst-cowork cuando hay `--apply-feedback` (`if defined APPLY_FEEDBACK set SKIP_ANALYST=`): el `--resume` del botón ♺ re-consumía una síntesis vieja sin re-analizar. M5 (hardening) `verify_resolved_after_run` para `revisar` SIN veredicto del analyst → marca **no verificable** (ámbar), nunca verde por heurística `len(texto)` (eliminado el falso verde de raíz). Verificado que `meta_agent`/`validation_agent`/`name_recovery` no destruyen `_meta.feedback_outcomes`. Tests `tests/test_feedback_applier.py` (29) + `tests/test_feedback_loop_wiring.py` (7); validación integración determinista 13/13 (apply→consume→verify sobre código real, sin LLM). | No (aditivo) |
| **L** | **2026-04-29** | **3 schema mismatches descubiertos post-validación AZ Valor**: L1 `_filter_lecturas` lee `analisis_completos`/`otros_readings` (no `analisis`/`lecturas`) + sintetiza texto desde READING_SCHEMA estructurado (resumen+opinión+puntos_clave+citas). L2 `_filter_letters` acepta cartas con texto_completo vacío si tienen campos K15 (tesis_gestora/decisiones_tomadas/contexto_mercado/citas_textuales). L4 `_classify_hecho_evento` clasifica heurísticamente eventos con 16 reglas regex específicas (Modificación folleto, Cambio auditor, Delegación gestión, etc.) cuando CNMV no aporta epígrafe. Tests `tests/test_fase_l_fixes.py` 19/19. (L3 fue falsa alarma: el campo es `tipo`, no `tipo_activo`) | No (aditivo) |

---

## 10. Quality Loop semantics (Fase K)

`agents/orchestrator.py:_run_quality_loop` orquesta retries del analyst+upstream agents hasta `max_iter=2` veces.

**Flujo per iter**:
1. Backup `output.json` → `output.iter_{n-1}.json` (K3 anti-regresión)
2. Detectar `fallos_estructura > 0` por `dashboard_quality_agent`
3. Reagentar upstream según `agente_responsable` de cada fallo:
   - Cascada gestores: `manager_profiler` → `manager_deep_agent` → `google_snippets` → `sibling_finder` (cada paso solo si anterior no encontró nombres reales)
   - Otros agents: `cnmv_enrichment`, `letters_collector`, etc.
4. Re-ejecutar `analyst_agent` con `quality_feedback`
5. Re-evaluar fallos. Si `n_fallos >= prev_fallos` (no mejoró):
   - **K3**: restaurar backup pre-iter (rollback) + abortar loop
6. Cleanup: borrar `output.iter_*.json` al finalizar

**Garantías post-K**:
- output.json final NUNCA es peor que la mejor iteración anterior (rollback automático)
- Manager_profile.json escrito por profiler PRESERVA campos lead/co cuando manager_deep_agent enriquece (merge K1)
- Llamadas Opus de extracción/clasificación son DETERMINISTAS (`temperature=0`)

---

## 11. Schema canónico de `manager_profile.json` (post-K)

```json
{
  "isin": "ES0159259011",
  "fund_name": "Magallanes European Equity",
  "gestora": "Magallanes Value Investors",
  "generated": "2026-04-29T...",

  "equipo_gestor": ["Iván Martín", "Blanca Hernández"],   ← canónico plano (max 2)
  "equipo": [{"nombre": "...", "cargo": "...", "biografia": "...", ...}],   ← detalle dicts
  "equipo_roles": {
    "Iván Martín": {"is_lead": true, "_source": "opus_high"},
    "Blanca Hernández": {"is_co": true, "_source": "opus_high"}
  },

  "articulos_completos": {
    "Iván Martín": [{"fuente_url", "titulo", "texto_completo", "fecha"}, ...]
  },

  "fuentes_web": [...],
  "fuentes_consultadas": [...],
  "informacion_cartas": [...],
  "informacion_cnmv": {...},

  "_opus_lead_confidence": "high|medium|low",   ← solo si presente
  "_known_public_undersourced": [...]            ← solo si NO vacío (K10)
}
```

**Quien escribe qué**:
- `manager_profiler.run()` → escribe `equipo_gestor`, `equipo`, `equipo_roles`, `_opus_lead_confidence`, `fuentes_web`
- `manager_deep_agent.run()` → MERGE (K1): añade `articulos_completos`, `_known_public_undersourced`. NO sobrescribe campos del profiler.
- Backup pre-deep en `manager_profile.backup_pre_deep.json` (recuperable on exception).

---

## 12. Optimización de coste LLM (Cost-Opt Fase 1, 2026-05-02)

Tras factura inesperada de €100 en Google Gemini API por iteración intensiva
durante desarrollo, se aplicaron 5 optimizaciones aditivas + 1 fallback.

### Componentes nuevos

| Componente | Ubicación | Función |
|---|---|---|
| `tools/llm_cache.py` | nuevo | Cache local hash(model+prompt+context), TTL 24h, JSON en `data/.llm_cache/` |
| `tools/cost_monitor.py` | nuevo | Telemetría persistente jsonl + CLI (`--today`, `--month`, `--by-agent`) |

### Optimizaciones activas

1. **O1 — Cache local LLM** (`get_cached`/`set_cached`): integrado en
   `_gemini_text` y `_sonnet_text`. Reruns con mismo prompt + mismo modelo →
   cache hit instantáneo (FREE). Disable con `LLM_CACHE_DISABLED=1`.

2. **O2 — Skip secciones existentes** (`SKIP_EXISTING_SECTIONS=1` env):
   Analyst saltará secciones donde `analyst_synthesis.X` ya tenga texto >100c
   o perfiles/opiniones. Útil para reruns parciales (regenerar solo secciones
   borradas/forzadas).

3. **O3 — Anthropic prompt caching** (`cache_control: ephemeral`): system
   prompts >2000 chars se envían como bloque cacheable. 90% descuento en cache
   hits. Aprovecha que las 8 secciones del analyst comparten `_system_role`.

4. **O9 — Quality loop `max_iter=1` default**: bajado de 5 a 1 (override con
   `QUALITY_LOOP_MAX_ITER=N`). Análisis Mayo 2026 mostró que iter 2+ raramente
   aporta valor (~70% generan output rejected by guard).

5. **O11 — Cost monitor persistente**: `data/cost_log.jsonl` append-only.
   Cada llamada LLM añade entry con model/agent/isin/tokens/cost. CLI:
   ```bash
   python -m tools.cost_monitor              # resumen sesión + 7d
   python -m tools.cost_monitor --today
   python -m tools.cost_monitor --month
   python -m tools.cost_monitor --by-agent
   python -m tools.cost_monitor --top-funds 10
   ```

### Op A — Fallback Anthropic cuando Gemini falla

Si `GEMINI_FALLBACK_ANTHROPIC=1` (default), `_gemini_text` y `_gemini_call`
caen automáticamente en `_haiku_text`/`_haiku_call` cuando:
- Gemini devuelve `PERMISSION_DENIED` / 403 / 401 (billing bloqueado, key
  rotada, proyecto suspendido)
- Init failure (no API key, library error)
- Tras agotar retries (rate limit prolongado)

Modelos Anthropic usados:
- `HAIKU_MODEL = "claude-haiku-4-5-20251001"` — fallback primario, ~3x más
  barato que Sonnet, similar coste a Gemini Flash
- `SONNET_MODEL = "claude-sonnet-4-5-20241022"` — Tier 1 análisis si
  `USE_SONNET=true`

Permite operar sin Gemini durante incidencias de billing o cambios de proveedor.

### Coste proyectado por fondo (post Cost-Opt)

| Concepto | Coste estimado |
|---|---|
| Pipeline ES nuevo (cache vacío) | $0.30-0.40 = €0.27-0.37 |
| Pipeline INT nuevo | $0.40-0.60 = €0.37-0.55 |
| Re-ejecutar mismo fondo (cache hit) | $0.05-0.10 |
| Regenerar 1 sección (con SKIP_EXISTING) | $0.01-0.02 |

**Coste mensual proyectado**: 50 fondos nuevos/mes = **€20-30/mes**
(vs €100 pre-optimizaciones).
