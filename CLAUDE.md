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

- **2026-04-27**: Fase C ejecutada — refactor de lectura, `output_accessor.py` ampliado a ~50 getters, convención documentada aquí. Output.json byte-idéntico al baseline. Detalles en `git tag pre-consolidacion-schema-2026-04-27`.
- **2026-04-27 (tarde)**: Fase E ejecutada — pipeline INT consolidado. Routing automático LU/IE/FR/DE vía `regulator_router`. `intl_agent.py` deprecado eliminado. Bug DNCA AUM €41B arreglado en `intl_extractor_v2._merge_share_classes` (filtro periodos válidos). 7 fondos huérfanos archivados en `data/funds.archived_orphans_20260427/`. Tests INT ampliados con validación AUM. Detalles en `git tag pre-fase-e-2026-04-27`.
