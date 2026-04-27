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

## 4. Diferencias ES vs INT

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

## 5. Convención de patches manuales

Cuando un script externo modifica `output.json` (caso `patch_all_nombres.py`, `clean_invented_gestores.py`, etc.):

1. Lee con accessor.
2. Modifica el path.
3. **Marca el path como manual** con `tools.output_merger.mark_manual_edit(data, "path.dotted")`.
4. Guarda con `tools.output_merger.save_output(isin, data)`.

El analyst respetará el `_manual_edits` en futuras regeneraciones (no sobrescribirá esos paths).

---

## 6. Detección de drift

`tools.output_accessor.detect_drift(data)` valida:
1. Metadata (nombre/gestora/isin) coherente entre top-level y analyst_synthesis.
2. KPI AUM coherente (drift > 5% → alerta).
3. Perfiles top-level vs analyst_synthesis no contradictorios (caso del 2026-04-27).
4. Cache `_int_*` poblado pero analyst sin procesar.

Integrado en `analyst_agent._save()` — drifts se registran en `meta_report.json`.

CLI: `python -m tools.output_accessor --audit-all` muestra todos los drifts.

---

## 7. Pendientes / TODO

- Refactor `dashboard/app.py` (Streamlit) — diferido por requerir verificación visual manual.
- Pre-commit hook para detectar accesos directos al schema (planeado).
- Generación automática de la tabla canónica de esta página desde `output_accessor.py` (`tools/gen_schema_doc.py`).

---

## 8. Histórico de cambios

- **2026-04-27**: Fase C ejecutada — refactor de lectura, `output_accessor.py` ampliado a ~50 getters, convención documentada aquí. Output.json byte-idéntico al baseline. Detalles en `git tag pre-consolidacion-schema-2026-04-27`.
