# Fund Analyzer — Aprendizajes destilados

Resumen de los aprendizajes acumulados en 35+ archivos de memoria local del autor durante la construcción iterativa del sistema (Fases A-M, Abril-Mayo 2026). Aquí solo los principios — los archivos originales contienen ejemplos con datos de fondos específicos.

---

## 1. Discovery (localización de docs)

- **Cascada universal**: web gestora directa → Wayback Machine CDX API → Google search (Serper) → Opus hints (sugiere dominios). En ese orden, abandonando si encuentra suficientes docs en el paso anterior.
- **Per-fund knowledge base**: `data/funds/{ISIN}/discovery_kb.json` acumula URLs que funcionaron. Re-runs futuros consultan KB primero.
- **Cross-fund learning**: `data/extraction_knowledge.json` registra patrones URL exitosos por gestora. Ej: si `dnca-investments.com/.../annual-reports/{year}` funcionó para un fondo DNCA, prueba el mismo patrón en otros DNCA.
- **Filtro ≥1 doc/año**: si una gestora publica AR 2x/año, prioriza Diciembre. Para semestrales, prioriza Junio.
- **Pre-step prospectus + KIID**: Finect y Morningstar suelen tener prospectus + KIID descargables ANTES de buscar AR. Estos dos docs cubren mucho cualitativo.
- **Pre-2019 search**: Wayback es la única fuente fiable para AR históricos pre-2019 de gestoras pequeñas.
- **Email IR fallback**: si tras todas las cascadas faltan docs, generar plantilla email a investor relations con plan-B manual (`ingest_manual` conversacional).
- **Discovery completa huecos también en ES**: aunque CNMV es la fuente primaria ES, discovery añade cartas y readings que CNMV no tiene.

## 2. Extracción de PDFs (concept-first 2-stage)

- **Mapper Pro + extractor Flash**: para annual reports >30pp, usar Gemini Pro 2.5 para mapear TOC → secciones, y Gemini Flash 2.5 para extraer detalles. Ahorra 70% coste vs Pro full-doc.
- **Factsheets <30pp van directo al extractor**: sin mapper. El doc cabe entero en context window. Ahorra 1 llamada LLM.
- **AUM = suma clases, no umbrella SICAV**: bug clásico — extraer AUM del paraguas en lugar del sub-fondo target. Filtrar entries con `periodo` que match `^\d{4}$` antes del max() lexicográfico.
- **NAV total ≠ NAV per share**: NAV total es millones; NAV per share son decimales (15.42€). Confundirlos crea outputs absurdos.
- **Note 7 = fees, Note 11 = FX histórico**: en SICAVs UCITS estándar.
- **Rentabilidad gestora vs fondo**: la gestora reporta su track record agregado; el fondo individual puede diferir. Capturar ambos cuando aparecen.
- **Sub-fondo disambiguation**: distinguir "Trojan UK" vs "Trojan Ireland" vs hermanos del mismo paraguas. Discovery + extractor deben validar ISIN exacto en cada doc.

## 3. Pipeline ES (CNMV) — LOCKED

- **NO modificar `cnmv_agent.py`** salvo bug crítico. Es base estable de 10+ fondos producción.
- **Extender ES de forma aditiva**: cuando aparezca un gap (ej. sectores de cartera incompletos), crear módulo aparte como `cnmv_enrichment.py` que enriquece sin tocar la base.
- **Nombre del fondo del último PDF**: el cnmv_agent puede guardar nombre antiguo si el fondo cambió. Ejecutar `patch_all_nombres.py` y añadir `_manual_edits["nombre"]` para que sobreviva.
- **Unicode Windows obligatorio**: `sys.stdout.reconfigure(encoding="utf-8")` en orchestrator. Sin él, quality loop crashea silenciosamente al imprimir caracteres acentuados.
- **Pipelines siempre serial, nunca paralelo en bash**: race conditions Windows + Opus retract loop pisan output.json. Usar `process_lock.py`.

## 4. Pipeline INT — Estado validado

- **Reguladores**: LU=CSSF (CSV maestro), IE=CBI, FR=AMF, DE=Bundesanzeiger (único con AR descargables). GB sin regulador único.
- **Identity card ≠ Annual Report**: los reguladores europeos NO publican AR descargables (excepto Bundesanzeiger). AR vienen del discovery (web gestora + Wayback + Google).
- **Discovery v2 producción** (validado con 3 fondos): URL-first classification + budget por tipo doc + anti-regresión.
- **Extractor v3 producción** (hold-out aprobado en 4 jurisdicciones): pipeline 2-stage validado. Limitaciones conocidas: clases con NAV=None, AUM umbrella en algunos paraguas, variabilidad LLM, rentabilidad típicamente vía Morningstar (no extractor).
- **Letters quarterly INT pendiente**: no hay agente dedicado para INT (gap actual).

## 5. Quality loop + dashboard

- **106 reglas declarativas** (`data/quality_rules.json`): scope ES/INT, severidad (info/warn/critico), problema + acción + agente_responsable. Editar el JSON para añadir reglas, no hardcodear.
- **Anti-regresión obligatoria**: backup pre-iter (`output.iter_N.json`). Si iter N tiene más fallos que N-1 → restaurar N-1.
- **`max_iter=1` por defecto**: análisis Mayo 2026 mostró que iter 2+ raramente aporta valor. Override con `QUALITY_LOOP_MAX_ITER=N` para fondos problemáticos.
- **Loop puede EMPEORAR datos** si re-ejecuta agentes de scraping sin merge. El analyst debe fusionar con output anterior, no sobrescribir.
- **5 reglas anti-bucle**: sin warm-up, sin retracción excesiva, fallback determinista, no inventar texto en re-tries, log iterativo de qué cambió.

## 6. Dashboard (HTML generador)

- **38 reglas estructurales** validadas con 5+ fondos: header completo (gestora/depositario/divisa cascadas), SRRI nunca inventado, ISIN visible + clases EUR, breakdown KPI con eje Y 100%, var sin "NUEVO" sin histórico, citas reconstruidas, cartera multi-asset, tipo activo completo.
- **Markdown bold renderiza**: `_re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', x)` en fortalezas/riesgos/timeline. Sin esto, queda `**texto**` literal.
- **NUNCA texto relleno/inventado**: si no hay dato, omitir sección o decir "no disponible". No "se considera un fondo equilibrado…" genérico.
- **Cualitativo literal del extractor**: no parafrasear. La gestora dice X → escribir X entre comillas.
- **Cambios gestor/ownership con detalle máximo**: histórico de quién entró/salió y cuándo. Sin esto, perfil incompleto.

## 7. Cost optimization (Fase Cost-Opt + Fase M)

- **Cache LLM local**: hash(model+prompt+context), TTL 24h. Re-runs con mismo prompt → cache hit instantáneo (FREE). `data/.llm_cache/`.
- **Anthropic prompt caching**: system prompts >2000 chars como bloque cacheable. 90% descuento en cache hits. Aprovecha que las 8 secciones del analyst comparten `_system_role`.
- **Skip secciones existentes**: `SKIP_EXISTING_SECTIONS=1` env (default ON post-Fase M). Salta secciones con texto >100c y mtime <30d.
- **Fallback Anthropic ↔ Gemini**: si Gemini devuelve 403/PERMISSION_DENIED, cae automáticamente en Haiku 4.5 (Anthropic). Permite operar durante incidencias de billing.
- **Quality loop max_iter=1**: bajado de 5 a 1 (Cost-Opt O9). Override solo para fondos problemáticos.
- **4 hints Opus → Haiku** (Fase M): `manager_profiler._enrich_with_opus`, `discovery_v2._opus_suggest_sources`, `letters_collector._opus_historical_hints`, `readings_collector._suggest_sources`. Tareas de clasificación pura — Haiku basta. Ahorro ~$0.15/fondo (45% del Opus).
- **Audit del analyst SE QUEDA en Opus**: 3 calls (`_opus_confirm_issue`, `_opus_audit_per_section`, `_audit_and_fix_loop`) requieren reasoning profundo. Haiku no llega.
- **Coste real proyectado post-Fase M**:
  - Fondo nuevo ES: $0.15-0.25 (vs $0.30-0.40 baseline)
  - Re-run mismo fondo: $0.01-0.05 (vs $0.20-0.30)

## 8. Datos integrity + manual edits

- **`tools/output_accessor.py`** = único punto de lectura. ~50 getters. NUNCA `data["X"]` directo. Si schema cambia, solo se actualiza el accessor.
- **`tools/output_merger.py`** = preserva `_manual_edits[]` entre runs. Marca path con `mark_manual_edit(data, "path.dotted")` y el analyst no lo sobrescribirá.
- **`tools/regression_guard.py`** = no acepta sección con menos contenido que la versión anterior.
- **Patches manuales sobreviven**: scripts como `patch_all_nombres.py`, `clean_invented_gestores.py` marcan paths como `_manual_edits` y ejecutan via `output_merger.save_output()`.

## 9. Anti-invención + validation

- **Anti-invención v2** (Fase I): validador post-LLM filtra entidades no presentes en `fuentes_web`. No genera perfiles de gestores que no aparecen en NINGUNA fuente.
- **Schema unificado equipo/equipo_gestor** (Fase I): `equipo` (dicts con cargo, biografía) y `equipo_gestor` (lista plana max 2). Schema dual permite acceso flexible.
- **Dedup acentual + cross-fund**: `Álvaro Guzmán` y `Alvaro Guzman` son la misma persona. Cross-fund check evita asignar mismo gestor a fondos no relacionados.
- **Opus identifica lead/co/confidence** (Fase J): reemplaza heurística arbitraria de "orden detección". Si confidence=low → tratar todos como peers (no inventar lead/co).

## 10. Process locks + concurrencia

- **Lock por ISIN** (`tools/process_lock.py`): cada agente (extractor/analyst/quality/orchestrator) adquiere lock al iniciar. Si ya hay otro vivo en el mismo ISIN, aborta.
- **Compatible con Windows**: maneja PID + stale detection.
- **Evita race conditions** entre quality loop + manual rerun + bash batch.

---

## Referencias originales

Los aprendizajes anteriores están destilados de los siguientes archivos en la memoria local del autor (`~/.claude/projects/.../memory/`):

- `feedback_dashboard_*.md` (6 versiones)
- `feedback_discovery_*.md` (3 versiones)
- `feedback_quality_*.md` (rules v3, v4, v6, loop_safeguards)
- `feedback_extractor_int_rules.md`
- `feedback_fase_*.md` (c, e)
- `feedback_kiid_extraction.md`
- `feedback_pipeline_serial.md`
- `feedback_process_locks.md`
- `feedback_unicode_windows_orchestrator.md`
- `feedback_cnmv_enrichment_pattern.md`
- `feedback_loop_anti_regression.md`
- `feedback_subfondo_disambiguation.md`
- `feedback_nombre_match_pdf.md`
- `feedback_es_pipeline_locked.md`
- `feedback_dashboard_int_*.md` (rules, v2)
- `feedback_dashboard_final_v3.md` (30 reglas consolidadas)
- `feedback_dashboard_header.md`
- `feedback_dashboard_principles.md`
- `feedback_myinvestor_failure.md`
- `feedback_quality_rules_process.md`
- `project_extractor_v3_validated.md`
- `project_letters_readings_v3.md`
- `project_intl_roadmap.md`
- `project_intl_regulators.md`
- `project_int_pdf_analysis.md`
- `project_holdings_cascade.md`

No se incluyen los archivos en sí porque contienen datos específicos de fondos analizados (output.json, decisiones puntuales) que no son necesarios para replicar el sistema.
