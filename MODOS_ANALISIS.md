# MODOS DE ANÁLISIS — funcionamiento canónico (2026-09-09)

Tres modos, comportamiento DISTINTO. El `scope` los distingue (`config.json.modo` + env `FUND_SCOPE`).
Regla de oro: **cada modo gatea qué se busca y cómo se sintetiza. La discovery/sourcing NUNCA es
"completa" salvo en el modo 1.**

---

## 1 · Análisis completo (`scope=full`)
**Cuándo:** fondo NUEVO, o fondo con POCO histórico de info disponible (re-análisis exhaustivo).
- **Discovery:** COMPLETA (busca toda la web: gestora, Wayback, Finect, g5_universal_search…).
- **Sourcing multi-año (ar-sourcing):** SÍ — AR/SAR/cartas de TODOS los años desde el lanzamiento (gap-targeted).
- **Lineage:** SÍ (si joven <7a o gap).
- **Extract / manager / letters / readings:** todo.
- **Analyst:** síntesis COMPLETA desde cero.

## 2 · Actualización anual (`scope=annual_update`)
**Cuándo:** fondo con histórico OK; queremos info del ÚLTIMO año / últimos años.
- **Discovery/sourcing FOCALIZADA (FUNDAMENTAL):** busca SOLO los docs NUEVOS desde el ÚLTIMO archivado
  **POR TIPO** (AR/SAR/carta), usando `publication_calendar` + `FUND_SINCE_DATE`. **NO re-descubre todo.**
  No re-busca años/tipos ya archivados (gap-targeting por tipo).
- **Lineage:** NO (ya resuelto).
- **Extract:** solo los docs nuevos.
- **Analyst:** ACTUALIZA **preservando el histórico**. Añade bloque **"Novedades {año}"**:
  - Cambios RELEVANTES del último año (sobre todo **cartera** y **consistencia**).
  - Qué **sigue igual** en estrategia / filosofía / equipo.
  - **NO quita peso al histórico** — lo mantiene intacto encima y añade el delta.

## 3 · Mejora con aporte local de documentos (`scope=aporte`)
**Cuándo:** Rafa sube material (docs_aportados/analisis_externos) para complementar/mejorar.
- **Discovery/sourcing:** **NINGUNO.** No busca nada en la web. Cero.
- **Lineage:** NO.
- **Extract:** SOLO los docs aportados (`raw/aportados/`) — se suman a los que ya tiene extraídos.
- **Readings/letters:** el aporte entra como **fuente prioritaria**.
- **Analyst:** COMPLEMENTA las conclusiones previas con el aporte. **NO quita peso a lo previo**:
  - Integra la info nueva del aporte.
  - Señala cambios RELEVANTES que revele el aporte, y qué **sigue igual** en estrategia/filosofía.
  - Mantiene el análisis existente y lo mejora, no lo rehace.

---

## Gating por `scope` (dónde se aplica en el código)

| Paso | full | annual_update | aporte |
|---|---|---|---|
| Discovery (`orchestrator` prep) | completa | focalizada (since_date por tipo) | **SKIP** |
| ar-sourcing (`analizar_fondo.bat` 1.6) | multi-año completo | solo años/tipos nuevos | **SKIP** |
| lineage (`bat` 1.7 / prep) | sí (joven/gap) | NO | NO |
| ingest aporte (`aportados.ingest`) | si hay | si hay | **SÍ (única fuente nueva)** |
| extract | todos | solo nuevos | solo aportados (+existentes) |
| analyst (skill) | desde cero | delta "Novedades", preserva histórico | complementa, preserva conclusiones |

**Señal de scope:** `data/funds/{ISIN}/config.json.modo` ∈ {`full`, `annual_update`, `aporte`} +
env `FUND_SCOPE`. El worker del portal lo fija según la acción de Rafa (nuevo/actualizar/aportar).
Los modos 2 y 3 **nunca** disparan discovery completa (era el bug: el aporte caía en `full`).
