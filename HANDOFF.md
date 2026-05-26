# HANDOFF — Sesión 2026-05-19 (v2-cowork)

**Fecha:** 2026-05-19
**Estado:** Working tree con todos los cambios aplicados, sin commit.
**Tests:** 62+ pasados (49 anteriores + 13 nuevos Claude Code + tests adicionales).

---

## Resumen ejecutivo

Sesión enfocada en hacer el pipeline de análisis de fondos INT **robusto a fallos** (cierre sesión Windows, agotamiento de tokens Claude Max, runs interrumpidos) + **mejorar la UX del catálogo** (cola multi-ISIN, categorización post-análisis, reanudación fácil).

### 3 bugs raíz que arreglamos en orden:

1. **G1 — `fund_name.split(" - ")[-1]` rompía queries para fondos INT multi-clase**
   - `"SEXTANT QUALITY FOCUS - Action F"` se convertía en `"Action F"` (genérico, 0 resultados Google)
   - Fix: `tools/fund_name_utils.py` con `extract_fund_short()` que detecta patrones de clase
   - Aplicado en 3 agentes: `readings_collector.py`, `letters_collector.py`, `manager_profiler.py`

2. **B1 — `proc.wait()` retornaba prematuro en Win11/Py3.14 + CREATE_NEW_PROCESS_GROUP**
   - Worker pasaba al siguiente run sin esperar al actual → 3 procesos paralelos
   - Fix: `proc.poll()` loop + cap 2h en `_queue_worker` y `watch_run`

3. **B2/P2 — `manager_profile.json not found` abortaba todo el pipeline**
   - Si `manager_profiler` no encontraba gestores (común INT sin equipo público), no escribía archivo → `bundle_exporter` abortaba
   - Fix doble: (a) `manager_profiler.run()` envuelve TODO en try/except y SIEMPRE escribe identity-only si falla; (b) `bundle_exporter` crea fallback si missing en lugar de abortar

---

## Componentes nuevos

### `tools/fund_name_utils.py` (NUEVO)
Helper para parseo robusto de nombres de fondos. Detecta patrones de clase comercial al final del nombre (`Action F`, `Class I`, `B EUR Acc`, etc.) y los descarta para queries Google.

### `tools/cleanup_supabase_isins.py` (NUEVO)
Limpia ISINs de Supabase (DB + Storage). Maneja `is_curated_universe` (reset campos si está en Excel maestro, delete row si es huérfano).

```bash
python -m tools.cleanup_supabase_isins --dry-run ISIN1 ISIN2 ISIN3
python -m tools.cleanup_supabase_isins ISIN1 ISIN2 ISIN3
```

### `tools/cleanup_test_runs.ps1` (ampliado)
Script PowerShell que limpia disco + Supabase + dashboards HTML:

```powershell
.\tools\cleanup_test_runs.ps1 -Isins X,Y,Z -ResetRegistry -ResetSupabase
```

### `tools/intl_url_filter.py` (NUEVO — Claude Code)
Filtro de URLs HTML candidatas para extracción cuanti. Blacklist (`/notre-vision`, `/journal-de-bord`, etc.) + whitelist (`/factsheet`, ISIN en URL).

---

## Cola multi-ISIN (`tools/web_server.py`)

### Endpoints añadidos (16 total)

| Endpoint | Función |
|---|---|
| `POST /api/analyze-batch` | Encola N ISINs `{isins: [...], cold_start: bool}` |
| `GET /api/queue` | Estado actual + stats |
| `DELETE /api/queue/<isin>` | Quita pendiente |
| `POST /api/queue/clear-finished` | Limpia entradas terminadas |
| `POST /api/queue/force-clear` | Limpia TODO (incluso zombies) |
| `POST /api/queue/check-tokens` | Chequeo manual tokens Max |
| `POST /api/queue/<isin>/resume` | Reanuda un ISIN |
| `POST /api/queue/resume-all` | Reanuda todos los reanudables |
| `POST /api/update-fund/<isin>` | Actualiza categorización en Supabase |

### Estados de items en cola

| Status | Significa | Botones |
|---|---|---|
| `queued` | Esperando worker | × quitar |
| `running` | Analizando | (ninguno) |
| `done` | Terminó OK | (ninguno) |
| `completed_with_warnings` | Exit ≠0 pero output válido | ↻ reanudar |
| `failed` | Sin output válido | ↻ reanudar |
| `interrupted` | Cierre sesión / crash | ↻ reanudar |
| `paused_waiting_tokens` | Claude Max sin tokens | ↻ + × |

### Persistencia

`data/queue_state.json` se actualiza tras cada cambio (atómico tmp + rename):
- `items`: todos los chips
- `tokens_blocked_until`: timestamp bloqueo tokens
- `tokens_last_check`: último chequeo

Al startup, `_load_queue_state()`:
- Items `running` → `interrupted` (zombies)
- Items `queued` → restaurados + worker arranca
- Items `paused_waiting_tokens` → restaurados + thread monitor arranca
- `tokens_blocked_until` respetado si aún no venció

### Detección de tokens Claude Max agotados

Worker monitoriza el log cada 5s buscando 13 patrones (`rate limit`, `quota exceeded`, `credit balance too low`, `model overloaded`, etc.).

Si detecta → mata bat → pausa cola → arranca monitor → check cada 30 min con `claude -p "ok"` hasta tokens disponibles.

User puede forzar verificación con botón "Verificar tokens ahora" o `POST /api/queue/check-tokens`.

### Detección de procesos huérfanos al startup

`_detect_orphan_bats()` reporta procesos bat sin parent web_server. Sugiere `Stop-Process -Id N`. Requiere `psutil` (skip silencioso si falta).

---

## UI del catálogo (`dashboard/catalog.html`)

### Banner cola (visible cuando hay items)

- Chips con colores por status + iconos
- Botón × en queued/paused (quitar)
- Botón ↻ en interrupted/paused/failed/completed_with_warnings (reanudar)
- Botones globales:
  - **↻ Reanudar todos** (merge: reusa datos parciales)
  - **🔄 Cold-start todos** (limpio: borra antes)
- Alert amarillo cuando hay items con problemas
- Alert naranja cuando tokens agotados con countdown

### Modal "+ Analizar nuevos ISINs"

- Textarea multi-ISIN (línea / coma)
- Botón "Encolar" + "Ver progreso →"

### Modal progreso multi-run

- Strip "Cola: 3 de 5 — siguientes: ABC, DEF"
- Tras `done`: "Abrir dashboard", "✏ Categorizar", "→ Siguiente run"
- Auto-transición al próximo run

### Por fila

- **📊** abre fund-dashboard cuanti
- **✏** modal categorización (clasificación, opinión, encaje, notas)
- **🔄** Regenerar si análisis incompleto
- Columna **Última act.** con sort DESC + filtro días
- Badge **⚠** en completitud si < 60%

### Filtros nuevos

- **Universo**: curado / huérfanos
- **Última act.**: 24h / 7d / 30d / 90d
- **Análisis cualitativo**: añade "⚠ Necesita revisión (compl. < 60%)"

---

## Pipeline INT — HTML fallback (P5)

`agents/intl_extractor_v2.py`:
- `_expand_candidates_from_aggregators()`: añade Morningstar/Quantalys/Citywire
- `_expand_candidates_from_prospectus()`: extrae URLs de PDFs
- Prompt Gemini anti-agregación (bug DNCA-€41B evitado)
- Auto-learn `gestoras_registry.json` cuando exitoso

`agents/discovery_v2.py`:
- `persist_html_fallback_to_registry()` expuesto módulo
- Respeta `auto_learned=false` (manual)

---

## Tests (~110 total)

```
tests/test_import_taxonomy_v2.py       18
tests/test_fund_group_cache.py         10
tests/test_discovery_v2_g5_g6.py       21
tests/test_intl_html_fallback.py       13
tests/test_fund_name_utils.py          18 (NUEVO M5)
tests/test_token_exhaustion_detection  11 (NUEVO M4)
tests/test_fase_l_fixes.py             19
tests/test_int_no_regression.py        2
```

Sanity:

```powershell
python -m pytest tests/ -q
```

---

## Cambios per archivo

```
M  agents/discovery_v2.py            (G5-G8 + helper persist_html_fallback)
M  agents/intl_extractor_v2.py       (G11 HTML fallback + BUG-C + BUG-D)
M  agents/letters_collector.py       (G1)
M  agents/manager_profiler.py        (G1 + P2 always-write)
M  agents/orchestrator.py            (B6 consume_html_fallback + P3)
M  agents/bundle_exporter.py         (P2 fallback)
M  dashboard/catalog.html            (F7 cola + UX1 + M3 + P4b/d/e)
M  dashboard/generate_dashboard.py   (P3 timeouts + DASHBOARD_SKIP_ENRICH)
M  tools/web_server.py               (F7 + B1-B4 + P4 tokens + M1-M2)
M  tools/import_taxonomy.py          (fix regresión agrupar_por_fondo)
M  data/gestoras_registry.json       (auto_learned)
M  data/trusted_sources.json         (amiralgestion.com)
M  analizar_fondo.bat                (G12 exit 0/5/10)

?? tools/fund_name_utils.py          (NUEVO G1)
?? tools/cleanup_supabase_isins.py   (NUEVO P1)
?? tools/cleanup_test_runs.ps1       (NUEVO P1)
?? tools/intl_url_filter.py          (NUEVO G13)
?? tests/test_fund_name_utils.py     (NUEVO M5)
?? tests/test_token_exhaustion_detection.py (NUEVO M4)
?? tests/test_intl_html_fallback.py  (NUEVO)
?? tests/test_fund_group_cache.py    (NUEVO)
?? tests/test_discovery_v2_g5_g6.py  (NUEVO)
```

---

## Cómo proceder cuando algo va mal

### Tokens Claude Max agotados
- Cola se autopausa
- Banner naranja con countdown
- Monitor reanuda solo al volver tokens
- Manual: botón "Verificar tokens ahora"

### Cierre sesión / wifi / crash
- queue_state.json sobrevive
- Al relanzar `python -m tools.web_server`:
  - running → interrupted
  - queued → siguen
  - paused → monitor arranca solo
- Banner amarillo con "↻ Reanudar todos"

### Análisis con warnings o quality bajo
- Status `completed_with_warnings` o `done` con badge ⚠
- Filtro "Necesita revisión" en catálogo
- Botón 🔄 Regenerar en fila (cold-start)
- O ↻ Reanudar en chip cola (merge)

### Procesos huérfanos tras crash
- Web_server al arrancar imprime aviso con PIDs
- Sugiere `Stop-Process -Id N`
- NO mata automáticamente

---

## Coste estimado por fondo

| Caso | Coste | Tiempo |
|---|---|---|
| Fondo ES nuevo (cache vacío) | $0.20-0.40 | 35-50 min |
| Fondo INT nuevo cache vacío | $0.30-0.50 | 40-55 min |
| HTML fallback (+ extra) | +$0.02-0.05 | +30s |
| Segundo del fund_group (cache hit F6) | $0.02-0.05 | 10-15 min |
| Re-análisis cache LLM caliente | $0.05-0.10 | 25-35 min |

---

## Pendiente próximas sesiones

- **N5**: resume real en bat (skip pasos completos cuando `--no-cold-start`) — Claude Code
- **N1**: verificar G12 exit codes 0/5/10 en bat — Claude Code
- **N3**: tests E2E end-to-end — Claude Code
- Cloudflare Tunnel acceso multi-dispositivo
- GitHub Pages catalog frontend (solo lectura)
- Pre-commit hook anti-acceso directo schema
- BBDD horfin integration (multi-broker)
- Investigar fondo ES0119207001 (analizado pero sin HTML local)

---

## Commit sugerido cuando esté validado

Sin commits hasta validar F5 (re-lanzar 5 ISINs con todos los fixes y verificar).

Cuando validado:

```powershell
cd "C:\Users\RafaelGarcía\OneDrive - Nazca\Escritorio\fund-analyzer"
git add -A
git status

# Mensaje sugerido:
git commit -F .commit_msg.tmp
# (preparar el mensaje en .commit_msg.tmp con encoding UTF-8 sin BOM)

git push origin v2-cowork
```

Resumen del commit: "feat(robustez+ux): cola multi-ISIN, autopause tokens Max, HTML fallback INT, resume tras crash, 110 tests"
