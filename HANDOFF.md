# HANDOFF — Bootstrap Supabase Fase 1 + Backfill + Migración v3

**Fecha:** 2026-05-14
**Estado:** Esperando reporte de Claude Code con los 6 pasos de migración a v3.

---

## ✓ Cronología de lo hecho

### Día 1 (2026-05-13) — Fase 1 inicial con Excel v2

| Archivo | Cambio |
|---|---|
| `tools/import_taxonomy.py` | Refactor v1 → schema Supabase (fund_groups + funds), 8 campos nuevos, mapping Excel v2 |
| `tools/supabase_client.py` | NUEVO — cliente lazy + test_connection |
| `tools/sync_to_supabase.py` | NUEVO — post-bat upload a Storage + tables, backfill mode |
| `dashboard/catalog.html` | loadCatalog fetch directo a Supabase REST, JOIN client-side, anon key embebida |
| `analizar_fondo.bat` | Paso 7 nuevo: `python -m tools.sync_to_supabase %ISIN%` |
| `data/excel_corrections.json` | NUEVO — 6 correcciones + 6 exclusiones |
| Supabase | 4 tablas creadas (fund_groups, funds, analysis_runs + fund_candidates opcional), 8 columnas ALTER, RLS disabled |

**Resultado día 1:**
- 147 fund_groups + 154 funds del Excel v2 ✓
- Catalog cargando desde Supabase ✓
- **Pero:** v2 solo tenía 30 fondos clasificados (Top=3, Bueno=4, Clase_similar=22, Clase_sucia=1)

### Día 1 bonus — Backfill de 26 análisis

```
python -m tools.sync_to_supabase --backfill-all
```

- 17 fondos OK en primera pasada
- 9 fondos fallaron por `'str' object has no attribute 'get'` (campos heterogéneos en output.json antiguos)
- Cowork aplicó fix defensivo en `sync_to_supabase.py` con helpers `_safe_get_dict`, `_safe_get_list`, `_safe_get_str`
- Retry de los 9 → 9/9 OK
- **Total: 26 análisis cualitativos en Supabase** con `has_qualitative_analysis=true`, `dashboard_storage_path` poblado, etc.

### Día 2 (2026-05-14) — Migración a Excel v3 "Def"

Usuario sube `20260513_Listado Fondos Def.xlsm` — el universo curado COMPLETO con 134/160 clasificados (vs 30/154 del v2).

| Cambio | Detalle |
|---|---|
| `data/fund_taxonomy_source.xlsx` | Sustituido con contenido del v3 .xlsm (137 KB) |
| `data/excel_corrections.json` | **Migrado a schema 2.0**: matching por `match.isin + match.nombre_starts` (robusto a cambios de fila entre versiones del Excel) |
| `tools/import_taxonomy.py` `build_correction_indexes()` | Soporta schema 1.0 (legacy por fila_excel) Y 2.0 (por ISIN+nombre) |
| `tools/import_taxonomy.py` `find_matching_correction()` | NUEVO — busca corrección aplicable con prioridad: ISIN+nombre > ISIN-only > fila |
| `tools/import_taxonomy.py` `upload_to_supabase()` | **PRESERVA campos del backfill** — excluye `has_qualitative_analysis`, `*_storage_path`, `gestores_perfiles_json`, `top_holdings_json`, `filosofia/estrategia/historia`, `fecha_ultimo_analisis`, etc. del upsert. Solo actualiza campos del Excel. Los 26 análisis sobreviven. |

**Esperado tras migración v3:**

| Métrica | Antes (v2) | Esperado (v3) | Comentario |
|---|---|---|---|
| ISINs en `funds` | 165 | ~170 | 160 del v3 + ~10 huérfanos del backfill |
| `fund_groups` | 158 | ~158 | similar (matching uuid5 idempotente) |
| Top | 3 | **63** | salto grande |
| Bueno | 4 | **42** | salto grande |
| Medio | 0 | 2 | |
| Clase_similar | 22 | 26 | |
| Clase_sucia | 1 | 1 | |
| Sin clasificar | 124 | 26 | (resto, sigue en watchlist) |
| `has_qualitative_analysis=true` | 26 | **26** | preservado (crítico) |
| `dashboard_storage_path` | 25 | **25** | preservado |

---

## Lo que debe haber pasado en tu hora fuera

Claude Code ejecutó autónomo los 6 pasos del prompt que le pasaste:

1. AST/Import válidos
2. Snapshot pre-upload
3. Dry-run con v3 → debe reportar 160 ISINs + distribución esperada
4. Upload real (con upsert que preserva backfill)
5. Snapshot post-upload → CRÍTICO: `analyzed` sigue siendo 26
6. Spot check Cobas ES0119199000 → `has_qualitative_analysis=True`, dashboard_storage_path no null

**Cuando vuelvas, lee el reporte de Claude Code.** Casos posibles:

- ✅ **Todo OK**: 63 Top, 42 Bueno en catalog; 26 análisis preservados. Pasamos al commit + push final.
- ⚠️ **Números cuadran pero analyzed bajó de 26 a 0**: el upsert NO preservó campos. Hay que investigar supabase-py 2.15.3 `upsert` behavior.
- ❌ **Dry-run no cuadra**: corrections.json mal configurado o el matching falla.

---

## TUS pasos cuando vuelvas (estimado 10 min)

### 1. Lee reporte de Claude Code

Resumen rápido: ¿paso 5 muestra `analyzed: 26`? Si sí → todo OK. Si no → me dices y arreglamos.

### 2. Hard reload catalog

```
http://localhost:5000/dashboard/catalog.html
```

Pulsa `Ctrl+Shift+R`. Stats esperados:
- **Universo total: ~170**
- **Top: 5/63** (5 del backfill que son Top + 58 no analizados todavía)
- **Bueno: ?/42**
- **Análisis cualitativo: 26**
- Filtra por "Top" → debe aparecer 63 filas

### 3. Commit + push (si todo OK)

Te dejo el bloque listo aquí abajo, sección "Commit message preparado".

---

## Commit message preparado (UTF-8 sin BOM)

Cuando todo esté OK, pega esto en PowerShell:

```powershell
cd "C:\Users\RafaelGarcía\OneDrive - Nazca\Escritorio\fund-analyzer"

# Verificar staging
git status --short

# Si quieres ver qué se va a commitear:
git diff --stat tools/import_taxonomy.py tools/supabase_client.py tools/sync_to_supabase.py dashboard/catalog.html data/excel_corrections.json HANDOFF.md analizar_fondo.bat

# Staging selectivo (sin datos de fondos)
git add tools/import_taxonomy.py tools/supabase_client.py tools/sync_to_supabase.py
git add dashboard/catalog.html
git add data/excel_corrections.json
git add HANDOFF.md
git add analizar_fondo.bat

git status --short

# Commit message
$msg = @"
feat(supabase): bootstrap Fase 1 + backfill 26 análisis + migración v3

Fase 1 (universo curado a Supabase):
- tools/import_taxonomy.py refactor (Excel v3 hoja Listado fondos)
- tools/supabase_client.py nuevo (lazy + test_connection)
- tools/sync_to_supabase.py nuevo (post-bat upload + backfill mode)
- dashboard/catalog.html: fetch directo a Supabase REST + JOIN client-side
- analizar_fondo.bat: paso 7 sync automático tras cada análisis

Backfill (26 análisis cualitativos existentes a Storage):
- Fix defensivo en sync_to_supabase para campos heterogéneos en output.json
- _safe_get_dict/list/str helpers
- 26 dashboards subidos a bucket 'funds-data'

Migración v2 -> v3 'Def' (160 ISINs, 134 clasificados):
- excel_corrections.json schema 2.0: matching por ISIN+nombre_starts (robusto)
- import_taxonomy upsert PRESERVA campos del backfill (no sobreescribe
  has_qualitative_analysis, *_storage_path, gestores_perfiles_json,
  top_holdings_json, filosofia, estrategia, historia, fecha_ultimo_analisis)

Resultado final Supabase:
- ~170 funds | ~158 fund_groups
- Distribucion clasificacion: Top=63, Bueno=42, Medio=2, Clase_similar=26, Clase_sucia=1
- 26 con analisis cualitativo + dashboard en Storage

Decisiones tomadas (documentadas en chat sesion):
- Schema fund_groups (FONDO) + funds (CLASE) separadas
- 8 campos extra via ALTER (encaje, evaluacion_jsonb, horizonte_temporal,
  broker_disponible, anos_antiguedad, rendimiento/portfolio/equipo_metrics_jsonb)
- enum clasificacion_user: Top/Bueno/Medio/Clase_similar/Clase_sucia
- Storage privado bucket 'funds-data', signed URLs en frontend
- Excel maestro queda como bootstrap fuente; sync Excel<-Supabase en futuro
- Hibrido C: fund_groups.class_isins_known TEXT[] (no filas zombies)
- Fondos huerfanos del backfill insertan como fund_groups+funds nuevos

Pendiente proximas sesiones:
- Marcar huerfanos del backfill con flag 'no curado' (is_curated_universe)
- Implementar fund_candidates desde hojas brokers (ABANCA, Mapfre, etc.)
- BBDD horfin integration (multi-broker availability)
- Cloudflare Pages o tunnel para acceso multi-dispositivo publico
- Investigar fondo ES0119207001 sin dashboard HTML local
"@

[System.IO.File]::WriteAllText(
    (Join-Path (Get-Location) ".commit_msg.tmp"),
    $msg,
    (New-Object System.Text.UTF8Encoding($false))
)

# Verifica primer byte (espera 66 = 'f')
[byte[]](Get-Content ".commit_msg.tmp" -Encoding Byte -TotalCount 3) | ForEach-Object { '{0:X2}' -f $_ }

# Commit
git commit -F .commit_msg.tmp
Remove-Item .commit_msg.tmp

# Push
git push origin v2-cowork

# Verificar
git log --oneline -3
```

---

## Pendientes (próximas sesiones)

| # | Tarea | Esfuerzo |
|---|---|---|
| 1 | Tabla `fund_candidates` con hojas broker (ABANCA, Mapfre, BBVA, CJRS) | 1-2h |
| 2 | Flag `is_curated_universe` en fund_groups para distinguir Excel vs huérfanos backfill | 30 min |
| 3 | Investigar fondo ES0119207001 (analizado pero sin HTML local) | 15 min |
| 4 | Cloudflare Tunnel para acceso desde móvil sin estar en local | 45 min |
| 5 | Sync Supabase → Excel (read-only output del estado actual) | 1h |
| 6 | Integración cuantitativa: cross-link a fund-dashboard `?isin=X` | 30 min |
| 7 | Botón "+ Analizar nuevo ISIN" del catalog conectado a Supabase (registra en Supabase mientras corre el bat) | 1-2h |
| 8 | Tests actualizados para `import_taxonomy.py` schema v3 | 1h |

---

## Estado de tasks (mid-sesión)

| # | Tarea | Status |
|---|---|---|
| #1 | A — Setup Supabase | ✅ completed |
| #2 | B — Setup local | ✅ completed |
| #3 | C — Adaptar import_taxonomy | ✅ completed |
| #4 | D — Bootstrap upload v2 | ✅ completed |
| #5 | E — Modificar catalog.html | ✅ completed |
| #6 | F — Validación E2E | 🟡 in_progress (esperando paso 6 Claude Code) |
| #7 | G — Refactor catalog.html | ✅ completed |
| #8 | H — sync_to_supabase.py | ✅ completed |
| #9 | I — Migración Excel v3 | 🟡 in_progress (esperando ejecución Claude Code) |
