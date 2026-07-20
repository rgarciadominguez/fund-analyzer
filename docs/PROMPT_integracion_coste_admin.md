# Prompt de integración — Panel de coste LLM (backend + frontend)

Copia esto y pásaselo a quien integre el panel de coste en la web/herramienta.

---

## Contexto

El pipeline de `fund-analyzer` (Python) registra el coste de cada llamada a los modelos
LLM en un log local append-only (`data/cost_log.jsonl`). Ya existe:

- **Agregación** por fondo, por mes, por categoría, por agente y por modelo
  (`tools/cost_monitor.py`).
- **Puente a Supabase** (`tools/cost_sync.py`) que vuelca dos rollups a Supabase para que
  la web pública los lea sin servidor local.
- **Endpoint local** `GET /api/admin/costs?days=N` (en `tools/web_server.py`) que devuelve
  todo junto.
- **Página de ejemplo** `dashboard/admin.html` (funciona ya, sirve de referencia visual).

Hay que integrarlo en **el backend y el frontend de la herramienta** de forma definitiva.

## Dos categorías de gasto (importante para el UI)

- `analisis_fondos` — texto: síntesis del analyst + generación de descripcion/opinion +
  clasificación de benchmark. **Consume el crédito API de Anthropic.**
- `procesar_imagenes` — visión sobre PDFs. **HOY corre en las skills cowork (`claude -p`)
  y factura a Claude Max, NO al crédito API.** Por eso sale a 0 en el crédito API. Mantener
  la categoría en el UI (columna/tarjeta), con una nota de que va por Max.

## Dos niveles que el usuario quiere ver

1. **Por fondo (análisis de fondo)**: coste acumulado por ISIN.
2. **Mensual**: coste por mes natural, con desglose de categoría.

---

## BACKEND

### 1. Tablas Supabase (ya hay DDL, ejecutar una vez)

Fichero: `data/migrations/2026-07-20_cost_tables.sql`. Crea:

```sql
cost_fund (
  isin text primary key,
  cost_usd numeric, cost_analisis_usd numeric, cost_imagenes_usd numeric,
  n_calls integer, updated_at timestamptz
)
cost_month (
  mes text,          -- 'YYYY-MM'
  categoria text,    -- 'analisis_fondos' | 'procesar_imagenes' | 'otros'
  cost_usd numeric, n_calls integer, updated_at timestamptz,
  primary key (mes, categoria)
)
```
Ambas con RLS de **lectura pública** (SELECT using true) — no hay datos sensibles, son
agregados de coste. Escritura solo con service_role (el pipeline).

### 2. Cómo se pueblan (ya cableado, no tocar)

- `tools/cost_sync.py --apply` hace el rollup completo (todos los fondos + meses).
- `tools/sync_to_supabase.py` llama a `cost_sync.sync_fund_cost(isin)` al final de cada
  análisis → el coste de cada fondo se actualiza solo al sincronizar.
- **Acción recomendada**: añadir `python -m tools.cost_sync --apply` al cron/health-check
  diario para refrescar los meses (el per-fondo ya se refresca en cada sync).

### 3. Endpoint (si el backend es el `web_server` Flask actual)

Ya existe: `GET /api/admin/costs?days=N` → `tools.cost_monitor.admin_overview(days)`, que
devuelve:

```json
{
  "por_categoria": { "total_usd": 13.78, "total_eur": 12.68,
    "categorias": { "analisis_fondos": {"cost_usd":..,"cost_eur":..,"n_calls":..,"pct":..} } },
  "por_agente": [ {"agent":"..","cost_usd":..,"n_calls":..,"tokens":..}, ... ],
  "por_modelo": [ {"model":"..","cost_usd":..,"n_calls":..}, ... ],
  "por_mes":   [ {"mes":"2026-07","cost_usd":..,"cost_eur":..,"n_calls":..,
                  "por_categoria":{"analisis_fondos":..}}, ... ],
  "por_fondo": [ {"isin":"..","cost_usd":..,"cost_eur":..,"n_calls":..,
                  "por_categoria":{...}}, ... ],   // top 25
  "hoy": {...}, "mes": {...}
}
```

Si el backend NO es este Flask (p.ej. el hub en Workers), no hace falta endpoint: el
frontend puede leer las tablas `cost_fund` / `cost_month` **directamente de Supabase** con
la anon key (ver frontend).

## FRONTEND

Panel "Admin · Coste" (pestaña o página). Referencia visual: `dashboard/admin.html`.

### Fuente de datos (dos modos, con fallback)

1. Si hay servidor backend: `GET /api/admin/costs?days=N`.
2. Si es web pública sin backend: leer Supabase directo (anon key, solo SELECT):
   ```
   GET {SUPABASE_URL}/rest/v1/cost_fund?select=*&order=cost_usd.desc&limit=25
   GET {SUPABASE_URL}/rest/v1/cost_month?select=*
   headers: { apikey: ANON_KEY, Authorization: 'Bearer ' + ANON_KEY }
   ```
   `SUPABASE_URL = https://mfbtebngddjjuwfaelat.supabase.co`
   (la anon key ya está embebida en catalog.html; reutilizar la misma.)

   Para `cost_month`: agrupar por `mes` sumando categorías → una fila por mes con desglose.

### Qué mostrar

- **Tarjetas arriba**: total, coste por categoría (Análisis de fondos / Procesar imágenes),
  hoy, proyección de mes.
- **Tabla "Por mes"**: mes · USD · EUR · Análisis · Imágenes · llamadas.
- **Tabla "Por fondo"**: ISIN · USD · EUR · llamadas. (Enlazable a la ficha del fondo.)
- **Tablas "Por agente" y "Por modelo"** (solo cuando hay endpoint; opcionales en público).
- **Nota fija** (obligatoria, honestidad):
  > Solo cuenta llamadas instrumentadas del pipeline Python (crédito API). El procesado de
  > imágenes de PDFs corre en cowork (`claude -p`) y factura a Claude Max, no a este crédito.
  > El saldo autoritativo está en console.anthropic.com → Usage.

### Detalle UI sugerido

- Colores por categoría: análisis = azul, imágenes = morado.
- EUR = USD × 0.92 (ratio aproximado; el backend ya lo da calculado en `cost_eur`).
- Selector de periodo (Hoy / 7d / 30d / Todo) que reconsulta.

## Resumen de ficheros de referencia en el repo

| Fichero | Qué es |
|---|---|
| `tools/cost_monitor.py` | agregaciones (by_fund, by_month, summary_by_category, admin_overview) |
| `tools/cost_sync.py` | rollup → Supabase (cost_fund, cost_month) |
| `data/migrations/2026-07-20_cost_tables.sql` | DDL de las 2 tablas |
| `tools/web_server.py` → `/api/admin/costs` | endpoint local |
| `dashboard/admin.html` | página de referencia (HTML+JS, con fallback a Supabase) |

## Único paso manual pendiente

Ejecutar el DDL `data/migrations/2026-07-20_cost_tables.sql` en Supabase → SQL Editor,
y un `python -m tools.cost_sync --apply` inicial. A partir de ahí se mantiene solo.
