---
name: lineage-resolver-cowork
description: Reconoce el LINEAGE/PREDECESOR de un fondo — estrategias que existían ANTES de su vehículo legal actual (AMC/RAIF/Cayman/feeder → UCITS, fondos renombrados, clases migradas) — para tener track-record e histórico COMPLETO (§0.9). Investiga en web (presentación institucional del gestor, factsheet "since inception", Morningstar, Citywire, bases de fondos) y escribe data/fund_lineage.json vía tools.lineage_kb. Úsala cuando Rafa diga "resolver lineage", "busca predecesor de X", "skill lineage", o cuando el pipeline encole un fondo en data/lineage_queue.json. Caso patrón: MontLake Alpha Fixed Income (UCITS 2024) que viene de un RAIF (2021) y un AMC (2018) de Fortune Financial Strategies.
---

# lineage-resolver-cowork v1

Corre bajo Claude Max (sin coste API). Identifica si la estrategia de un fondo tiene **track record anterior a su vehículo legal actual** y lo registra para que el pipeline arme el histórico completo (quant + narrativa + cartera).

## Cuándo se dispara
- El pipeline (prep) encola en `data/lineage_queue.json` los fondos INT jóvenes (<7 años) o con gap de histórico y posible señal de predecesor (serie NAV real de Morningstar empieza antes del lanzamiento legal — ver `tools/lineage_detect.py`).
- O Rafa pide resolver un ISIN concreto.

## Entrada
- `data/lineage_queue.json` (`pending: [{isin, age, predecessor_signal, longest_series_isin, longest_series_start, gap}]`), o un ISIN dado.
- `data/funds/{ISIN}/output.json` para la identidad: `nombre`, `gestora`, gestores, ISINs de clases, fecha de inicio.
- El resultado del detector determinista (ya te dice si la serie real empieza antes del lanzamiento legal, y en qué clase).

## Qué investigar (web) — patrón validado en MontLake
Para el fondo, su **gestora** y su **lead PM**, busca un predecesor con el MISMO gestor/estrategia:
1. **Presentación institucional / factsheet del gestor**: busca "since inception", "track record since", una fecha de inception MUY anterior al lanzamiento legal, y **notas al pie** tipo "Performance until <fecha> derived from <ISIN/nombre>". Es la prueba más fuerte (así se resolvió MontLake: la presentación empalmaba AMC → RAIF → UCITS con ISINs XS…/LU… en las notas).
2. **Morningstar / Citywire / Bloomberg LEI / Fundsquare / bases de fondos**: ¿existe un fondo MÁS ANTIGUO con el mismo nombre/estrategia y el MISMO gestor? (distinto ISIN/domicilio/estructura).
3. **Web de la gestora**: histórico de la estrategia, "our funds", relanzamientos, "the strategy has been managed since…".
4. Tipos de predecesor habituales: **AMC** (ISIN XS…, certificado gestionado), **RAIF/SICAV** (LU…), **fondo Cayman/offshore**, **feeder**, o el mismo fondo con **otro nombre** (renombrado).

## Reglas de rigor (OBLIGATORIAS)
- Solo cuenta como predecesor si es el **MISMO gestor/firma** (o el mismo lead PM) y la **MISMA estrategia**. Un fondo con nombre parecido de otra gestora NO cuenta.
- Distingue **continuación legal** (raro: mismo fondo migrado) de **vehículo separado con la misma estrategia** (lo común).
- **Caveats de honestidad**: si el track viene de la presentación del gestor es **auto-reportado**; si el vehículo previo tenía **mandato distinto** (p.ej. RAIF más laxo que UCITS), decláralo. No inventes ISINs ni fechas: cada dato con su fuente.
- Si NO encuentras predecesor creíble, escribe un registro con `predecessors: []` y `confidence: "none"` + qué comprobaste (para no volver a investigarlo en vano).

## Salida — escribe con `tools.lineage_kb.upsert(record)`
Ejecuta Python (`python -c "from tools.lineage_kb import upsert; upsert({...})"`) con este schema:
```json
{
  "isin": "IE000Z9YV312",
  "strategy_name": "Fortune Alpha Fixed Income",
  "manager": "Fortune Financial Strategies S.A.",
  "lead_pm": "Simon Khalili",
  "strategy_inception": "2018-10",
  "current_vehicle": {"isin": "...", "name": "...", "type": "UCITS", "from": "2024-05-23"},
  "predecessors": [
    {"isin": "XS...", "name": "...", "type": "AMC|RAIF|SICAV|Cayman|renamed",
     "from": "YYYY-MM", "to": "YYYY-MM", "morningstar_secid": null,
     "nav_series_available": false, "caveat": "..."}
  ],
  "track_record": {
    "quant_series_isin": "<clase (idealmente EUR) cuya serie Morningstar cubre más histórico real>",
    "quant_series_start": "YYYY-MM-DD",
    "quant_note": "de dónde sale el histórico y qué caveat lleva"
  },
  "sources": ["url1", "url2"],
  "confidence": "high|medium|low|none",
  "caveat_global": "...",
  "resolved_at": "YYYY-MM-DD",
  "resolved_by": "skill:lineage-resolver-cowork"
}
```
Consejo para `track_record.quant_series_isin`: usa el detector (`python -m tools.lineage_detect {ISIN}`) — su `longest_series_isin`/`longest_series_start` te dan la clase con la serie NAV real más larga (prefiere la EUR si empata). Ahí es donde Morningstar ya empalmó el predecesor.

## Después de escribir
- Si hay un predecesor con AR propios (p.ej. un RAIF que publica cuentas), añádelo a `data/known_annual_reports.json` (para que `fetch_annual_report` baje sus AR y se extienda el histórico de cartera).
- Quita el fondo de `data/lineage_queue.json` (`pending`).
- Resume en 3 líneas: predecesores hallados, desde cuándo es el track real, y caveats.
