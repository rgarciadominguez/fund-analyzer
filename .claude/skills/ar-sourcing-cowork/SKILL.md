---
name: ar-sourcing-cowork
description: Fuerza el sourcing MÁXIMO de documentos multi-año de un fondo — informes anuales (AR), semestrales (SAR) y cartas del gestor de CADA año desde el lanzamiento real — para poder construir gráficos de evolución de cartera año a año (como Carmignac). Investiga en web (gestora, Finect, Morningstar doc library, swissfunddata, Fundsquare, Wayback CDX id_) y REGISTRA lo hallado en known_annual_reports.json + known_manager_letters.json, los descarga y los deja listos para extracción. Úsala en cada análisis nuevo (la llama analizar_fondo.bat) o cuando Rafa diga "sourcing AR de X", "busca informes de X", "skill ar-sourcing X".
---

# ar-sourcing-cowork v1

Corre bajo Claude Max (WebSearch/WebFetch, sin coste API). Objetivo de Rafa: que TODOS los fondos salgan
como Carmignac — con AR y SAR de varios años → histórico de cartera/geo/sector año a año. **Intentarlo al
MÁXIMO**: cuantos más años reales encontremos, mejor el análisis de evolución y consistencia.

El discovery determinista (discovery_v2 harvest_wayback) ya lo intenta, pero falla cuando no sabe el
dominio exacto de la gestora, el año de inicio, o `classify_url` no reconoce el nombre nativo del PDF.
Esta skill es el BACKSTOP capaz (como los subagentes que resolvieron Carmignac/RL/MontLake).

## Entrada
- ISIN del fondo. Lee la identidad de `data/funds/{ISIN}/output.json` (o intl_data/cnmv_data):
  `nombre`, `gestora`, el paraguas (umbrella), los ISINs de clase, y el **año de lanzamiento real**
  (`python -m tools.fund_age {ISIN}` → año; para fondos con predecesor usa `data/fund_lineage.json`).
- Cobertura actual: `python -m tools.doc_completeness {ISIN}` (te dice qué años de AR/SAR/carta FALTAN).
- KB existente: `data/known_annual_reports.json` (umbrella → `reports[]`) y `data/known_manager_letters.json`.

## Qué buscar — AR + SAR + cartas de CADA año desde el lanzamiento
Para el fondo, su **paraguas** y su **gestora**, busca por cada año faltante:
1. **Informe anual (AR)** y **semestral (SAR)** — el informe COMPLETO con Schedule of Investments (de ahí
   salen holdings/geo/sector por año). Fuentes, en orden:
   - **Finect doclegal** por ISIN (`https://www.finect.com/fondos-inversion/{ISIN}`) → doclegal
     documenttype=4 (AR) / 5 (SAR). Solo da el MÁS RECIENTE.
   - **Web de la gestora**: "Report and Accounts" / "Rapport annuel" / "Fund literature" / "Documentos".
   - **Morningstar doc library** (`doc.morningstar.com/document/<hash>.msdoc`), **swissfunddata.ch**,
     **Fundsquare / Fundinfo** — content-addressed, durables.
   - **Wayback Machine (CDX)** para años viejos: enumera el dominio de la gestora
     `http://web.archive.org/cdx/search/cdx?url=<dominio>*&matchType=domain&filter=mimetype:application/pdf&collapse=urlkey&output=json&limit=1000`
     y quédate con los que sean AR/SAR del paraguas correcto. **Da la URL RAW** `https://web.archive.org/web/<ts>id_/<original>` (el `id_` evita el truncado a ~1MB).
   - Google: `"<paraguas>" annual report <año> filetype:pdf`, etc.
2. **Cartas / comentarios del gestor** por trimestre/año (monthly commentary, letter to investors).

## Reglas de rigor
- VERIFICA que cada PDF es del paraguas correcto y contiene el sub-fondo (por nombre; el ISIN puede no
  imprimirse). NO mezcles otro emisor (contaminación tipo Natixis/BlackRock). Ante la duda, descárgalo y
  mira la primera página.
- El año = el que CUBRE el informe (cierre fiscal), no la fecha de publicación.
- Honestidad: si una gestora no publica histórico online (p.ej. RLAM FY2018-23, RAIF privados), dilo y
  quédate con lo que SÍ hay. No inventes URLs — cada una verificada.

## Salida — REGISTRAR y DESCARGAR
1. **known_annual_reports.json**: en la entrada de umbrella del fondo (créala si no existe con
   `{isins:[...], documents_page:"", reports:[]}`), añade cada año a `reports[]` como
   `{"year":"YYYY","type":"annual_report"|"semi_annual_report","url":"<url directa/durable>"}`, y asegúrate
   de que el ISIN target está en `isins`.
2. **known_manager_letters.json**: registra las cartas halladas (patrón + URLs).
3. **Descarga + deja listo para extracción** (para que el histórico de cartera se construya en ESTE run):
   ```
   python -m tools.fetch_annual_report --isin {ISIN}
   python -m tools.ensure_kb_ar {ISIN}      # descarga KB + registra en pending_extraction
   python -m tools.ensure_kb_letters {ISIN}
   ```
4. Resume: cuántos AR/SAR/cartas por año conseguiste y de qué años (y cuáles NO hay públicos).

Tras esto, `extract-pdfs-cowork` procesará todos los años y `build_historical_series` construirá los
gráficos de evolución (holdings/geo/sector año a año) — como Carmignac.
