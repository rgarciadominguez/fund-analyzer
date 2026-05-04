# Plan: Discovery v2 — rediseño desde cero

## Contexto y motivación

Discovery v1 ha acumulado parches reactivos (Cloudflare bypass, monthly penalty,
umbrella AR exception, URL template learner, wayback news, identity resolver vía
FT/Morningstar/KID…) que resuelven síntomas locales pero degradan el
comportamiento global:

- Budget de 60 descargas se satura en PDFs que luego se rechazan
- Validator rechaza AR umbrella legítimos por no tener el ISIN en las 5 primeras pág.
- Scoring de candidatos pondera monthly factsheets por encima de AR/SAR
- Crawl encuentra 900 candidatos y descarga los 60 peores por orden de score
- Se clasifica después de descargar, no antes

Caso límite Trojan (IE00B6T42S66): 217 PDFs visibles en taml.co.uk, sólo 4
descargados, ninguna Investor Letter, ninguna Special Paper, AR capturado es el
UK (no el Ireland).

## Objetivo v2

Dado un ISIN, maximizar:
- Annual Reports desde inception (o últimos 5 años)
- Semi-Annual / Interim Reports (últimos 5 años)
- Quarterly Letters / Investor Letters (últimos 3 años)
- Prospectus vigente
- KID/KIID vigente
- Factsheet EOY (diciembre) últimos 3 años

Con un budget de 60 descargas útiles (no desperdiciadas en candidatos que luego
se rechazan).

## Principios rectores

1. **URL-first classification.** Clasificar por filename + URL antes de gastar
   budget de descarga. Un `Investment-Report-No-12.pdf` en `/uploads/2024/03/`
   es quarterly_letter 2024-Q1 sin necesidad de bajarlo.

2. **Target-driven, no bulk-download.** Por cada hueco (doc_type + periodo)
   lanzar búsqueda dirigida hasta cubrir. Cerrar el target al encontrar.

3. **Domain-trust escalado.** PDF en dominio de la gestora ya descubierta →
   validación ligera (parseable + size > 50 KB). No exigir ISIN interno si la
   URL es de la gestora.

4. **Harvest exhaustivo del primary domain.** Cuando se identifica la web real
   de la gestora con página del fondo, agotar TODOS los PDFs clasificables ahí
   antes de ir a Google / Wayback / CDN externos.

5. **Budget por tipo.** Repartir el budget entre doc_types para que factsheets
   no monopolicen.
   - AR/SAR: 15
   - Quarterly letters: 20
   - Factsheets EOY: 5
   - Prospectus/KID: 4 (latest-only)
   - Buffer / descubrimiento: 16

6. **Cloudflare + TLS fingerprinting resueltos de entrada.** `curl_cffi` con
   impersonate Chrome como único cliente para todos los fetch; httpx se deja
   solo para APIs JSON simples.

## Arquitectura nueva

```
agents/discovery_v2/
├── __init__.py
├── identity.py         # resolver nombre + gestora + website (KID > Finect > regulator)
├── url_classifier.py   # classify_pdf_url(url,text) → (doc_type, periodo, confidence, fiscal_hint)
├── harvest.py          # dado un dominio, fetch página + extraer href*.pdf + clasificar
├── prioritizer.py      # cola de candidatos por target gap × quality × recency
├── fetcher.py          # curl_cffi client con retry + cache + magic-byte sniff
├── validator.py        # validación ligera por categoría
├── state.py            # SharedState simplificado: targets, covered, budget-por-tipo
├── pipeline.py         # orquesta fases 1-5
└── finect_adapter.py   # canal redundante universal (Morningstar via Finect JSON)
```

### Fases del pipeline

**Fase 1 — Identity resolution** (presupuesto: 5 http + 1 google)
- Input: ISIN + output del regulator
- Cascada:
  1. Si regulator dio nombre + gestora → skip
  2. Google dork `site:finect.com/fondos-inversion "{ISIN}"` → slug Finect →
     parse JSON `documents[]` → nombre + ManCo + MS_ID + 5 PDFs
  3. Si aún falta gestora: descargar KID del prestep → extraer
     `Investment Manager` / `Manager's website at ...` → fetch homepage title
- Output: `identity.nombre_oficial`, `.gestora_oficial`, `.ManCo`,
  `.investment_manager`, `.gestora_websites` (lista)

**Fase 2 — Harvest primary** (presupuesto: 30 http + 30 download)
- Por cada website conocido (gestora_websites ∪ prestep_domains):
  - Fetch con curl_cffi
  - Probar rutas: `/`, `/funds`, `/funds/{slug}`, `/our-funds`, `/documents`,
    `/literature`, `/investors`, `/insights`, `/publications`
  - BFS profundidad 2
  - Extraer todos los `href*.pdf`
  - Clasificar cada uno por URL → `(doc_type, periodo, conf)`
  - Filtrar: keep si `(doc_type, periodo)` está en targets missing, O si es
    `quarterly_letter` con año reciente (últimos 3)
  - Enqueue en `prioritizer` con score: `target_gap_bonus × domain_score × recency_bonus`
- Consumir cola con budget por tipo hasta cubrir o agotar

**Fase 3 — Complementary** (presupuesto: 15 download)
- Finect JSON (canal redundante universal, 5-8 docs típicos)
- Google dorks dirigidos sólo a HUECOS concretos:
  `"{ISIN}" OR "{nombre}" "annual report" {year} filetype:pdf`
- Sólo doc-types aún no cubiertos

**Fase 4 — Wayback CDN histórico** (presupuesto: 15 http + 10 download)
- Solo si aún faltan ≥2 AR históricos
- CDX domain-wide sobre gestora_websites con filter PDF y filename tipo AR/SAR
- Descarga directa archived URL
- NO intentar wayback de páginas HTML (inviable confirmado v1)

**Fase 5 — Email fallback + cierre**
- Si gap > 50% → `email_agent.maybe_draft_request`
- Beep final
- Guardar `intl_discovery_data.json`

### url_classifier (núcleo de la clasificación antes-de-descargar)

```python
def classify_pdf_url(url: str, link_text: str = "") -> dict:
    """
    Devuelve:
      {
        "doc_type": "annual_report|semi_annual_report|quarterly_letter|factsheet|kid|prospectus|unknown",
        "periodo": "YYYY" o "YYYY-MM" o "",
        "fiscal_hint": "eoy|mid_year|monthly|qN|",
        "confidence": 0..100,
        "skip_reason": "" o "monthly_factsheet|privacy_policy|brochure|..."
      }
    """
```

Reglas por orden (primera que match gana):
1. Filename contiene `annual-report` / `annual_report` / `jahresbericht` /
   `rapport-annuel` / `informe-anual` → AR, periodo = año del filename o del
   folder `/YYYY/`, conf=90
2. Filename contiene `semi-annual` / `interim-report` / `halbjahres` /
   `rapport-semestriel` → SAR, conf=90
3. Filename match `Investment-Report-No-(\d+)` o `Investor-Letter-DD.MM.YYYY` o
   `quarterly-letter` / `carta-trimestral` / `lettre-trimestrielle` →
   quarterly_letter, periodo del año detectado
4. Filename contiene `fact-sheet` / `factsheet` + mes+año → factsheet,
   fiscal_hint=eoy si mes=Dec, mid_year si Jun, monthly resto. Si monthly y no
   Dec/Jun → `skip_reason="monthly_factsheet"`
5. Filename contiene `kid` / `kiid` / `priips` / `datos-fundamentales` → kid
6. Filename contiene `prospectus` / `folleto` / `prospekt` → prospectus
7. Keywords negativos (`privacy`, `cookies`, `terms`, `brochure`, `glossary`,
   `application-form`) → `skip_reason`
8. Fallback: unknown

Confidence < 50 y no skip → se descarga solo si el target de ese doc_type aún
tiene hueco Y el dominio es primary.

### prioritizer

Cola (heap) donde cada candidato tiene score:

```
score = base_score(doc_type)           # AR=100, SAR=90, letter=70, factsheet=50
      + gap_bonus                      # +50 si cubre un hueco concreto
      + domain_score(host)             # gestora_own=+30, parent_cdn=+15, other=0
      + recency_bonus(periodo)         # +20 si ≤2años, +10 si 3-5, 0 resto
      + isin_match_bonus               # +40 si ISIN exacto en URL, +15 si nombre fondo
      + sicav_match_bonus              # +10 si SICAV paraguas match
      - classifier_penalty             # -40 si skip_reason
```

### Cambios fuera de discovery_v2

- `intl_discovery_agent.py` → llama a `discovery_v2.pipeline.run()` en lugar del
  flujo antiguo
- Mantener `email_agent.py`, `ingest_manual.py` tal como están (ya funcionan)
- Viejo `agents/discovery/` NO se borra — queda como fallback y para no romper
  `cnmv_agent` etc. que lo referencian

## Lo que NO vamos a hacer

- No reemplazar email_agent ni ingest_manual (ya funcionan)
- No tocar cnmv_agent / regulators (funcionan con `agents/discovery/` actual)
- No implementar identity resolver vía Morningstar (anti-bot) ni FT manager regex
  (da persona, no company)
- No implementar wayback news-harvesting (inviable confirmado)

## Estimación

- identity.py: 40 LOC
- url_classifier.py: 100 LOC
- harvest.py: 80 LOC
- prioritizer.py: 30 LOC
- fetcher.py: 60 LOC
- validator.py: 40 LOC
- state.py: 50 LOC
- pipeline.py: 120 LOC
- finect_adapter.py: 70 LOC
- Tests con Storm + DNCA + Trojan: 40 LOC

Total ≈ 630 LOC de código nuevo, sin tocar lo viejo.

## Criterios de éxito

Post-implementación, en Trojan IE00B6T42S66:
- ≥2 Annual Reports (incluyendo el de Trojan Funds Ireland, no UK)
- ≥1 Interim Report
- ≥3 Investor Letters / Investment Reports numerados
- 1 factsheet EOY
- 1 KID + 1 prospectus
- Budget download ≤40/60
- Tiempo ≤10 min

En DNCA LU1694789378:
- ≥1 AR (2024)
- ≥1 SAR (2025)
- ≥2 quarterly letters
- 1 factsheet EOY
- 1 KID + 1 prospectus

En Storm LU0840158819 (regresión):
- ≥15 docs (igual o mejor que los 18 actuales, sin perder ninguno)

## Orden de implementación

1. `url_classifier.py` + tests unitarios con filenames reales vistos en Trojan/DNCA/Storm
2. `fetcher.py` + `validator.py`
3. `harvest.py` + `prioritizer.py` + `state.py`
4. `identity.py` + `finect_adapter.py`
5. `pipeline.py` integrando todo
6. Hook en `intl_discovery_agent.py`
7. Tests end-to-end en los 3 fondos
8. Ajustes finos por resultados
