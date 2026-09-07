# HANDOFF — Sesión 2026-09-06/07 · contexto completo

> Mapa de TODO lo hecho esta sesión y el estado del sistema (análisis · fund-analyzer · conexión con
> el portal · fund-dashboard). El detalle fino está en las **memorias** (`~/.claude/projects/<clave>/memory/`,
> copiadas al proyecto nuevo). Aquí está la visión de conjunto para retomar sin perderse.

---

## 0 · Cómo retomar
- **Ruta nueva del repo:** `C:\Users\RafaelGarcía\OneDrive - Nazca\Rafa\Personal\Asesoría Financiera\fund-analyzer`
  (se movió el 2026-09-07; la del Escritorio está PROHIBIDA y se borra al reiniciar — ver `PROMPT_fund_analyzer_mudanza_carpeta.md` en `horizonte-datos`).
- Arrancar: `cd /d "<ruta nueva>"; claude --resume` → sesión `2e08bde7` (esta conversación).
- **Todo committeado en GitHub `v2-cowork @ 6c94a88`.**
- **Siguiente tarea: A1 — retrofit lote 2** (ver §5).
- Antes de cerrar cualquier tarea: `python -m tools.check_ruta_repo` (0 = limpio).

---

## 1 · Lo construido esta sesión en fund-analyzer

### 1.1 Sistema de LINEAGE / predecesor  → [[lineage-predecessor-system]]
Reconoce fondos que vienen de OTRO vehículo (AMC/RAIF/Cayman→UCITS, renombrados) para track-record e
histórico completo (§0.9: track = serie NAV real más larga, no la fecha legal).
- `tools/lineage_detect.py` (determinista, 0 tokens): serie NAV real empieza antes del lanzamiento legal → predecesor.
- `tools/ensure_lineage.py`: gate en prep (INT, <7 años o gap) → detecta + encola.
- skill `lineage-resolver-cowork`: investiga web e identifica los vehículos → `data/fund_lineage.json`.
- `tools/apply_lineage.py` + `morningstar_daily.compute_metrics` + `track_record_isin.resolve_track_record`
  lineage-aware → quant extendido con etiqueta; dashboard con banner + `_dash_secid` a la clase con serie larga.
- **Caso MontLake** (IE000Z9YV312/IE000RDB0I49): estrategia desde 2018 (AMC XS1853197686 → RAIF LU2334862211
  jun-2021 → UCITS may-2024, gestor Fortune FS/Khalili). Quant 2021-2026, narrativa con el predecesor.
  Docs del RAIF = privados (techo real).

### 1.2 Auto-resume ante "hit your limits"  → [[autoresume-hit-limits]]
La cola del web_server pausa, parsea la hora de reset (Madrid), duerme hasta reset+5min y reanuda en
`--resume` (no cold-start). Fix clave: item reanudado continúa donde se quedó. `tools/retrofit_fund.py`
tiene el MISMO auto-resume (para los retrofit fuera de la cola).

### 1.3 Gap-targeting + completitud documental  → [[doc-completeness-por-anio]]
- `tools/fund_age.launch_year`: año real (lineage→output.json→**Supabase**). Bug arreglado: muchos INT
  no traían fecha → no se flaggeaba multi-año (RL parecía de 1 año siendo de 2015).
- `tools/doc_completeness.assess`: cobertura AR/SAR/carta por año + huecos `faltan_ar/faltan_sar/faltan_carta`.
  Rejilla visible en la pestaña Documentos del dashboard.
- **REGLA FUNDAMENTAL (Rafa):** el sourcing busca SOLO los huecos, nunca re-busca lo que ya hay.
  `retrofit_fund._gap_prompt` EMBEBE el gap en el prompt (determinista); la skill `ar-sourcing` tiene el
  PASO 0 obligatorio que lo computa.

### 1.4 Skill `ar-sourcing-cowork` (fondos nuevos salen como Carmignac)
Para cada fondo INT nuevo (cableada en `analizar_fondo.bat` paso 1.6) busca AR+SAR+cartas de los años que
faltan (gestora/Finect/Morningstar doclib/swissfunddata/Wayback id_) → KB → descarga → extract construye la
evolución multi-año en el mismo run. `discovery_v2` también más fiable (inception fallback a `fund_age`).

### 1.5 Anti RE-contaminación  → [[ie000z9yv312-annual-report-contaminated]]
Limpiar output.json NO basta: el re-run re-mete el doc si el fichero físico + la URL original siguen en
`raw/discovery` + `discovery_kb.json/known_urls`. `tools/clean_fund_docs.py` purga TODO (fichero, refs por
basename Y por substring de URL `DROP_URL_SUBSTRINGS`, Storage). `archive_docs` nunca archiva `DROP_BASENAMES`.
Caso: MontLake abría un Natixis 2004 desde `web.archive.org/…natixis.com`.

### 1.6 `archive_docs` mejorado (docs multi-año de TODOS)
Reconoce AR/SAR con nombre NATIVO de gestora (`anr-`, `sar-…-20240630`, `rechenschaft`, `comptes-annuels`,
`interim`) + saca el año del PDF cuando falta (`_year_from_pdf`). Re-archivado de los 69 fondos → muchos
pasaron de 1 AR a varios en la pestaña Documentos.

### 1.7 Selector de clase (dashboard) + charts  → [[portal-embeds-worker-fund-url]]
Opciones del `<select>` visibles + al cambiar de clase se **recargan los gráficos** con la serie de esa
clase (`window.switchClass`: primaria = SecId lineage; otra = resuelve por ISIN). El gráfico Base-100
muestra el histórico completo de la clase elegida.

### 1.8 `tools/retrofit_fund.py` (proceso completo por fondo)
sourcing SOLO huecos → extract todos → analyst con años nuevos → archive + sync + commit. Auto-resume de
cuota + auto-retry de red + step-skip. Uso: `python -m tools.retrofit_fund <ISIN...>`.

---

## 2 · Conexión fund-analyzer ↔ Supabase ↔ Portal

### 2.1 El Worker (lo que Rafa ve al pulsar "ver análisis")  → [[portal-embeds-worker-fund-url]]
El portal EMBEBE un iframe `https://fund-analyzer.rafagdominguez96.workers.dev/fund-{ISIN}?v={fecha}` con
el ISIN de la clase pulsada. El Worker (Cloudflare static assets `./dashboard/` + `worker/index.js` modo
avanzado, `run_worker_first:["/fund-*"]`) ROUTEA cualquier clase → el HTML del PRIMARIO del grupo (mapa
`dashboard/_class_map.json`, generado por `tools/build_class_map.py`) e inyecta un selector de clase.
Sin esto las clases sin HTML daban 404. Deploy = push a v2-cowork (CF Workers Builds).

### 2.2 `es_primario_del_grupo` (§0.9)
Se recomputa en `export_horfin_catalog.build()` por grupo: la clase que TIENE análisis cualitativo, EUR,
más track = 1 primario/grupo, alineado con el dashboard. (Antes se arrastraba stale → la vista de
categorización mostraba varias clases por fondo.) Verificado: 60 grupos multi-clase, 0 con ≠1 primario.

### 2.3 Categorización / Tareas (colapso por fondo)
FIX de datos hecho: es_primario correcto + `has_qualitative_analysis` deduplicado a 1 por grupo + `divisa`
poblada en el catálogo. **El portal (dev) ya aplicó** el colapso por `fund_group_id` (1 fila/fondo, resto en
"+N clases") y la propagación de clasificación a nivel grupo. **Pendiente del dev:** alinear 6 grupos ES con
nota divergente (es clasificación RAFA_ONLY; no la tocamos).

### 2.4 Documentos
Doble sitio alineado: `output.json.analyst_synthesis.documentos.informes_pdf` (lo pinta el dashboard/iframe)
+ `fund_groups.portfolio_metrics_jsonb.documentos` (lo empuja `portal_analyze_worker.push_meta` → sync-meta →
`asset_meta.documentos` para la ficha rápida). Pestaña Documentos agrupada por tipo (Informes anuales /
semestrales / Folleto / KID / Ficha / Cartas), CNMV/XMLs solo en ES.

### 2.5 Cache-bust
El `?v=` del iframe = `fund_groups.fecha_ultimo_analisis`. Si cambias contenido sin mover esa fecha, el
navegador/CF sirve caché vieja → tras regenerar hay que bumpear `fecha_ultimo_analisis` + `_refresh_portal_catalog`.

### 2.6 Contrato portal (NO cambiar)  → [[portal-sync-webhook]] [[sync-siempre-tres-destinos]]
Solo REST. Entra: sync-metricas/sync-clases/sync-meta. Sale: admin/assets/cola-analisis + inputs-rafa.
Campos RAFA_ONLY (clasificacion_user/opinion_user/encaje_texto) el pipeline NO los pisa. Norma: un cambio de
datos → fund-analyzer + Supabase + portal al día (los tres destinos).

### 2.7 ⚠️ DRIFT del servidor del portal (aviso de otra sesión)
Otra sesión desplegó cambios DIRECTAMENTE al servidor vivo del portal por SSH (parejas + ver→actuar +
fix editor de fondos), SIN pasar por el repo local `horizonte-datos`. **EL SERVIDOR ES LA VERDAD.** Antes de
desplegar algo del repo local al portal, hacer `scp` de sus ficheros primero (lista en el prompt que pasó Rafa:
plugin `horizonte-seguimiento` en Hostinger, `ssh -p 65002 u627070525@46.202.158.118`). NO tocó nada de
fund-analyzer ni los endpoints del contrato (admin/catalogo, admin/activos/inputs, admin/analisis/output, métricas).

---

## 3 · fund-dashboard
Repo aparte (`C:\Users\RafaelGarcía\fund-dashboard`). El Worker de fund-analyzer sirve los dashboards de
análisis; `morningstar_daily` usa la misma vía que el fund-dashboard (serie diaria por SecId, cuadra 100%).
No se tocó esta sesión.

---

## 4 · Estado de los análisis (lo hecho hoy)

### 4.1 Retrofit multi-año de 7 fondos (de 0 a varios años de evolución de cartera)
| Fondo | años cartera |
|---|---|
| Robeco QI Emerging LU0329355670 | 9 (2014-2025) |
| Robeco QI Momentum LU1048590381 | 9 |
| Robeco Chinese LU1654173217 | 8 |
| Vontobel European LU0153585137 | 6 |
| Goldman Sachs LU0234681749 | 5 |
| Ashoka India IE00BDR0JY05 | 4 |
| MontLake-USD IE000RDB0I49 | 1 (techo real: 2024 + RAIF privado) |

### 4.2 Carmignac / Royal London (antes del lote)
- **Carmignac** LU1623762843: 6 años (2020-2025) — el ejemplo que motivó "que todos salgan así".
- **Royal London Global Bond Opps** IE00BGSVCP50: 2 años (FY2024/2025). FY2018-2023 NO existen públicos
  (verificado 2 barridos exhaustivos; RLAM no publica histórico). Techo real. → [[ie00bgsvcp50-rlam-global-bond-opps]]

---

## 5 · Pendiente (ON-HOLD) — dónde seguir

**A · Sourcing/retrofit multi-año**
- **A1 · Retrofit lote 2** ⭐ (BARATO, docs ya en disco): Trojan, Baillie Gifford, GAM, JPMorgan, BNY…
  → computar candidatos (fondos con AR/SAR físicos multi-año y pocas `posiciones.historicas`) → `python -m tools.retrofit_fund <ISIN...>`.
- **A2 · Re-sourcear (Lista B)** (CARO, búsqueda web): ~16 viejos con pocos AR — JPMorgan Europe, GAM,
  DNCA Credit, Jupiter, DNCA Alpha Bonds… Descartar techos confirmados (RL×2, Sifter).
- **A3 · ES semestral multi-año** (GRATIS, determinista): ~35 ES — bajar todos los años del semestral CNMV
  (cnmv_agent ya baja H2 multi-año, hay que cablearlo). Cierra Goal 2 para ES.

**B · Portal (dev):** alinear los 6 grupos ES con nota divergente.

**C · Fondos concretos:** JPMorgan Eq+ `LU0289214628` (pos=0) · Sissener Canopus `LU0694231910` (vacío) ·
anuales vencidas (DNCA Alpha Bonds, DNCA Credit, Equam) · cola **rf_redo** (24 ES).

---

## 6 · Gotchas / no perseguir fantasmas (de la mudanza)
- Nunca ruta absoluta a mano en código NI en datos (`output.json` etc.) → siempre relativa a `ROOT`
  (`Path(__file__).parent.parent`). El problema gordo de la mudanza fueron 15.965 rutas absolutas en 938 ficheros.
- Fallos que NO son de la mudanza: canario `morningstar_serie` (`_URL` no existe, deriva de código) · índices
  investing.com HTTP 403 (desde junio) · `SUPABASE_ACCESS_TOKEN` vacío en `.env` · túnel Cloudflare bloqueado
  por DNS de Orange (usar 1.1.1.1). NO reescribir los ~50 ficheros históricos que mencionan el Escritorio.
- `techo de la gestora`: "máximo años" depende de lo que publique cada gestora online. Donde publican histórico
  (Carmignac, Trojan, Robeco…) → muchos años; donde solo el ciclo actual (RLAM, RAIF privados) → techo bajo.
  Se documenta en `known_annual_reports.json._nota` para no repetir esfuerzo.
