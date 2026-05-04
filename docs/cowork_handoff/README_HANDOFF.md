# Fund Analyzer — Cowork handoff

Paquete autocontenido para evaluar / replicar el sistema desde otro entorno (compañero, herramienta gratuita, etc.).

---

## ¿Qué hace este sistema?

Dado el **ISIN** de un fondo de inversión:

1. Descarga datos oficiales del regulador (CNMV ES / CSSF LU / CBI IE / AMF FR / Bundesanzeiger DE)
2. Recolecta cartas trimestrales del gestor desde la web de la gestora (live + Wayback Machine)
3. Localiza análisis externos en Morningstar, Citywire, blogs profesionales
4. Identifica y enriquece perfiles del equipo gestor (LinkedIn, prensa especializada)
5. Sintetiza todo con LLMs (Claude Sonnet/Haiku/Opus + Gemini Flash) en 8 secciones
6. Genera `output.json` consolidado + dashboard HTML autocontenido por fondo

Pensado para inversores particulares que quieren auditoría profunda de un fondo sin pagar por terminales caros (Bloomberg / Morningstar Premium).

---

## ¿Por qué existe?

El usuario es inversor que analiza fondos manualmente — leyendo informes CNMV, cartas trimestrales, prensa especializada. El proceso era manual, lento y opaco. Este sistema automatiza la recolección + síntesis preservando criterios profesionales (anti-invención, no completar huecos con texto genérico, citar fuentes, etc.).

**No es un robo-advisor**. No recomienda comprar/vender. Genera ficha analítica con datos verificables.

---

## Quickstart (3 comandos)

```bash
# 1. Instalar
pip install -r requirements.txt

# 2. Configurar API keys (3 servicios externos)
cp .env.example .env
# Editar .env con:
#   ANTHROPIC_API_KEY=sk-ant-...
#   GOOGLE_API_KEY=AIza...
#   SERPER_API_KEY=...        # Serper.dev (Google search wrapper)

# 3. Ejecutar
python -m agents.orchestrator --isin ES0112231008 --auto
```

Salida: `data/funds/ES0112231008/output.json` + `dashboard/fund-ES0112231008.html`.

Tiempo: 10-25 min. Coste: $0.20-0.50 LLM por fondo.

---

## Arquitectura en 1 frase

**Orchestrator secuencial** que coordina **~10 agentes especializados**, cada uno escribe su `_data.json` parcial, y el `analyst_agent` final consolida en `output.json` siguiendo un schema canónico documentado en `CLAUDE.md`.

Detalle: ver `ARCHITECTURE.md`.

---

## ¿Puede una herramienta gratuita replicar esto?

**Sí, con caveats**:

✅ **Lo que sí**: el código en sí es Python estándar (asyncio + httpx + pdfplumber + lxml). Ninguna dependencia propietaria. Toda la lógica de extracción/parsing es replicable.

⚠️ **Lo que requiere coste externo**:
- **Anthropic API** (Claude): los 8 agentes de síntesis del `analyst_agent` + audit del Opus. Sin esto, el output queda en datos crudos sin narrativa.
- **Google Gemini API**: usado por `intl_extractor_v2` para parsing de annual reports INT (concept-first 2-stage). Tiene generoso free tier (15 requests/min, 1500/day, suficiente para uso personal).
- **Serper.dev**: Google Search API. **$50 = 50.000 queries**. Pipeline usa ~30-80 queries/fondo. Alternativa: SerpAPI, Brave Search API, o cualquier wrapper Google.

🔄 **Drop-in replacements posibles**:
- Anthropic → cualquier LLM con tool use (OpenAI, Mistral, Llama). Hay que adaptar `tools/claude_extractor.py` y `gemini_wrapper.py`.
- Gemini → Anthropic (ya existe fallback automático en `gemini_wrapper._fallback_sonnet_extract`).
- Serper → Brave Search API (gratis hasta 2K queries/mes).
- LLMs locales (Ollama + Llama 3.1 70B): viable para extracción estructurada, calidad menor en síntesis narrativa.

❌ **Lo que NO se puede saltar**:
- Conexión a CNMV (público gratis, sin API key).
- Descarga de PDFs públicos de gestoras (sin API).
- Wayback Machine API (gratis, rate limited).

**Coste mínimo viable** con todo gratis: ~$0 si solo necesitas datos crudos (CNMV + PDFs descargados). Si quieres síntesis narrativa, ~$0.20/fondo con setup más barato (Gemini Flash + cache).

---

## Documentos en este paquete

| Archivo | Contenido |
|---|---|
| `README_HANDOFF.md` | Este archivo — entrada principal |
| `ARCHITECTURE.md` | Diagrama agentes + flujo orchestrator + contrato output.json |
| `MEMORY_SUMMARY.md` | 30+ aprendizajes destilados (Discovery, Quality, Pipeline ES/INT, Cost-opt) |
| `KNOWN_GAPS.md` | 6 gaps de documentación operativa para uso en producción |
| `MANIFEST.md` | Listado de qué código + datos hay que entregar para replicar |
| `README.md` | Copia del README raíz del proyecto (quickstart técnico) |
| `CLAUDE.md` | Convenciones canónicas (schema output.json, accessors, pipeline INT, fases) |
| `.env.example` | Template de variables de entorno |
| `requirements.txt` | 10 dependencias Python |
| `LICENSE` | MIT |
| `sample_outputs/` | **7 fondos validados** (output.json + dashboard HTML). Ver el resultado real del sistema |

---

## Roadmap actual del proyecto

- ✅ **Fase K (2026-04-29)**: auditoría sistémica, 11 fixes (anti-regresión quality loop, merge profiler↔deep, temperature=0, etc.)
- ✅ **Fase L (2026-04-29)**: 3 schema mismatches descubiertos post-validación
- ✅ **Cost-Opt (2026-05-02)**: cache LLM, prompt caching Anthropic, fallback Gemini→Haiku, max_iter=1, instrumentación 100%
- 🔄 **Fase M (en curso, 2026-05-04)**: downgrade 4 calls Opus→Haiku (ahorro 45%), skip-fresh para re-runs (ahorro 90%), paquete documentación cowork

---

## Soporte

Repositorio: privado por ahora (push a GitHub pendiente tras Fase M). Contacto del autor: rafagdominguez96@gmail.com.
